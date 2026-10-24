import 'dotenv/config';
import amqp from 'amqplib';
import pg from 'pg';
import { send } from './providers.js';

const { Pool } = pg;
const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const RETRY_QUEUES = ['retry.5s', 'retry.30s', 'retry.5m'];
const RETRY_DELAYS = [5000, 30000, 300000];
const EXCHANGE = 'notifications';
let connection;
let channel;
let stopping = false;
let active = 0;
const consumerTags = [];

async function setupTopology(ch) {
  await ch.assertExchange(EXCHANGE, 'direct', { durable: true });
  for (const route of ['email', 'sms', 'push']) {
    const queue = `queue.${route}`;
    await ch.assertQueue(queue, { durable: true, arguments: { 'x-queue-type': 'quorum' } });
    await ch.bindQueue(queue, EXCHANGE, route);
  }
  await ch.assertQueue('queue.dlq', { durable: true });
  await ch.assertExchange('notification-retry', 'direct', { durable: true });
  for (const route of ['email', 'sms', 'push']) {
    for (let i = 0; i < RETRY_QUEUES.length; i++) {
      const name = `queue.retry.${RETRY_QUEUES[i]}.${route}`;
      await ch.assertQueue(name, { durable: true, arguments: { 'x-message-ttl': RETRY_DELAYS[i], 'x-dead-letter-exchange': EXCHANGE, 'x-dead-letter-routing-key': route } });
      await ch.bindQueue(name, 'notification-retry', `${i}.${route}`);
    }
  }
}

function publish(exchange, routingKey, content, properties) {
  return new Promise((resolve, reject) => channel.publish(exchange, routingKey, content, { persistent: true, contentType: 'application/json', ...properties }, (error) => error ? reject(error) : resolve()));
}

async function recordAttempt(client, id, attemptNo, success, error, duration) {
  await client.query('INSERT INTO delivery_attempts (notification_id, attempt_no, success, error, duration_ms) VALUES ($1, $2, $3, $4, $5)', [id, attemptNo, success, error, duration]);
}

async function processMessage(message) {
  let event;
  try { event = JSON.parse(message.content.toString()); } catch { channel.nack(message, false, false); return; }
  const id = event.notificationId;
  const retryCount = Number(message.properties.headers?.['x-retry-count'] || 0);
  const client = await pool.connect();
  active++;
  try {
    await client.query('BEGIN');
    const result = await client.query('SELECT * FROM notifications WHERE id = $1 FOR UPDATE', [id]);
    if (!result.rowCount || result.rows[0].status === 'sent' || result.rows[0].status === 'failed') {
      await client.query('COMMIT');
      channel.ack(message);
      return;
    }
    await client.query("UPDATE notifications SET status = 'processing', updated_at = NOW() WHERE id = $1", [id]);
    await client.query('COMMIT');
    const notification = result.rows[0];
    const started = Date.now();
    try {
      await send(notification);
      await client.query('BEGIN');
      await recordAttempt(client, id, notification.attempts + 1, true, null, Date.now() - started);
      await client.query("UPDATE notifications SET status = 'sent', attempts = attempts + 1, last_error = NULL, updated_at = NOW() WHERE id = $1", [id]);
      await client.query('COMMIT');
      channel.ack(message);
    } catch (error) {
      const duration = Date.now() - started;
      await client.query('BEGIN');
      const nextAttempt = notification.attempts + 1;
      await recordAttempt(client, id, nextAttempt, false, error.message, duration);
      if (retryCount < RETRY_QUEUES.length) {
        const retryQueue = `retry.${RETRY_QUEUES[retryCount]}.${notification.channel}`;
        await publish('notification-retry', `${retryCount}.${notification.channel}`, message.content, {
          expiration: undefined,
          headers: { ...message.properties.headers, 'x-retry-count': retryCount + 1 },
          messageId: id,
          type: retryQueue,
        });
        await client.query("UPDATE notifications SET status = 'retrying', attempts = attempts + 1, last_error = $2, updated_at = NOW() WHERE id = $1", [id, error.message]);
      } else {
        await publish('', 'queue.dlq', message.content, { headers: { ...message.properties.headers, 'x-retry-count': retryCount, 'x-last-error': error.message }, messageId: id });
        await client.query("UPDATE notifications SET status = 'failed', attempts = attempts + 1, last_error = $2, updated_at = NOW() WHERE id = $1", [id, error.message]);
      }
      await client.query('COMMIT');
      channel.ack(message);
    }
  } catch (error) {
    await client.query('ROLLBACK').catch(() => {});
    console.error('Failed to process message; requeueing:', error);
    channel.nack(message, false, true);
  } finally {
    client.release();
    active--;
  }
}

async function start() {
  connection = await amqp.connect(process.env.RABBITMQ_URL);
  connection.on('error', (error) => console.error('RabbitMQ error:', error.message));
  connection.on('close', () => { if (!stopping) { console.error('RabbitMQ disconnected; exiting for restart'); process.exit(1); } });
  channel = await connection.createConfirmChannel();
  await setupTopology(channel);
  await channel.prefetch(10);
  for (const route of ['email', 'sms', 'push']) {
    const result = await channel.consume(`queue.${route}`, processMessage, { noAck: false });
    consumerTags.push(result.consumerTag);
  }
  console.log('NotifyFlow worker consuming email, sms, and push queues');
}

async function shutdown() {
  if (stopping) return;
  stopping = true;
  await Promise.all(consumerTags.map((tag) => channel.cancel(tag).catch(() => {})));
  const deadline = Date.now() + 25000;
  while (active > 0 && Date.now() < deadline) await new Promise((resolve) => setTimeout(resolve, 100));
  await Promise.allSettled([channel?.close(), connection?.close(), pool.end()]);
  process.exit(0);
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
start().catch((error) => { console.error(error); process.exit(1); });
