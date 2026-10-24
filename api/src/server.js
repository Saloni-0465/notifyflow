import 'dotenv/config';
import express from 'express';
import cors from 'cors';
import pg from 'pg';
import Redis from 'ioredis';
import amqp from 'amqplib';
import { randomUUID } from 'node:crypto';

const { Pool } = pg;
const app = express();
app.use(cors());
app.use(express.json({ limit: '256kb' }));

const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const redis = new Redis(process.env.REDIS_URL, { maxRetriesPerRequest: 2 });
const EXCHANGE = 'notifications';
let connection;
let channel;

async function connectQueue() {
  connection = await amqp.connect(process.env.RABBITMQ_URL);
  connection.on('error', (error) => console.error('RabbitMQ connection error:', error.message));
  connection.on('close', () => { console.error('RabbitMQ connection closed; exiting for restart'); process.exit(1); });
  channel = await connection.createConfirmChannel();
  await channel.assertExchange(EXCHANGE, 'direct', { durable: true });
  for (const route of ['email', 'sms', 'push']) {
    const queue = `queue.${route}`;
    await channel.assertQueue(queue, { durable: true, arguments: { 'x-queue-type': 'quorum' } });
    await channel.bindQueue(queue, EXCHANGE, route);
  }
  await channel.assertQueue('queue.dlq', { durable: true });
}

function publish(routingKey, message, options = {}) {
  return new Promise((resolve, reject) => channel.publish(EXCHANGE, routingKey, Buffer.from(JSON.stringify(message)), {
    persistent: true,
    contentType: 'application/json',
    messageId: message.notificationId,
    headers: { 'x-retry-count': 0 },
    ...options,
  }, (error) => error ? reject(error) : resolve()));
}

app.get('/health', async (_req, res) => {
  const checks = {};
  try { await pool.query('SELECT 1'); checks.postgres = 'ok'; } catch { checks.postgres = 'error'; }
  try { await redis.ping(); checks.redis = 'ok'; } catch { checks.redis = 'error'; }
  try { checks.rabbitmq = channel?.connection ? 'ok' : 'error'; } catch { checks.rabbitmq = 'error'; }
  const healthy = Object.values(checks).every((value) => value === 'ok');
  res.status(healthy ? 200 : 503).json({ status: healthy ? 'ok' : 'degraded', checks });
});

app.post('/notifications', async (req, res, next) => {
  const { idempotencyKey, channel: deliveryChannel, recipient, template, payload = {} } = req.body ?? {};
  if (typeof idempotencyKey !== 'string' || !idempotencyKey.trim() || idempotencyKey.length > 255) {
    return res.status(400).json({ error: 'idempotencyKey is required (max 255 characters)' });
  }
  if (!['email', 'sms', 'push'].includes(deliveryChannel)) return res.status(400).json({ error: 'channel must be email, sms, or push' });
  if (typeof recipient !== 'string' || !recipient.trim()) return res.status(400).json({ error: 'recipient is required' });
  if (typeof template !== 'string' || !template.trim()) return res.status(400).json({ error: 'template is required' });
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) return res.status(400).json({ error: 'payload must be a JSON object' });

  const key = idempotencyKey.trim();
  const redisKey = `idem:${key}`;
  let reserved = false;
  try {
    let cachedId;
    try { cachedId = await redis.get(redisKey); } catch (error) { console.warn('Redis unavailable; using PostgreSQL idempotency constraint:', error.message); }
    if (cachedId && cachedId !== 'pending') {
      const result = await pool.query('SELECT id, status FROM notifications WHERE id = $1', [cachedId]);
      if (result.rowCount) return res.status(200).json({ ...result.rows[0], duplicate: true });
    }
    try { reserved = (await redis.set(redisKey, 'pending', 'NX', 'EX', 86400)) === 'OK'; }
    catch (error) { console.warn('Redis unavailable; relying on PostgreSQL uniqueness:', error.message); }
    if (!reserved) {
      const existing = await pool.query('SELECT id, status FROM notifications WHERE idempotency_key = $1', [key]);
      if (existing.rowCount) return res.status(200).json({ ...existing.rows[0], duplicate: true });
    }
    const id = randomUUID();
    const inserted = await pool.query(
      `INSERT INTO notifications (id, idempotency_key, channel, recipient, template, payload)
       VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (idempotency_key) DO NOTHING
       RETURNING id, status`, [id, key, deliveryChannel, recipient.trim(), template.trim(), JSON.stringify(payload)]
    );
    if (!inserted.rowCount) {
      const existing = await pool.query('SELECT id, status FROM notifications WHERE idempotency_key = $1', [key]);
      await redis.set(redisKey, existing.rows[0].id, 'EX', 86400).catch(() => {});
      return res.status(200).json({ ...existing.rows[0], duplicate: true });
    }
    try {
      await publish(deliveryChannel, { notificationId: id, channel: deliveryChannel });
    } catch (error) {
      await pool.query("UPDATE notifications SET status = 'failed', last_error = $2, updated_at = NOW() WHERE id = $1", [id, `Queue publish failed: ${error.message}`]);
      throw error;
    }
    await redis.set(redisKey, id, 'EX', 86400).catch(() => {});
    return res.status(202).json({ id, status: 'queued' });
  } catch (error) {
    if (reserved) await redis.del(redisKey).catch(() => {});
    return next(error);
  }
});

app.get('/notifications/:id', async (req, res, next) => {
  try {
    const result = await pool.query('SELECT * FROM notifications WHERE id = $1', [req.params.id]);
    if (!result.rowCount) return res.status(404).json({ error: 'Notification not found' });
    const attempts = await pool.query('SELECT attempt_no, success, error, duration_ms, created_at FROM delivery_attempts WHERE notification_id = $1 ORDER BY attempt_no', [req.params.id]);
    res.json({ ...result.rows[0], deliveryAttempts: attempts.rows });
  } catch (error) { next(error); }
});

app.get('/notifications', async (req, res, next) => {
  try {
    const limit = Math.min(Math.max(Number.parseInt(req.query.limit, 10) || 50, 1), 100);
    const offset = Math.max(Number.parseInt(req.query.offset, 10) || 0, 0);
    const status = req.query.status;
    if (status && !['queued', 'processing', 'retrying', 'sent', 'failed'].includes(status)) return res.status(400).json({ error: 'Invalid status filter' });
    const result = await pool.query(`SELECT id, channel, recipient, template, status, attempts, last_error, created_at, updated_at FROM notifications ${status ? 'WHERE status = $3' : ''} ORDER BY created_at DESC LIMIT $1 OFFSET $2`, status ? [limit, offset, status] : [limit, offset]);
    res.json({ notifications: result.rows, limit, offset });
  } catch (error) { next(error); }
});

app.post('/notifications/:id/retry', async (req, res, next) => {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const result = await client.query("UPDATE notifications SET status = 'queued', attempts = 0, last_error = NULL, updated_at = NOW() WHERE id = $1 AND status = 'failed' RETURNING id, channel", [req.params.id]);
    if (!result.rowCount) {
      await client.query('ROLLBACK');
      const found = await pool.query('SELECT status FROM notifications WHERE id = $1', [req.params.id]);
      return found.rowCount ? res.status(409).json({ error: `Cannot retry notification with status ${found.rows[0].status}` }) : res.status(404).json({ error: 'Notification not found' });
    }
    await publish(result.rows[0].channel, { notificationId: result.rows[0].id, channel: result.rows[0].channel });
    await client.query('COMMIT');
    res.status(202).json({ id: result.rows[0].id, status: 'queued' });
  } catch (error) {
    await client.query('ROLLBACK').catch(() => {});
    next(error);
  } finally { client.release(); }
});

app.get('/stats', async (_req, res, next) => {
  try {
    const [counts, throughput, queues] = await Promise.all([
      pool.query("SELECT status, COUNT(*)::int AS count FROM notifications GROUP BY status"),
      pool.query("SELECT date_trunc('minute', created_at) AS time, COUNT(*)::int AS count FROM notifications WHERE created_at > NOW() - INTERVAL '30 minutes' GROUP BY 1 ORDER BY 1"),
      Promise.all(['email', 'sms', 'push', 'dlq'].map(async (name) => {
        try { return [name, await channel.checkQueue(name === 'dlq' ? 'queue.dlq' : `queue.${name}`)]; } catch { return [name, { messageCount: 0 }]; }
      })),
    ]);
    const statuses = Object.fromEntries(['queued', 'processing', 'retrying', 'sent', 'failed'].map((status) => [status, 0]));
    for (const row of counts.rows) statuses[row.status] = row.count;
    res.json({ counts: statuses, throughput: throughput.rows.map((row) => ({ time: row.time, count: row.count })), queues: Object.fromEntries(queues.map(([name, queue]) => [name, queue.messageCount])) });
  } catch (error) { next(error); }
});

app.use((error, _req, res, _next) => {
  console.error(error);
  res.status(500).json({ error: 'Internal server error' });
});

const port = Number(process.env.PORT || 3000);
await connectQueue();
const server = app.listen(port, () => console.log(`NotifyFlow API listening on ${port}`));

async function shutdown() {
  server.close();
  await Promise.allSettled([pool.end(), redis.quit(), channel?.close(), connection?.close()]);
  process.exit(0);
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
