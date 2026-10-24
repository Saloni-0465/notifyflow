import nodemailer from 'nodemailer';

const failureRate = Math.min(Math.max(Number(process.env.FAIL_RATE ?? 0.2), 0), 1);
const emailTransport = process.env.EMAIL_TRANSPORT || 'mock';
let transporter;
if (emailTransport === 'smtp') {
  if (!process.env.SMTP_HOST || !process.env.SMTP_USER || !process.env.SMTP_PASS) throw new Error('SMTP_HOST, SMTP_USER, and SMTP_PASS are required when EMAIL_TRANSPORT=smtp');
  transporter = nodemailer.createTransport({ host: process.env.SMTP_HOST, port: Number(process.env.SMTP_PORT || 587), secure: Number(process.env.SMTP_PORT) === 465, auth: { user: process.env.SMTP_USER, pass: process.env.SMTP_PASS } });
}

const interpolate = (text, payload) => text.replace(/{{\s*(\w+)\s*}}/g, (_match, key) => String(payload[key] ?? ''));

export async function send({ channel, recipient, template, payload }) {
  if (recipient === 'fail@test.com' || recipient.includes(':always-fail')) throw new Error('Configured demo failure for recipient');
  if (channel === 'email' && transporter) {
    const subject = template === 'welcome' ? 'Welcome!' : template === 'order_placed' ? 'Your order is confirmed' : `Notification: ${template}`;
    const text = template === 'welcome'
      ? `Hello ${payload.name || 'there'}, welcome aboard!`
      : template === 'order_placed'
        ? `Hello ${payload.name || 'there'}, your order ${payload.orderId || ''} has been placed.`
        : `${template}\n${JSON.stringify(payload, null, 2)}`;
    return transporter.sendMail({ from: process.env.EMAIL_FROM || 'NotifyFlow <notifications@example.com>', to: recipient, subject: interpolate(subject, payload), text: interpolate(text, payload) });
  }
  await new Promise((resolve) => setTimeout(resolve, 100 + Math.floor(Math.random() * 201)));
  if (channel !== 'email' && Math.random() < failureRate) throw new Error(`Mock ${channel} provider failed (FAIL_RATE=${failureRate})`);
  if (channel === 'email' && emailTransport !== 'mock') throw new Error(`Unsupported email transport: ${emailTransport}`);
  console.log(JSON.stringify({ event: 'notification.delivered', channel, recipient, template }));
  return { messageId: `mock-${Date.now()}` };
}
