import http from 'k6/http';
import { check } from 'k6';

export const options = {
  scenarios: {
    notifications: {
      executor: 'constant-arrival-rate',
      rate: 200,
      timeUnit: '1s',
      duration: '50s',
      preAllocatedVUs: 50,
      maxVUs: 500,
    },
  },
  thresholds: { http_req_failed: ['rate<0.01'], http_req_duration: ['p(95)<250', 'p(99)<500'] },
};

export default function () {
  const seq = `${__VU}-${__ITER}-${Date.now()}`;
  const response = http.post(`${__ENV.API_URL || 'http://localhost:3000'}/notifications`, JSON.stringify({
    idempotencyKey: `load-${seq}`,
    channel: 'sms',
    recipient: `+1555${String(__VU).padStart(4, '0')}${String(__ITER).padStart(4, '0')}`,
    template: 'load_test',
    payload: { run: '10k' },
  }), { headers: { 'Content-Type': 'application/json' } });
  check(response, { 'accepted or duplicate': (res) => res.status === 202 || res.status === 200 });
}
