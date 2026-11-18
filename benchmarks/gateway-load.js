import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  vus: 20,
  duration: '30s',
  thresholds: {
    http_req_failed: ['rate<0.01'], // http errors should be less than 1%
  },
};

const BASE_URL = __ENV.GATEWAY_URL || 'http://localhost:8085';

export function setup() {
  const authRes = http.post(
    `${BASE_URL}/api/auth/tokens/batch`,
    JSON.stringify({ count: 1000, prefix: 'k6_user' }),
    { headers: { 'Content-Type': 'application/json' } }
  );

  if (authRes.status !== 200) {
    throw new Error(`Failed to acquire batch JWTs from Gateway: ${authRes.status} ${authRes.body}`);
  }

  const tokens = JSON.parse(authRes.body).tokens;
  return { tokens };
}

export default function (data) {
  // Distribute across ~1,000 unique user tokens per VU and iteration
  const userIdx = (__VU * 1000 + __ITER) % data.tokens.length;
  const userObj = data.tokens[userIdx];

  const payload = JSON.stringify({
    cart: [
      { id: 'k6_item_1', name: 'Cloud Native T-Shirt', price: 29.99 },
      { id: 'k6_item_2', name: 'Distributed Systems Cap', price: 19.50 },
    ],
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${userObj.token}`,
    },
  };

  const res = http.post(`${BASE_URL}/api/payment`, payload, params);

  check(res, {
    'status is 200': (r) => r.status === 200,
  });

  sleep(0.05);
}

