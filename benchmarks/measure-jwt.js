import jwt from '../api-gateway/node_modules/jsonwebtoken/index.js';
import jws from '../api-gateway/node_modules/jws/index.js';
import crypto from 'node:crypto';
import { performance } from 'node:perf_hooks';

const secret = process.env.JWT_SECRET || 'streammart-super-secure-jwt-secret-key-32-bytes!';
const keyObj = crypto.createSecretKey(Buffer.from(secret));
const token = jwt.sign({ userId: 'k6_benchmark_user' }, secret, { expiresIn: '24h' });
const iters = 10000;

console.log('===============================================================');
console.log(`🔐 JWT Verification Benchmark (${iters.toLocaleString()} iterations)`);
console.log('===============================================================');

// 1. Raw HMAC-SHA256
{
  const [header, payload] = token.split('.');
  const data = `${header}.${payload}`;
  const start = performance.now();
  for (let i = 0; i < iters; i++) {
    crypto.createHmac('sha256', secret).update(data).digest('base64url');
  }
  const dur = performance.now() - start;
  const usPerOp = (dur / iters) * 1000;
  const opsSec = (iters / (dur / 1000));
  console.log(`1. Raw crypto.createHmac (SHA256):     ${usPerOp.toFixed(2)} µs/op | ${opsSec.toFixed(0)} ops/s`);
}

// 2. jws.verify
{
  const start = performance.now();
  for (let i = 0; i < iters; i++) {
    jws.verify(token, 'HS256', secret);
  }
  const dur = performance.now() - start;
  const usPerOp = (dur / iters) * 1000;
  const opsSec = (iters / (dur / 1000));
  console.log(`2. jws.verify (HS256):                 ${usPerOp.toFixed(2)} µs/op | ${opsSec.toFixed(0)} ops/s`);
}

// 3. jwt.verify with KeyObject (bypasses createPublicKey attempt)
{
  const start = performance.now();
  for (let i = 0; i < iters; i++) {
    jwt.verify(token, keyObj);
  }
  const dur = performance.now() - start;
  const usPerOp = (dur / iters) * 1000;
  const opsSec = (iters / (dur / 1000));
  console.log(`3. jwt.verify with KeyObject:          ${usPerOp.toFixed(2)} µs/op | ${opsSec.toFixed(0)} ops/s`);
}

// 4. jwt.verify with raw string secret (standard library call)
{
  const start = performance.now();
  for (let i = 0; i < iters; i++) {
    jwt.verify(token, secret);
  }
  const dur = performance.now() - start;
  const usPerOp = (dur / iters) * 1000;
  const opsSec = (iters / (dur / 1000));
  console.log(`4. jwt.verify with string secret:      ${usPerOp.toFixed(2)} µs/op | ${opsSec.toFixed(0)} ops/s`);
}

console.log('===============================================================');
