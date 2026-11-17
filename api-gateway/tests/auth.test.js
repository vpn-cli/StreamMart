import test from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import jwt from "jsonwebtoken";

// Set test environment
process.env.NODE_ENV = "test";
process.env.JWT_SECRET = "test-jwt-secret-key-for-api-gateway-verification-32!";

const { default: app } = await import("../index.js");

function createTestServer() {
  return new Promise((resolve) => {
    const server = http.createServer(app);
    server.listen(0, () => {
      const port = server.address().port;
      resolve({
        server,
        url: `http://localhost:${port}`,
        close: () => new Promise((res) => server.close(res)),
      });
    });
  });
}

test("API Gateway Auth & Endpoint Security Suite", async (t) => {
  const testContext = await createTestServer();
  const baseUrl = testContext.url;

  t.after(async () => {
    await testContext.close();
  });

  await t.test("GET /api/auth/token is completely removed and returns 404", async () => {
    const res = await fetch(`${baseUrl}/api/auth/token`);
    assert.equal(res.status, 404, "GET /api/auth/token must be unavailable (404)");
  });

  await t.test("POST /api/auth/token is blocked (404) in production mode even if DEMO_AUTH=true", async () => {
    const originalEnv = process.env.NODE_ENV;
    const originalDemo = process.env.DEMO_AUTH;
    try {
      process.env.NODE_ENV = "production";
      process.env.DEMO_AUTH = "true";

      const res = await fetch(`${baseUrl}/api/auth/token`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ userId: "attacker" }),
      });

      assert.equal(res.status, 404, "POST /api/auth/token must return 404 in production mode");
      const body = await res.json();
      assert.equal(body.error, "Not Found");
    } finally {
      process.env.NODE_ENV = originalEnv;
      process.env.DEMO_AUTH = originalDemo;
    }
  });

  await t.test("POST /api/auth/token is blocked (404) when DEMO_AUTH is not 'true'", async () => {
    const originalDemo = process.env.DEMO_AUTH;
    try {
      delete process.env.DEMO_AUTH;

      const res = await fetch(`${baseUrl}/api/auth/token`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ userId: "some_user" }),
      });

      assert.equal(res.status, 404, "POST /api/auth/token must return 404 without DEMO_AUTH=true");
    } finally {
      process.env.DEMO_AUTH = originalDemo;
    }
  });

  await t.test("POST /api/auth/token succeeds (200) only when DEMO_AUTH=true and NODE_ENV!=production", async () => {
    const originalEnv = process.env.NODE_ENV;
    const originalDemo = process.env.DEMO_AUTH;
    try {
      process.env.NODE_ENV = "development";
      process.env.DEMO_AUTH = "true";

      const res = await fetch(`${baseUrl}/api/auth/token`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ userId: "test_verified_user" }),
      });

      assert.equal(res.status, 200, "POST /api/auth/token should succeed in demo mode");
      const data = await res.json();
      assert.equal(data.userId, "test_verified_user");
      assert.ok(data.token, "Should return a signed JWT token");

      // Verify token cryptographic signature
      const decoded = jwt.verify(data.token, process.env.JWT_SECRET);
      assert.equal(decoded.userId, "test_verified_user");
    } finally {
      process.env.NODE_ENV = originalEnv;
      process.env.DEMO_AUTH = originalDemo;
    }
  });

  await t.test("POST /api/payment rejects unauthenticated requests with 401", async () => {
    const res = await fetch(`${baseUrl}/api/payment`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ cart: [] }),
    });

    assert.equal(res.status, 401, "Requests without Authorization header must return 401");
  });

  await t.test("POST /api/payment rejects invalid token with 403 Forbidden", async () => {
    const res = await fetch(`${baseUrl}/api/payment`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": "Bearer forged.invalid.token",
      },
      body: JSON.stringify({ cart: [] }),
    });

    assert.equal(res.status, 403, "Requests with invalid token signature must return 403");
  });
});
