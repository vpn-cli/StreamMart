import test from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import jwt from "jsonwebtoken";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const gatewayEntry = path.resolve(__dirname, "..", "index.js");

// Set test environment (use CI-provided JWT_SECRET if set, otherwise fixed test secret)
process.env.NODE_ENV = "test";
process.env.JWT_SECRET =
  process.env.JWT_SECRET || "test-jwt-secret-key-for-api-gateway-verification-32!";

const { default: app } = await import("../index.js");
const { verifyToken } = await import("../auth.middleware.js");

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

  await t.test("Sign + verify round-trip uses the same secret across index.js and auth.middleware.js", async () => {
    const originalEnv = process.env.NODE_ENV;
    const originalDemo = process.env.DEMO_AUTH;
    try {
      process.env.NODE_ENV = "development";
      process.env.DEMO_AUTH = "true";

      const res = await fetch(`${baseUrl}/api/auth/token`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ userId: "roundtrip_user" }),
      });

      assert.equal(res.status, 200);
      const { token } = await res.json();
      assert.ok(token, "Token should be issued by gateway sign route");

      // Verify the signed token through auth.middleware.js verifyToken
      const req = { headers: { authorization: `Bearer ${token}` } };
      let statusCode = null;
      let responseBody = null;
      const mockRes = {
        status(code) {
          statusCode = code;
          return this;
        },
        json(body) {
          responseBody = body;
          return this;
        },
      };
      let nextCalled = false;
      verifyToken(req, mockRes, () => {
        nextCalled = true;
      });

      assert.equal(nextCalled, true, "verifyToken must accept token signed by index.js");
      assert.equal(statusCode, null, `Unexpected error status: ${statusCode} ${JSON.stringify(responseBody)}`);
      assert.equal(req.user.userId, "roundtrip_user");
      assert.equal(req.headers["x-user-id"], "roundtrip_user");

      // Confirm token signed with a different secret is rejected by verifyToken
      const wrongToken = jwt.sign({ userId: "roundtrip_user" }, "d".repeat(32), { expiresIn: "1h" });
      const badReq = { headers: { authorization: `Bearer ${wrongToken}` } };
      let badStatus = null;
      let badNextCalled = false;
      verifyToken(
        badReq,
        {
          status(code) {
            badStatus = code;
            return this;
          },
          json() {
            return this;
          },
        },
        () => {
          badNextCalled = true;
        }
      );
      assert.equal(badNextCalled, false, "verifyToken must reject token signed with a different secret");
      assert.equal(badStatus, 403);
    } finally {
      process.env.NODE_ENV = originalEnv;
      process.env.DEMO_AUTH = originalDemo;
    }
  });

  await t.test("Gateway refuses to start when JWT_SECRET is missing", () => {
    const env = { ...process.env, NODE_ENV: "development", PORT: "0" };
    delete env.JWT_SECRET;

    const result = spawnSync(process.execPath, [gatewayEntry], {
      env,
      encoding: "utf8",
      timeout: 5000,
    });

    assert.notEqual(result.status, 0, "Gateway must exit non-zero when JWT_SECRET is missing");
    assert.match(result.stderr, /JWT_SECRET/i, "Startup error must clearly mention JWT_SECRET");
  });

  await t.test("Gateway refuses to start when JWT_SECRET is shorter than 32 characters", () => {
    const env = { ...process.env, NODE_ENV: "development", PORT: "0", JWT_SECRET: "a".repeat(31) };

    const result = spawnSync(process.execPath, [gatewayEntry], {
      env,
      encoding: "utf8",
      timeout: 5000,
    });

    assert.notEqual(result.status, 0, "Gateway must exit non-zero when JWT_SECRET is shorter than 32 chars");
    assert.match(result.stderr, /32/, "Startup error must mention the 32-character minimum");
  });
});
