import express from "express";
import cors from "cors";
import jwt from "jsonwebtoken";
import { createProxyMiddleware } from "http-proxy-middleware";
import { verifyToken } from "./auth.middleware.js";

const app = express();
app.use(cors());

// Health Check endpoint for Docker Compose & orchestration
app.get("/health", (req, res) => res.status(200).send("API Gateway OK"));

const JWT_SECRET = process.env.JWT_SECRET || "streammart-super-secure-jwt-secret-key-32-bytes!";

// Demo token issuing endpoint for clients, tests, and benchmarks.
// SECURITY GUARD: Strictly disabled in production mode (NODE_ENV=production) or when DEMO_AUTH is not explicitly "true".
app.post("/api/auth/token", express.json(), (req, res) => {
  const isProduction = process.env.NODE_ENV === "production";
  const allowDemoAuth = process.env.DEMO_AUTH === "true" && !isProduction;

  if (!allowDemoAuth) {
    return res.status(404).json({ error: "Not Found" });
  }

  const userId = req.body?.userId || "user_demo_123";
  const token = jwt.sign({ userId }, JWT_SECRET, { expiresIn: "24h" });
  res.status(200).json({ token, userId });
});

// Proxy payment requests, enforcing JWT authorization
// Target: internal payment-service within Docker Compose network
app.use(
  "/api/payment",
  verifyToken, 
  createProxyMiddleware({
    target: process.env.PAYMENT_SERVICE_URL || "http://payment-service:8000",
    changeOrigin: true,
    pathRewrite: {
      "^/api/payment": "/payment", 
    },
  })
);

let server;
const PORT = process.env.PORT || 8080;
if (process.env.NODE_ENV !== "test") {
  server = app.listen(PORT, () => console.log(`🚀 API Gateway securely routing traffic on port ${PORT}`));
}

// Graceful shutdown handling
const shutdown = () => {
  console.log("API Gateway: shutting down gracefully...");
  if (server) {
    server.close(() => {
      console.log("API Gateway closed.");
      process.exit(0);
    });
  } else {
    process.exit(0);
  }
};

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);

export default app;
