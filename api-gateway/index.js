import express from "express";
import cors from "cors";
import { createProxyMiddleware } from "http-proxy-middleware";
import { verifyToken } from "./auth.middleware.js";

const app = express();
app.use(cors());

// Health Check
app.get("/health", (req, res) => res.status(200).send("API Gateway OK"));

// Proxy payment requests, enforcing JWT authorization
app.use(
  "/api/payment",
  verifyToken, 
  createProxyMiddleware({
    // If running in K8s, it uses 'payment-service', else localhost
    target: process.env.PAYMENT_SERVICE_URL || "http://localhost:8000",
    changeOrigin: true,
    pathRewrite: {
      "^/api/payment": "/payment-service", 
    },
  })
);

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => console.log(`🚀 API Gateway securely routing traffic on port ${PORT}`));
