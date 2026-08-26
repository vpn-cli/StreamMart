# StreamMart Architecture 🚀
[![Node.js CI](https://github.com/your-username/stream-mart/actions/workflows/ci.yml/badge.svg)](https://github.com/your-username/stream-mart/actions/workflows/ci.yml)

An enterprise-grade, event-driven e-commerce microservices architecture.

## Overview
This repository contains a decoupled backend system utilizing **Apache Kafka** for event streaming and asynchronous communication. Each service is containerized via **Docker**, orchestrated using **Kubernetes**, and natively integrated with **Neon (Serverless PostgreSQL)** via **Drizzle ORM** for persistence. 

## Services Include
- **Client (Next.js)**: Modern frontend for interacting with endpoints.
- **Payment Service**: Submits payments to the database and streams `payment-successful` events.
- **Order Service**: Consumes payment events, generates secure order records, and publishes downstream events.
- **Email Service**: Consumes successful order events to dispatch asynchronous email receipts.
- **Analytic Service**: Aggregates all events to populate a unified metrics dashboard.

## Tech Stack
- **Node.js & Express.js**
- **Apache Kafka & Zookeeper**
- **Kubernetes & Docker**
- **Neon (Serverless PostgreSQL)**
- **Drizzle ORM**
- **Jest & Supertest** for integration testing
- **GitHub Actions** for CI/CD

## Running Locally
1. Configure your keys via `.env` (copied from `.env.template`).
2. Start Kubernetes infrastructure (Kafka, Zookeeper, Microservices):
   ```bash
   kubectl apply -f k8s/infrastructure.yaml
   kubectl apply -f k8s/microservices.yaml
   ```

*View `.github/workflows/ci.yml` for automated testing pipeline details.*
