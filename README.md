# Distributed Task Queue System

This project implements a distributed task queue system where clients submit tasks
that are processed asynchronously by worker nodes.

## Architecture Overview
- API Service (FastAPI)
- Worker Service (Python)
- RabbitMQ (Message Broker)
- Redis (Cache)
- PostgreSQL (Persistent Storage)

## How to Run (Local)
Requirements:
- Docker
- Docker Compose

Steps:
1. Clone repository
2. Run: docker compose up --build
3. Access API: http://localhost:8000/docs

## Features
- Asynchronous task processing
- Distributed workers
- Fault tolerance via message queue
- Containerized deployment

## Author
[Your Name]
