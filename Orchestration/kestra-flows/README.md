# Kestra Local Development Setup

This directory contains the infrastructure setup for running Kestra locally with Docker Compose.

## Prerequisites

- Docker and Docker Compose installed
- Terraform applied (GCP resources created)
- Python virtual environment activated (for scripts)

## Setup Instructions

1. **Apply Terraform Configuration**
   ```bash
   cd terraform
   terraform init
   terraform plan
   terraform apply
   ```

2. **Configure Environment Variables**
   ```bash
   cp .env.example .env
   # Edit .env with your actual values from Terraform outputs
   ```

3. **Get Service Account Key**
   ```bash
   cd terraform
   terraform output service_account_key > ../service_account_key.json
   # Base64 encode the key for Docker Compose
   cat ../service_account_key.json | base64 -w 0
   # Copy the output to SERVICE_ACCOUNT_KEY in .env
   ```

4. **Start Kestra**
   ```bash
   docker-compose up -d
   ```

5. **Access Kestra UI**
   - Web UI: http://localhost:8080
   - API: http://localhost:8081

## Services

- **Kestra**: Workflow orchestration platform
- **PostgreSQL**: Database for Kestra metadata and queues

## Development Workflow

1. Create flows in the `kestra-flows/` directory
2. Flows will be automatically loaded by Kestra
3. Use the web UI to monitor and trigger executions

## Production Deployment

For production, consider:
- Kestra Cloud (managed service)
- GKE + Cloud SQL
- Cloud Run + Cloud SQL

## Cleanup

```bash
docker-compose down -v  # Remove containers and volumes
```