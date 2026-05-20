# Ops API

The operational control plane for the Market Data Reliability Platform.
A FastAPI service exposing REST endpoints for pipeline health monitoring,
curve inspection, replay management, DLQ analysis, and alert routing.

## Endpoints

### Health & Status

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Liveness probe — returns 200 if process is up |
| GET | `/api/v1/status` | Connectivity check: Kafka, Redis, MinIO |
| GET | `/api/v1/providers` | All provider health snapshots |
| GET | `/api/v1/providers/{provider}` | Single provider detail |

### Curves

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/v1/curves` | List all instruments with snapshot summary |
| GET | `/api/v1/curves/{instrument}` | Full ForwardCurveSnapshot |
| GET | `/api/v1/curves/{instrument}/history` | Last N snapshots (sorted set) |

### Replay

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/v1/replay` | Submit replay job |
| GET | `/api/v1/replay` | List recent jobs |
| GET | `/api/v1/replay/{job_id}` | Job status and progress |

### DLQ

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/v1/dlq` | DLQ stats: depth, categories, recent entries |
| POST | `/api/v1/dlq/replay` | Submit DLQ replay for a time window |

### Alerts

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/v1/alerts/webhook` | Receive AlertManager webhook |

## Alert Routing

On receiving a webhook the service:
1. Returns 200 immediately (never blocks AlertManager)
2. Dispatches to Teams and/or SMTP in background tasks

Configure via environment variables:

| Variable | Description |
|----------|-------------|
| `ALERT_TEAMS_ENABLED` | Enable Teams notifications (true/false) |
| `TEAMS_WEBHOOK_URL` | Teams incoming webhook URL |
| `ALERT_EMAIL_ENABLED` | Enable SMTP email notifications (true/false) |
| `SMTP_HOST` | SMTP server hostname |
| `SMTP_PORT` | SMTP port (default: 587) |
| `SMTP_USER` | SMTP authentication username |
| `SMTP_PASSWORD` | SMTP authentication password |
| `SMTP_FROM` | Sender email address |
| `SMTP_TO` | Comma-separated recipient list |

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka brokers |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis connection |
| `S3_ENDPOINT_URL` | *(unset)* | MinIO endpoint |
| `S3_BUCKET_BRONZE` | `mdrp-bronze` | Bronze bucket |
| `PORT` | `8080` | Uvicorn listen port |
| `METRICS_PORT` | `8007` | Prometheus metrics port |
| `CORS_ORIGINS` | `*` | Comma-separated allowed origins |
| `OTEL_ENABLED` | `true` | Enable OpenTelemetry tracing |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://jaeger:4317` | OTLP collector |
| `LOG_LEVEL` | `INFO` | Logging verbosity |

## Running Locally

```bash
# With Docker Compose (recommended)
docker compose up ops-api

# Directly
pip install -e libs/common
pip install -e services/replay-engine
pip install -e services/ops-api
uvicorn ops_api.main:app --host 0.0.0.0 --port 8080 --reload
```

## API Documentation

Once running, interactive docs are available at:
- Swagger UI: http://localhost:8080/docs
- ReDoc: http://localhost:8080/redoc
- OpenAPI JSON: http://localhost:8080/openapi.json

## Architecture Notes

- All route handlers are `async` — no blocking I/O on the event loop.
- Synchronous clients (boto3, confluent-kafka admin, smtplib) run in thread-pool executors.
- Shared resources (Redis, Kafka producer, storage client, JobStore) are created once at startup and stored on `app.state`.
- The `JobStore` from the replay-engine package is imported directly so the ops-api and replay-engine share the same Redis schema without duplication.
