# MDRP — Session Context for Next Claude Chat

Paste this file at the start of your next conversation.

---

## What This Project Is

**Market Data Reliability Platform (MDRP)** — a production-grade real-time data pipeline
that ingests energy market data (TTF, NBP, Brent, WTI, EU ETS forward curves), validates
and normalises it, stores immutable raw events in S3, loads trusted data to Snowflake,
serves the latest curves via Redis, and provides full observability + replay capability.

Built to demonstrate: real-time data systems, AWS at scale, end-to-end ownership, and
hard infrastructure problems (not dashboards). Mercuria context — commodity trading firm.

**GitHub:** https://github.com/lokeshpeta6y-cloud/market-data-reliability-platform
**Git author:** Lokesh P / lokeshpeta6y-cloud@users.noreply.github.com  
**Local path:** `c:\Users\lpeta\PycharmProjects\AI projects\Market Data Reliability Platform`

---

## What Is Fully Built (20 commits, all pushed to GitHub)

### Shared Library (`libs/common/src/mdrp_common/`)
- `models.py` — Pydantic v2 domain models: RawMarketEvent, ValidatedMarketEvent, CurveEvent,
  DLQEvent, ForwardCurveSnapshot, ReplayJob, ProviderHealthSnapshot + all enums
- `kafka_client.py` — MdrpProducer (idempotent, lz4, acks=all), MdrpConsumer (manual commits),
  Topics class with 5 topic constants, ensure_topics(), producer_context()
- `metrics.py` — 30+ Prometheus metrics (EVENTS_INGESTED_TOTAL, DLQ_EVENTS_TOTAL,
  BRONZE_WRITE_DURATION_SECONDS, CONSUMER_LAG, QUALITY_SCORE, etc.)
- `settings.py` — BaseServiceSettings (pydantic-settings), all env vars including Kafka,
  Redis, S3/MinIO, Snowflake, Databento, OTel
- `storage.py` — BronzeStorageClient: write_parquet_batch() Snappy Parquet to S3,
  list_partitions() for replay, read_parquet_batch(), ensure_bucket() for MinIO
  Partition key: `bronze/{provider}/{YYYY-MM-DD}/{HH}/events_{batch_id}.parquet`

### Services (`services/`)
| Service | Port | Key details |
|---|---|---|
| provider-emulator | 8001 | Ornstein-Uhlenbeck price process, 7 fault types (dup 2%, malformed 1%, delayed 5%, OOO 3%, schema drift 0.5%, stale 1%, partial curve 2%) |
| validation-service | 8002 | Redis SETNX dedup, QualityScorer with penalty table per FaultType, routes to DLQ |
| bronze-writer | 8003 | Batches to Parquet (500 events or 30s), S3 write, skips offset commit on failure |
| normalization-service | 8004 | TenorMapper (14 regex patterns), InstrumentMapper, Redis INCR for version |
| redis-writer | 8005 | PIPELINE transactions, ZREMRANGEBYRANK history trim, staleness detection |
| replay-engine | 8006 | 3 modes: Bronze S3 replay, DLQ replay (offsets_for_times), Databento historical |
| ops-api | 8007 | FastAPI: health, curves, replay, dlq, alerts routers. AlertRouter → Teams + SMTP |
| silver-loader | 8008 | Snowflake PUT+COPY INTO, ON_ERROR=CONTINUE, linear-backoff reconnect |
| gold-loader | 8009 | Tumbling windows, MERGE INTO on curve_name+as_of, min completeness 0.80 |

### Infrastructure
- `docker-compose.yml` — full stack: redpanda, redis, minio, prometheus, grafana,
  alertmanager, jaeger + all 9 services on mdrp-network
- `config/prometheus/alerts/` — 13 alert rules (ProviderOutage, DLQSpike,
  ConsumerLagHigh, StaleCurveData, RedisDown, SnowflakeLoadFailure, etc.)
- `config/alertmanager/alertmanager.yml` — webhook to ops-api, inhibit rules
- `config/grafana/dashboards/pipeline-overview.json` — 9-panel dashboard
- `infra/terraform/` — modules: networking (VPC), s3 (Bronze bucket + lifecycle),
  ecs (Fargate cluster + task defs + ALB), secrets (7 Secrets Manager secrets),
  eventbridge (2 replay schedules). All us-east-1.
- `infra/snowflake/` — 6 DDL scripts: database, schemas, tables, roles, stages, views
- `tests/` — unit (models, tenor_mapper, validator with fakeredis, fault_injector),
  integration (INTEGRATION_TESTS=true), chaos (CHAOS_TESTS=true)
- `.devcontainer/devcontainer.json` — Docker-in-Docker, Python 3.11, port forwarding

### Demo layer (`demo/`)
- `demo/api.py` — standalone FastAPI with realistic mock data, zero external deps.
  Runs with: `demo\.venv\Scripts\python demo\api.py` → http://localhost:8007/docs
- `demo/dashboard.html` — dark-theme ops dashboard. Open directly in browser.
  Auto-refreshes every 8s. Shows: KPIs, pipeline services, Kafka topics, provider
  quality scores, DLQ breakdown, forward curves, recent DLQ events.

---

## Key Technical Decisions

| Decision | Choice | Why |
|---|---|---|
| Streaming | Redpanda | Kafka-compatible, no ZooKeeper, lighter for local dev |
| Bronze format | Parquet + Snappy on S3/MinIO | Columnar, queryable with Athena in prod |
| Deduplication | Redis SETNX with TTL | O(1), atomic, race-safe |
| Serving cache | Redis Hash + Sorted Set | <5ms reads, versioned history |
| Tracing | OpenTelemetry → Jaeger | Vendor-neutral |
| Snowflake loading | COPY INTO with staging | Supports replay idempotency |
| Local S3 | MinIO | S3-compatible, same boto3 code path |
| Alert routing | AlertManager → ops-api webhook → Teams + SMTP | Decoupled dedup/grouping |
| Fault injection | Config-driven emulator | Reproducible named scenarios |

---

## Data Model (Energy Forward Curves)

Instruments: TTF (EUR/MWh), NBP (EUR/MWh), BRENT (USD/bbl), WTI (USD/bbl), EU_ETS (EUR/t CO2)

Tenors: monthly (2025-06), quarterly (2025-Q3), seasonal, calendar year (2026-CAL)

Kafka topics:
- `market.events.raw` — from emulator, pre-validation
- `market.events.validated` — passed validation
- `market.events.normalized` — canonical curve events
- `market.events.dlq` — failed validation
- `market.events.replay` — replay engine output

Bronze S3 path: `bronze/{provider}/{YYYY-MM-DD}/{HH}/events_{batch_id}.parquet`

---

## Accounts & Credentials (fill in your own)

| Service | Account | Notes |
|---|---|---|
| GitHub | lokeshpeta6y-cloud | Repo already pushed |
| AWS | Not created yet | us-east-1, free tier |
| Snowflake | ME29964.us-east-1 | US East Ohio, $400 free credit |
| Databento | Not created yet | $125 free credit, optional |

Credentials go in `.env` (already git-ignored). Never paste in chat.

Snowflake DDL to run (in order, in Snowflake Worksheet):
```
infra/snowflake/001_database_and_warehouses.sql
infra/snowflake/002_schemas.sql
infra/snowflake/003_tables.sql
infra/snowflake/004_roles_and_grants.sql
infra/snowflake/005_stages_and_pipes.sql
infra/snowflake/006_views.sql
```

---

## Environment Constraints (this machine)

- **OS:** Windows 11 Enterprise, Mercuria corporate laptop
- **No admin access** — cannot install software or enable Hyper-V
- **Corporate proxy** — TLS interception active (git push works via HTTPS/443)
- **Firewall blocks** — WebSocket connections, SSH tunnels, non-standard ports
  - GitHub Codespaces: blocked (browser WebSocket)
  - VS Code Codespaces: blocked (ECONNRESET on SSH tunnel)
  - Docker Desktop: installed but fails to start (needs admin/WSL2)
- **Python 3.12.2** — available locally
- **Git Credential Manager 2.7.3** — handles GitHub auth via browser

---

## What's Running Right Now

Demo API: `demo\.venv\Scripts\python demo\api.py`
- Swagger UI: http://localhost:8007/docs
- Dashboard: open `demo\dashboard.html` in browser

To restart if port 8007 is stuck:
```powershell
$p = Get-NetTCPConnection -LocalPort 8007 | Select -ExpandProperty OwningProcess -First 1
Stop-Process -Id $p -Force
```

---

## What's Left To Do

### To run the full stack (needs Docker — do on a personal machine)
```bash
git clone https://github.com/lokeshpeta6y-cloud/market-data-reliability-platform.git
cd market-data-reliability-platform
cp .env.example .env
docker compose up -d
```
Then open:
- Grafana: http://localhost:3000 (admin/admin)
- Ops API: http://localhost:8007/docs
- Prometheus: http://localhost:9090
- Jaeger: http://localhost:16686
- MinIO: http://localhost:9001 (minioadmin/minioadmin)
- Redpanda: http://localhost:9644

### To deploy to AWS
1. Create AWS account (aws.amazon.com, us-east-1)
2. Run `make tf-init && make tf-plan && make tf-apply`
3. Push images: `make push`

### Optional enhancements not yet built
- GitHub Actions CI/CD (`.github/workflows/ci.yml`)
- Bootstrap script for Terraform S3 state bucket (`scripts/bootstrap.sh`)
- ECS Auto Scaling policies based on consumer lag
- Live Databento integration (needs API key from databento.com)

---

## How to Continue

Tell Claude:
> "Read CONTEXT.md in the project root. Continue building the MDRP platform."

Then specify what you want next — e.g.:
- "Add GitHub Actions CI/CD"
- "Wire up real Snowflake credentials and run the DDL"
- "Run the full docker compose stack" (needs personal machine with Docker)
- "Add ECS auto-scaling to the Terraform modules"
