#!/bin/bash
exec > /var/log/mdrp-init.log 2>&1

echo "=== MDRP bootstrap start ==="
export AWS_DEFAULT_REGION="us-east-2"
SECRET_PREFIX="mdrp/prod"

echo "--- Installing Docker and git ---"
dnf install -y docker git jq
systemctl enable --now docker
usermod -aG docker ec2-user

echo "--- Installing Docker Compose v2 ---"
mkdir -p /usr/local/lib/docker/cli-plugins
curl -fsSL "https://github.com/docker/compose/releases/download/v2.27.0/docker-compose-linux-x86_64" \
  -o /usr/local/lib/docker/cli-plugins/docker-compose
chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
docker compose version

echo "--- Cloning repo ---"
cd /opt
git clone "https://github.com/lokeshpeta6y-cloud/market-data-reliability-platform.git" mdrp
cd /opt/mdrp

echo "--- Fetching secrets ---"
get_secret() {
  aws secretsmanager get-secret-value \
    --secret-id "${SECRET_PREFIX}/$1" \
    --region "${AWS_DEFAULT_REGION}" \
    --query SecretString --output text 2>/dev/null || echo "MISSING"
}

SF_ACCOUNT=$(get_secret "snowflake-account")
SF_USER=$(get_secret "snowflake-user")
SF_PAT=$(get_secret "snowflake-pat-token")
DB_KEY=$(get_secret "databento-api-key")

echo "--- Writing .env ---"
cat > /opt/mdrp/.env << 'ENVEOF'
KAFKA_BOOTSTRAP_SERVERS=redpanda:9092
KAFKA_CONSUMER_GROUP_PREFIX=mdrp
KAFKA_PRODUCER_ACKS=all
KAFKA_PRODUCER_LINGER_MS=5
KAFKA_CONSUMER_MAX_POLL_INTERVAL_MS=300000
KAFKA_CONSUMER_COMMIT_BATCH_SIZE=100
KAFKA_SCHEMA_REGISTRY_URL=http://redpanda:8081
REDIS_URL=redis://redis:6379/0
REDIS_MAX_CONNECTIONS=20
REDIS_INSTRUMENT_TTL_SECONDS=300
REDIS_QUALITY_SCORE_TTL_SECONDS=60
S3_ENDPOINT_URL=
AWS_DEFAULT_REGION=us-east-2
S3_BRONZE_BUCKET=mdrp-bronze
S3_REPLAY_BUCKET=mdrp-replay
S3_BRONZE_PREFIX=events
BRONZE_FLUSH_BATCH_SIZE=1000
BRONZE_FLUSH_INTERVAL_SECONDS=30
SNOWFLAKE_WAREHOUSE=MDRP_LOAD_WH
SNOWFLAKE_DATABASE=MARKET_DATA
SNOWFLAKE_SCHEMA=BRONZE
SNOWFLAKE_STAGE=MDRP_STAGE
SNOWFLAKE_ROLE=MDRP_WRITER
SNOWFLAKE_PASSWORD=
SNOWFLAKE_PRIVATE_KEY_PATH=
DATABENTO_DATASET=GLBX.MDP3
DATABENTO_SYMBOLS=ESM4,CLM4,GCM4
DATABENTO_SCHEMA=mbp-1
DATABENTO_MODE=historical-streaming
DATABENTO_LOOKBACK_DAYS=5
TEAMS_WEBHOOK_URL=
SMTP_HOST=
SMTP_PORT=587
SMTP_USERNAME=
SMTP_PASSWORD=
ALERT_FROM_EMAIL=mdrp-alerts@example.com
ALERT_TO_EMAILS=
PAGERDUTY_INTEGRATION_KEY=
OTEL_EXPORTER_OTLP_ENDPOINT=http://jaeger:4317
OTEL_EXPORTER_OTLP_PROTOCOL=grpc
OTEL_TRACES_SAMPLER_ARG=1.0
OTEL_METRIC_EXPORT_INTERVAL=15000
LOG_LEVEL=INFO
LOG_FORMAT=json
HTTP_PORT=8000
METRICS_PORT_PROVIDER_EMULATOR=8001
METRICS_PORT_VALIDATION=8002
METRICS_PORT_BRONZE_WRITER=8003
METRICS_PORT_NORMALIZATION=8004
METRICS_PORT_REDIS_WRITER=8005
METRICS_PORT_REPLAY_ENGINE=8006
METRICS_PORT_OPS_API=8007
METRICS_PORT_SILVER_LOADER=8008
METRICS_PORT_GOLD_LOADER=8009
INSTRUMENTS=["TTF","NBP","TTF_POWER","BRENT","WTI","EU_ETS"]
PROVIDER_NAME=provider-emulator
PUBLISH_INTERVAL_SECONDS=5.0
FAULT_RATE_DUPLICATE=0.02
FAULT_RATE_MALFORMED=0.01
FAULT_RATE_DELAYED=0.05
FAULT_RATE_OUT_OF_ORDER=0.03
FAULT_RATE_SCHEMA_DRIFT=0.005
FAULT_RATE_STALE=0.01
FAULT_RATE_PARTIAL_CURVE=0.02
DELAY_MIN_SECONDS=2.0
DELAY_MAX_SECONDS=30.0
DELAY_QUEUE_MAX_SIZE=500
REPLAY_MAX_CONCURRENT_JOBS=3
REPLAY_MAX_SPEED_MULTIPLIER=100
REPLAY_CHUNK_SIZE=5000
ENVEOF

# Append the secrets that were fetched dynamically
echo "SNOWFLAKE_ACCOUNT=${SF_ACCOUNT}" >> /opt/mdrp/.env
echo "SNOWFLAKE_USER=${SF_USER}" >> /opt/mdrp/.env
echo "SNOWFLAKE_PAT_TOKEN=${SF_PAT}" >> /opt/mdrp/.env
echo "DATABENTO_API_KEY=${DB_KEY}" >> /opt/mdrp/.env

echo "--- Building Docker images ---"
cd /opt/mdrp
docker compose -f docker-compose.cloud.yml build 2>&1
echo "--- Starting stack ---"
docker compose -f docker-compose.cloud.yml up -d 2>&1

echo "=== MDRP bootstrap complete ==="
docker compose -f docker-compose.cloud.yml ps
