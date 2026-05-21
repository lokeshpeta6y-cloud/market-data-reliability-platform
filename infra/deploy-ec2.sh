#!/usr/bin/env bash
# ============================================================
# MDRP — EC2 Cloud Deploy Script
# ============================================================
#
# Provisions an EC2 instance on Amazon Linux 2023, installs Docker,
# clones the repo, pulls secrets from Secrets Manager, and
# starts the full cloud stack.
#
# Prerequisites:
#   1. AWS CLI configured (aws configure) with permission to:
#        ec2:RunInstances, ec2:DescribeInstances,
#        ec2:CreateSecurityGroup, ec2:AuthorizeSecurityGroupIngress,
#        iam:PassRole, secretsmanager:GetSecretValue
#   2. Key pair 'mdrp' already exists in us-east-2
#   3. Secrets Manager secrets populated (run terraform first,
#      or see "Manual Secrets Setup" in README)
#   4. S3 buckets mdrp-bronze and mdrp-replay exist
#   5. Set REPO_URL below to your git repository
#
# Usage:
#   bash infra/deploy-ec2.sh
#
# Tear down:
#   aws ec2 terminate-instances --instance-ids <id> --region us-east-2
# ============================================================

set -euo pipefail

# -------------------------------------------------------
# Configuration — edit these before running
# -------------------------------------------------------
AWS_REGION="us-east-2"
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
KEY_PAIR_NAME="mdrp"
INSTANCE_TYPE="m7i-flex.large"
SECURITY_GROUP_NAME="mdrp-ec2-sg"
IAM_INSTANCE_PROFILE="mdrp-ec2-instance-profile"
S3_BRONZE_BUCKET="mdrp-bronze"
S3_REPLAY_BUCKET="mdrp-replay"
SECRET_PREFIX="mdrp/prod"

# Set this to your git repository URL (SSH or HTTPS).
# For a private repo set up a deploy key or use HTTPS with a token.
REPO_URL="https://github.com/lokeshpeta6y-cloud/market-data-reliability-platform.git"

# -------------------------------------------------------
# Validate repo URL is set
# -------------------------------------------------------
if [[ -z "$REPO_URL" ]]; then
  echo "ERROR: REPO_URL is not set."
  exit 1
fi

# -------------------------------------------------------
# Colours
# -------------------------------------------------------
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log()  { echo -e "${GREEN}[deploy]${NC} $*"; }
warn() { echo -e "${YELLOW}[warn]${NC} $*"; }

# -------------------------------------------------------
# 1. Create EC2 Instance Profile (idempotent)
# -------------------------------------------------------
log "Ensuring EC2 IAM role and instance profile exist..."

ROLE_NAME="mdrp-ec2-role"

# Create role if it doesn't exist
aws iam get-role --role-name "$ROLE_NAME" --region "$AWS_REGION" >/dev/null 2>&1 || \
aws iam create-role \
  --role-name "$ROLE_NAME" \
  --assume-role-policy-document '{
    "Version":"2012-10-17",
    "Statement":[{
      "Effect":"Allow",
      "Principal":{"Service":"ec2.amazonaws.com"},
      "Action":"sts:AssumeRole"
    }]
  }' >/dev/null

# Attach inline policy: S3 read/write + Secrets Manager read
aws iam put-role-policy \
  --role-name "$ROLE_NAME" \
  --policy-name "mdrp-ec2-policy" \
  --policy-document "{
    \"Version\": \"2012-10-17\",
    \"Statement\": [
      {
        \"Sid\": \"BronzeStorage\",
        \"Effect\": \"Allow\",
        \"Action\": [\"s3:GetObject\",\"s3:PutObject\",\"s3:DeleteObject\",\"s3:ListBucket\",\"s3:GetBucketLocation\"],
        \"Resource\": [
          \"arn:aws:s3:::${S3_BRONZE_BUCKET}\",
          \"arn:aws:s3:::${S3_BRONZE_BUCKET}/*\",
          \"arn:aws:s3:::${S3_REPLAY_BUCKET}\",
          \"arn:aws:s3:::${S3_REPLAY_BUCKET}/*\"
        ]
      },
      {
        \"Sid\": \"SecretsRead\",
        \"Effect\": \"Allow\",
        \"Action\": \"secretsmanager:GetSecretValue\",
        \"Resource\": \"arn:aws:secretsmanager:${AWS_REGION}:${AWS_ACCOUNT_ID}:secret:${SECRET_PREFIX}/*\"
      }
    ]
  }" >/dev/null

# Create instance profile if it doesn't exist
aws iam get-instance-profile --instance-profile-name "$IAM_INSTANCE_PROFILE" >/dev/null 2>&1 || {
  aws iam create-instance-profile --instance-profile-name "$IAM_INSTANCE_PROFILE" >/dev/null
  aws iam add-role-to-instance-profile \
    --instance-profile-name "$IAM_INSTANCE_PROFILE" \
    --role-name "$ROLE_NAME" >/dev/null
  # IAM propagation delay
  log "Waiting 15s for IAM propagation..."
  sleep 15
}

log "IAM instance profile ready."

# -------------------------------------------------------
# 2. Security Group (idempotent)
# -------------------------------------------------------
log "Ensuring security group '$SECURITY_GROUP_NAME' exists..."

SG_ID=$(aws ec2 describe-security-groups \
  --filters "Name=group-name,Values=$SECURITY_GROUP_NAME" \
  --region "$AWS_REGION" \
  --query "SecurityGroups[0].GroupId" \
  --output text 2>/dev/null || echo "None")

if [[ "$SG_ID" == "None" || -z "$SG_ID" ]]; then
  SG_ID=$(aws ec2 create-security-group \
    --group-name "$SECURITY_GROUP_NAME" \
    --description "MDRP evaluation stack" \
    --region "$AWS_REGION" \
    --query "GroupId" --output text)

  # SSH — restrict to your current IP
  MY_IP=$(curl -s https://checkip.amazonaws.com)/32
  aws ec2 authorize-security-group-ingress --group-id "$SG_ID" --region "$AWS_REGION" \
    --ip-permissions \
      "IpProtocol=tcp,FromPort=22,ToPort=22,IpRanges=[{CidrIp=$MY_IP,Description=SSH}]" \
      "IpProtocol=tcp,FromPort=8000,ToPort=8000,IpRanges=[{CidrIp=0.0.0.0/0,Description=ops-api}]" \
      "IpProtocol=tcp,FromPort=3000,ToPort=3000,IpRanges=[{CidrIp=0.0.0.0/0,Description=Grafana}]" \
      "IpProtocol=tcp,FromPort=9090,ToPort=9090,IpRanges=[{CidrIp=0.0.0.0/0,Description=Prometheus}]" \
      "IpProtocol=tcp,FromPort=16686,ToPort=16686,IpRanges=[{CidrIp=0.0.0.0/0,Description=Jaeger}]" >/dev/null
fi

log "Security group: $SG_ID"

# -------------------------------------------------------
# 3. Resolve latest Amazon Linux 2023 AMI
# -------------------------------------------------------
AMI_ID=$(aws ec2 describe-images \
  --owners amazon \
  --filters \
    "Name=name,Values=al2023-ami-2023.*-x86_64" \
    "Name=state,Values=available" \
  --query "sort_by(Images, &CreationDate)[-1].ImageId" \
  --region "$AWS_REGION" \
  --output text)

log "AMI: $AMI_ID"

# -------------------------------------------------------
# 4. User-data: bootstraps the instance on first boot
# -------------------------------------------------------
USER_DATA=$(cat <<USERDATA
#!/bin/bash
set -euo pipefail
exec > /var/log/mdrp-init.log 2>&1

echo "=== MDRP bootstrap start ==="
export AWS_DEFAULT_REGION="${AWS_REGION}"

# Install Docker and git
dnf install -y docker git jq
systemctl enable --now docker
usermod -aG docker ec2-user

# Docker Compose v2
mkdir -p /usr/local/lib/docker/cli-plugins
curl -fsSL "https://github.com/docker/compose/releases/download/v2.27.0/docker-compose-linux-x86_64" \
  -o /usr/local/lib/docker/cli-plugins/docker-compose
chmod +x /usr/local/lib/docker/cli-plugins/docker-compose

# Clone repo
cd /opt
git clone "${REPO_URL}" mdrp
cd mdrp

# Pull secrets from Secrets Manager and write .env
SECRET() {
  aws secretsmanager get-secret-value \
    --secret-id "${SECRET_PREFIX}/\$1" \
    --region "${AWS_REGION}" \
    --query SecretString --output text 2>/dev/null || echo ""
}

DATABENTO_KEY=\$(SECRET "databento-api-key")
SF_ACCOUNT=\$(SECRET "snowflake-account")
SF_USER=\$(SECRET "snowflake-user")
SF_PAT=\$(SECRET "snowflake-pat-token")
TEAMS_URL=\$(SECRET "teams-webhook-url")

cat > /opt/mdrp/.env <<ENV
# Generated by deploy-ec2.sh — do not edit manually
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

# S3_ENDPOINT_URL intentionally blank — uses real AWS S3
S3_ENDPOINT_URL=
AWS_DEFAULT_REGION=${AWS_REGION}
S3_BRONZE_BUCKET=${S3_BRONZE_BUCKET}
S3_REPLAY_BUCKET=${S3_REPLAY_BUCKET}
S3_BRONZE_PREFIX=events
BRONZE_FLUSH_BATCH_SIZE=1000
BRONZE_FLUSH_INTERVAL_SECONDS=30

SNOWFLAKE_ACCOUNT=\${SF_ACCOUNT}
SNOWFLAKE_USER=\${SF_USER}
SNOWFLAKE_PAT_TOKEN=\${SF_PAT}
SNOWFLAKE_PASSWORD=
SNOWFLAKE_PRIVATE_KEY_PATH=
SNOWFLAKE_ROLE=MDRP_WRITER
SNOWFLAKE_WAREHOUSE=MDRP_LOAD_WH
SNOWFLAKE_DATABASE=MARKET_DATA
SNOWFLAKE_SCHEMA=BRONZE
SNOWFLAKE_STAGE=MDRP_STAGE

DATABENTO_API_KEY=\${DATABENTO_KEY}
DATABENTO_DATASET=GLBX.MDP3
DATABENTO_SYMBOLS=ESM4,CLM4,GCM4
DATABENTO_SCHEMA=mbp-1
DATABENTO_MODE=historical-streaming
DATABENTO_LOOKBACK_DAYS=5

TEAMS_WEBHOOK_URL=\${TEAMS_URL}
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
ENV

echo ".env written."

# Build and start the cloud stack
cd /opt/mdrp
docker compose -f docker-compose.cloud.yml build --parallel 2>&1 | tail -20
docker compose -f docker-compose.cloud.yml up -d

echo "=== MDRP bootstrap complete ==="
USERDATA
)

# -------------------------------------------------------
# 5. Launch EC2 Instance
# -------------------------------------------------------
log "Launching EC2 instance ($INSTANCE_TYPE)..."

INSTANCE_JSON=$(aws ec2 run-instances \
  --image-id "$AMI_ID" \
  --instance-type "$INSTANCE_TYPE" \
  --key-name "$KEY_PAIR_NAME" \
  --security-group-ids "$SG_ID" \
  --iam-instance-profile "Name=$IAM_INSTANCE_PROFILE" \
  --block-device-mappings '[{"DeviceName":"/dev/xvda","Ebs":{"VolumeSize":40,"VolumeType":"gp3","DeleteOnTermination":true}}]' \
  --user-data "$USER_DATA" \
  --tag-specifications \
    "ResourceType=instance,Tags=[{Key=Name,Value=mdrp-eval},{Key=Project,Value=mdrp}]" \
  --region "$AWS_REGION" \
  --query "Instances[0]")

INSTANCE_ID=$(echo "$INSTANCE_JSON" | grep -o '"InstanceId": "[^"]*"' | head -1 | cut -d'"' -f4)
log "Instance ID: $INSTANCE_ID"

# -------------------------------------------------------
# 6. Wait for running state and public IP
# -------------------------------------------------------
log "Waiting for instance to reach 'running' state..."
aws ec2 wait instance-running --instance-ids "$INSTANCE_ID" --region "$AWS_REGION"

PUBLIC_IP=$(aws ec2 describe-instances \
  --instance-ids "$INSTANCE_ID" \
  --region "$AWS_REGION" \
  --query "Reservations[0].Instances[0].PublicIpAddress" \
  --output text)

PUBLIC_DNS=$(aws ec2 describe-instances \
  --instance-ids "$INSTANCE_ID" \
  --region "$AWS_REGION" \
  --query "Reservations[0].Instances[0].PublicDnsName" \
  --output text)

# -------------------------------------------------------
# 7. Print access details
# -------------------------------------------------------
echo ""
echo "================================================================"
echo "  MDRP Cloud Stack — Launched"
echo "================================================================"
echo ""
echo "  Instance ID : $INSTANCE_ID"
echo "  Public IP   : $PUBLIC_IP"
echo ""
echo "  Bootstrap is running in the background (~5 min to build images)"
echo "  Monitor:  ssh -i ~/.ssh/mdrp.pem ec2-user@$PUBLIC_IP"
echo "            sudo tail -f /var/log/mdrp-init.log"
echo ""
echo "  URLs (available once bootstrap completes):"
echo "    Ops API    http://$PUBLIC_IP:8000/api/v1/curves"
echo "    Grafana    http://$PUBLIC_IP:3000  (admin / mdrp_grafana)"
echo "    Prometheus http://$PUBLIC_IP:9090"
echo "    Jaeger     http://$PUBLIC_IP:16686"
echo ""
echo "  To share with an evaluator, provide:"
echo "    - The URLs above"
echo "    - Evaluation AWS credentials (from: terraform output eval_user_access_key)"
echo "    - Snowflake PAT token (from: aws secretsmanager get-secret-value \\"
echo "        --secret-id mdrp/prod/snowflake-pat-token --region us-east-2)"
echo ""
echo "  Tear down when done:"
echo "    aws ec2 terminate-instances --instance-ids $INSTANCE_ID --region $AWS_REGION"
echo "================================================================"
