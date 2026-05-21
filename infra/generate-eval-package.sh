#!/usr/bin/env bash
# Generates eval-access.md — a filled-in copy of EVAL_GUIDE.md with all live
# credentials substituted in. The output file is gitignored; share via email.
#
# Usage:
#   bash infra/generate-eval-package.sh
#
# Prerequisites:
#   - AWS CLI configured with admin credentials (same account as the stack)
#   - Run from the repo root

set -euo pipefail

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
log()  { echo -e "${GREEN}[eval-pkg]${NC} $*"; }
warn() { echo -e "${YELLOW}[warn]${NC} $*"; }
die()  { echo -e "${RED}[error]${NC} $*"; exit 1; }

REGION="us-east-2"
SECRET_PREFIX="mdrp/prod"
TEMPLATE="EVAL_GUIDE.md"
OUTPUT="eval-access.md"
EVAL_USER="mdrp-eval-prod"

[[ -f "$TEMPLATE" ]] || die "Run this script from the repo root (EVAL_GUIDE.md not found)."

# -------------------------------------------------------
# 1. EC2 public IP
# -------------------------------------------------------
log "Fetching EC2 public IP..."
HOST=$("${AWS_CLI:-aws}" ec2 describe-instances \
  --filters "Name=tag:Name,Values=mdrp-eval" "Name=instance-state-name,Values=running" \
  --region "$REGION" \
  --query "Reservations[0].Instances[0].PublicIpAddress" \
  --output text 2>/dev/null || true)

if [[ -z "$HOST" || "$HOST" == "None" ]]; then
  warn "No running mdrp-eval instance found. Enter the EC2 public IP manually:"
  read -r HOST
fi
log "EC2 IP: $HOST"

# -------------------------------------------------------
# 2. Secrets from Secrets Manager
# -------------------------------------------------------
secret() {
  "${AWS_CLI:-aws}" secretsmanager get-secret-value \
    --secret-id "${SECRET_PREFIX}/$1" \
    --region "$REGION" \
    --query SecretString --output text 2>/dev/null || true
}

log "Fetching secrets from Secrets Manager..."
SF_ACCOUNT=$(secret "snowflake-account")
SF_USER=$(secret "snowflake-user")
SF_PAT=$(secret "snowflake-pat-token")

[[ -n "$SF_ACCOUNT" ]] || die "Could not fetch snowflake-account from Secrets Manager."
[[ -n "$SF_USER" ]]    || die "Could not fetch snowflake-user from Secrets Manager."
[[ -n "$SF_PAT" ]]     || die "Could not fetch snowflake-pat-token from Secrets Manager."

# -------------------------------------------------------
# 3. Grafana password — from .env if set, otherwise default
# -------------------------------------------------------
GRAFANA_PASSWORD="mdrp_grafana"
if [[ -f ".env" ]]; then
  ENV_GF=$(grep -E "^GRAFANA_ADMIN_PASSWORD=" .env 2>/dev/null | cut -d'=' -f2 || true)
  [[ -n "$ENV_GF" ]] && GRAFANA_PASSWORD="$ENV_GF"
fi

# -------------------------------------------------------
# 4. AWS eval credentials — create a fresh key each run
#    Delete the key after the eval period to keep the account clean.
# -------------------------------------------------------
log "Creating AWS eval access key for $EVAL_USER..."
KEY_JSON=$("${AWS_CLI:-aws}" iam create-access-key \
  --user-name "$EVAL_USER" 2>/dev/null || true)

if [[ -n "$KEY_JSON" ]]; then
  EVAL_KEY_ID=$(echo "$KEY_JSON" | python3 -c \
    "import sys,json; d=json.load(sys.stdin); print(d['AccessKey']['AccessKeyId'])")
  EVAL_SECRET=$(echo "$KEY_JSON" | python3 -c \
    "import sys,json; d=json.load(sys.stdin); print(d['AccessKey']['SecretAccessKey'])")
  log "Created access key: $EVAL_KEY_ID"
  warn "This secret is shown once — it is embedded in $OUTPUT."
  warn "Delete after eval: aws iam delete-access-key --user-name $EVAL_USER --access-key-id $EVAL_KEY_ID"
else
  warn "Could not create eval access key. Fill in AWS credentials manually in $OUTPUT."
  EVAL_KEY_ID="<fill in>"
  EVAL_SECRET="<fill in>"
fi

# -------------------------------------------------------
# 5. Substitute into template using Python
# -------------------------------------------------------
log "Generating $OUTPUT..."

python3 << PYEOF
template = open("$TEMPLATE").read()

subs = [
    # header note
    (
        "> **Before you start:** Replace \`<HOST>\` throughout this document with the EC2 public IP provided to you.\n"
        "> Snowflake account, user, and all credentials are provided alongside this file.",
        "> **All credentials are filled in below — no substitution needed.**"
    ),
    # credentials table
    ("<HOST>",                                                           "$HOST"),
    ("admin / _password provided separately_",                          "admin / \`$GRAFANA_PASSWORD\`"),
    ("| Snowflake account | _provided separately_ |",                   "| Snowflake account | \`$SF_ACCOUNT\` |"),
    ("| Snowflake user | _provided separately_ |",                      "| Snowflake user | \`$SF_USER\` |"),
    ("| Snowflake PAT token | _provided separately_ |",                 "| Snowflake PAT token | \`$SF_PAT\` |"),
    ("| AWS eval credentials | _provided separately_ (read-only: S3 + Snowflake secret) |",
        "| AWS eval key ID | \`$EVAL_KEY_ID\` |\n"
        "| AWS eval secret | \`$EVAL_SECRET\` |\n"
        "| AWS region | \`$REGION\` |"),
    # Snowflake connection block
    ("- **Account:** _provided separately_",  "- **Account:** \`$SF_ACCOUNT\`"),
    ("- **User:** _provided separately_",     "- **User:** \`$SF_USER\`"),
    ("- **PAT token:** _provided separately_","- **PAT token:** \`$SF_PAT\`"),
]

for old, new in subs:
    template = template.replace(old, new)

with open("$OUTPUT", "w") as f:
    f.write(template)
PYEOF

# -------------------------------------------------------
# 6. Summary
# -------------------------------------------------------
echo ""
echo "================================================================"
echo "  $OUTPUT generated — DO NOT commit this file"
echo "================================================================"
echo ""
echo "  EC2 host      : $HOST"
echo "  Snowflake     : $SF_ACCOUNT / $SF_USER"
echo "  Grafana       : admin / $GRAFANA_PASSWORD"
echo "  AWS eval key  : $EVAL_KEY_ID"
echo ""
echo "  Send $OUTPUT to the evaluator via email."
echo ""
if [[ "$EVAL_KEY_ID" != "<fill in>" ]]; then
  echo "  After the eval, revoke the key:"
  echo "    aws iam delete-access-key \\"
  echo "      --user-name $EVAL_USER \\"
  echo "      --access-key-id $EVAL_KEY_ID"
fi
echo "================================================================"
