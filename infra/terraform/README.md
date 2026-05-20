# Market Data Reliability Platform — Terraform Deployment Guide

This guide covers bootstrapping and deploying the MDRP production infrastructure
to a clean AWS account (us-east-1 default region).

---

## Prerequisites

| Tool | Minimum version | Install |
|------|-----------------|---------|
| Terraform | 1.8.0 | https://developer.hashicorp.com/terraform/downloads |
| AWS CLI | 2.x | https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html |
| Docker | 24.x | Required to build and push service images to ECR |

You must also have AWS credentials with sufficient permissions (AdministratorAccess
or a scoped policy covering IAM, ECS, ECR, S3, Secrets Manager, EventBridge,
CloudWatch, VPC, and DynamoDB).

Configure your credentials before running Terraform:

```bash
aws configure --profile mdrp-prod
# or
export AWS_PROFILE=mdrp-prod
```

---

## Directory layout

```
infra/terraform/
├── environments/
│   └── prod/
│       ├── main.tf                  # Root module — calls all child modules
│       ├── variables.tf
│       ├── outputs.tf
│       └── terraform.tfvars.example # Copy and fill in before first apply
└── modules/
    ├── networking/                  # VPC, subnets, NAT gateway
    ├── s3/                          # Bronze S3 bucket + Athena workgroup
    ├── ecs/                         # Cluster, ECR, task definitions, ALB
    ├── secrets/                     # Secrets Manager secrets
    └── eventbridge/                 # Scheduled ECS task rules
```

---

## Step 1 — Bootstrap the S3 state backend

Terraform stores its state in S3 with DynamoDB locking. These resources must
exist before `terraform init` can configure the backend.

Run once in the target account:

```bash
# Create the state bucket (versioning and encryption are recommended)
aws s3api create-bucket \
  --bucket mdrp-terraform-state-prod \
  --region us-east-1 \
  --create-bucket-configuration LocationConstraint=us-east-1

aws s3api put-bucket-versioning \
  --bucket mdrp-terraform-state-prod \
  --versioning-configuration Status=Enabled

aws s3api put-bucket-encryption \
  --bucket mdrp-terraform-state-prod \
  --server-side-encryption-configuration \
    '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'

aws s3api put-public-access-block \
  --bucket mdrp-terraform-state-prod \
  --public-access-block-configuration \
    "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"

# Create the DynamoDB locking table
aws dynamodb create-table \
  --table-name mdrp-terraform-locks \
  --attribute-definitions AttributeName=LockID,AttributeType=S \
  --key-schema AttributeName=LockID,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --region us-east-1
```

---

## Step 2 — Prepare tfvars

```bash
cd infra/terraform/environments/prod
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars — fill in real API keys, Snowflake credentials, etc.
# Never commit terraform.tfvars to version control.
```

---

## Step 3 — Initialise Terraform

```bash
cd infra/terraform/environments/prod
terraform init
```

Terraform downloads the AWS provider (~500 MB first run) and configures the
S3 backend. You should see:

```
Terraform has been successfully initialized!
```

---

## Step 4 — Plan

Review the full set of resources that will be created:

```bash
terraform plan -out=tfplan
```

Inspect the plan carefully before applying — pay attention to IAM roles,
bucket policies, and security group rules.

---

## Step 5 — Apply

```bash
terraform apply tfplan
```

A full first-time apply takes approximately 10–15 minutes (NAT gateway and
ALB provisioning dominate the time). Terraform prints outputs on completion:

| Output | Description |
|--------|-------------|
| `s3_bronze_bucket_name` | Bronze S3 bucket name |
| `ecr_repository_urls` | Map of service → ECR URL (use when tagging/pushing images) |
| `ecs_cluster_arn` | Cluster ARN (use in EventBridge rules and CI/CD) |
| `vpc_id` | VPC ID |
| `private_subnet_ids` | Private subnet IDs |
| `cloudwatch_log_group_name` | Log group for all services (`/mdrp/prod`) |
| `ops_api_alb_dns_name` | Public DNS name for the ops-api load balancer |

---

## Pushing images to ECR

After `terraform apply`, build and push each service image. Example for
`silver-loader`:

```bash
# Get ECR login token
aws ecr get-login-password --region us-east-1 \
  | docker login --username AWS --password-stdin \
    $(terraform output -raw ecr_repository_urls | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['silver-loader'].rsplit('/',1)[0])")

# Build and push
docker build -t silver-loader services/silver-loader/
docker tag silver-loader:latest \
  $(terraform output -json ecr_repository_urls | python3 -c "import sys,json; print(json.load(sys.stdin)['silver-loader'])"):latest
docker push \
  $(terraform output -json ecr_repository_urls | python3 -c "import sys,json; print(json.load(sys.stdin)['silver-loader'])"):latest
```

---

## Applying Snowflake migrations

Run the numbered SQL scripts in order against your Snowflake account using
SnowSQL or the Snowflake web console:

```bash
snowsql -a <account> -u <admin_user> -f infra/snowflake/001_database_and_warehouses.sql
snowsql -a <account> -u <admin_user> -f infra/snowflake/002_schemas.sql
snowsql -a <account> -u <admin_user> -f infra/snowflake/003_tables.sql
snowsql -a <account> -u <admin_user> -f infra/snowflake/004_roles_and_grants.sql
snowsql -a <account> -u <admin_user> -f infra/snowflake/005_stages_and_pipes.sql
snowsql -a <account> -u <admin_user> -f infra/snowflake/006_views.sql
```

After running `004_roles_and_grants.sql`, update the `MDRP_SVC_USER` password
placeholder with the value stored in AWS Secrets Manager at
`mdrp/prod/snowflake-password`.

---

## Day-2 operations

### Updating a task definition

Task definition changes (new image tag, environment variable) are performed
by CI/CD — Terraform ignores `task_definition` and `desired_count` changes
on ECS services (see `lifecycle.ignore_changes`). Update via:

```bash
aws ecs register-task-definition --cli-input-json file://task-def-patch.json
aws ecs update-service \
  --cluster mdrp-prod-cluster \
  --service mdrp-prod-silver-loader \
  --task-definition mdrp-prod-silver-loader:<new_revision>
```

### Rotating secrets

```bash
aws secretsmanager put-secret-value \
  --secret-id mdrp/prod/databento-api-key \
  --secret-string "new-key-value"
```

ECS services pick up the new value on the next task restart (force a new
deployment if immediate rotation is required):

```bash
aws ecs update-service \
  --cluster mdrp-prod-cluster \
  --service mdrp-prod-silver-loader \
  --force-new-deployment
```

### Scaling a service

```bash
aws ecs update-service \
  --cluster mdrp-prod-cluster \
  --service mdrp-prod-silver-loader \
  --desired-count 3
```

---

## Destroying the environment

> **Warning**: This is irreversible and will delete the S3 Bronze bucket
> contents if `force_destroy = true` is set. Verify before proceeding.

```bash
cd infra/terraform/environments/prod
terraform destroy
```

The S3 state bucket and DynamoDB table are created outside Terraform and must
be deleted manually if no longer needed:

```bash
aws s3 rb s3://mdrp-terraform-state-prod --force
aws dynamodb delete-table --table-name mdrp-terraform-locks --region us-east-1
```

---

## Common issues

| Problem | Likely cause | Fix |
|---------|-------------|-----|
| `NoSuchBucket` on `terraform init` | State bucket not created | Run Step 1 bootstrap commands |
| `Error: creating ECS Service: AccessDeniedException` | Missing IAM permissions | Verify AWS credentials have ECS + IAM permissions |
| `InvalidParameterException: … secret does not exist` | Secrets not yet created | Run `terraform apply` in full (secrets module runs before ECS) |
| Snowpipe `COPY INTO` inserts 0 rows | File format mismatch | Check `STRIP_OUTER_ARRAY` and that files are newline-delimited JSON |
| NAT gateway charges unexpected | Single NAT in use | Expected — `single_nat_gateway = true` for cost savings; set `false` for HA |
