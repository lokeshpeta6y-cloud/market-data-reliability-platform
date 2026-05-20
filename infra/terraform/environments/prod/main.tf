###############################################################################
# Market Data Reliability Platform — Production Root Module
###############################################################################

terraform {
  required_version = ">= 1.8.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.50"
    }
  }

  backend "s3" {
    # Bootstrap: create this bucket manually before running `terraform init`
    # See infra/terraform/README.md for instructions.
    bucket         = "mdrp-terraform-state-prod"
    key            = "prod/terraform.tfstate"
    region         = "eu-west-1"
    encrypt        = true
    dynamodb_table = "mdrp-terraform-locks"
  }
}

###############################################################################
# Provider
###############################################################################

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = local.common_tags
  }
}

###############################################################################
# Locals
###############################################################################

locals {
  common_tags = {
    Project     = "market-data-reliability-platform"
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

###############################################################################
# Networking
###############################################################################

module "networking" {
  source = "../../modules/networking"

  project_name = var.project_name
  environment  = var.environment
  vpc_cidr     = var.vpc_cidr
}

###############################################################################
# S3 / Bronze storage
###############################################################################

module "s3" {
  source = "../../modules/s3"

  project_name         = var.project_name
  environment          = var.environment
  ecs_task_role_arn    = module.ecs.ecs_task_role_arn
}

###############################################################################
# Secrets Manager
###############################################################################

module "secrets" {
  source = "../../modules/secrets"

  project_name       = var.project_name
  environment        = var.environment
  databento_api_key  = var.databento_api_key
  snowflake_account  = var.snowflake_account
  snowflake_user     = var.snowflake_user
  snowflake_password = var.snowflake_password
  smtp_host          = var.smtp_host
  smtp_username      = var.smtp_username
  smtp_password      = var.smtp_password
  teams_webhook_url  = var.teams_webhook_url
}

###############################################################################
# ECS — Cluster, Task Definitions, Services, ECR, ALB
###############################################################################

module "ecs" {
  source = "../../modules/ecs"

  project_name         = var.project_name
  environment          = var.environment
  aws_region           = var.aws_region
  ecr_repository_names = var.ecr_repository_names
  private_subnet_ids   = module.networking.private_subnet_ids
  public_subnet_ids    = module.networking.public_subnet_ids
  vpc_id               = module.networking.vpc_id
  s3_bronze_bucket_arn = module.s3.bronze_bucket_arn
  secret_arns          = module.secrets.secret_arns
}

###############################################################################
# EventBridge — Scheduled Rules
###############################################################################

module "eventbridge" {
  source = "../../modules/eventbridge"

  project_name        = var.project_name
  environment         = var.environment
  ecs_cluster_arn     = module.ecs.ecs_cluster_arn
  replay_task_def_arn = module.ecs.task_definition_arns["replay-engine"]
  ops_api_task_def_arn = module.ecs.task_definition_arns["ops-api"]
  private_subnet_ids  = module.networking.private_subnet_ids
  ecs_security_group_id = module.ecs.ecs_security_group_id
  ecs_task_role_arn   = module.ecs.ecs_task_role_arn
  ecs_execution_role_arn = module.ecs.ecs_execution_role_arn
}
