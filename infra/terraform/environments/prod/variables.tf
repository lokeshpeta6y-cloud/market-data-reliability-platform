variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "eu-west-1"
}

variable "project_name" {
  description = "Short project identifier, used in resource names"
  type        = string
  default     = "mdrp"
}

variable "environment" {
  description = "Deployment environment (prod, staging, dev)"
  type        = string
  default     = "prod"
}

variable "vpc_cidr" {
  description = "CIDR block for the VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "ecr_repository_names" {
  description = "List of ECR repository names — one per service"
  type        = list(string)
  default = [
    "provider-emulator",
    "validation-service",
    "bronze-writer",
    "normalization-service",
    "redis-writer",
    "silver-loader",
    "gold-loader",
    "replay-engine",
    "ops-api",
  ]
}

variable "databento_api_key" {
  description = "Databento API key — stored in Secrets Manager"
  type        = string
  sensitive   = true
}

variable "snowflake_account" {
  description = "Snowflake account identifier (e.g. xy12345.eu-west-1)"
  type        = string
}

variable "snowflake_user" {
  description = "Snowflake service-user name"
  type        = string
}

variable "snowflake_password" {
  description = "Snowflake service-user password — stored in Secrets Manager"
  type        = string
  sensitive   = true
}

variable "smtp_host" {
  description = "SMTP relay host for alerting e-mails"
  type        = string
  default     = ""
}

variable "smtp_username" {
  description = "SMTP username"
  type        = string
  default     = ""
}

variable "smtp_password" {
  description = "SMTP password — stored in Secrets Manager"
  type        = string
  sensitive   = true
  default     = ""
}

variable "teams_webhook_url" {
  description = "Microsoft Teams incoming-webhook URL for ops alerts"
  type        = string
  sensitive   = true
  default     = ""
}
