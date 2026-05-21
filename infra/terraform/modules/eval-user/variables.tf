variable "project_name" {
  description = "Short project identifier"
  type        = string
}

variable "environment" {
  description = "Deployment environment"
  type        = string
}

variable "aws_region" {
  description = "AWS region where resources live"
  type        = string
}

variable "aws_account_id" {
  description = "AWS account ID (12-digit)"
  type        = string
}

variable "bronze_bucket_name" {
  description = "Name of the Bronze S3 bucket the evaluator may read"
  type        = string
  default     = "mdrp-bronze"
}

variable "expiry_date" {
  description = "ISO-8601 date after which the eval credentials should be rotated / deleted"
  type        = string
}
