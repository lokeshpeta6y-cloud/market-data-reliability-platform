variable "project_name" {
  description = "Short project identifier"
  type        = string
}

variable "environment" {
  description = "Deployment environment"
  type        = string
}

variable "aws_region" {
  description = "AWS region"
  type        = string
}

variable "ecr_repository_names" {
  description = "List of service names for which ECR repositories are created"
  type        = list(string)
}

variable "private_subnet_ids" {
  description = "Private subnet IDs where ECS tasks run"
  type        = list(string)
}

variable "public_subnet_ids" {
  description = "Public subnet IDs for the ops-api ALB"
  type        = list(string)
}

variable "vpc_id" {
  description = "VPC ID"
  type        = string
}

variable "s3_bronze_bucket_arn" {
  description = "ARN of the S3 bronze bucket — granted to the ECS task role"
  type        = string
}

variable "secret_arns" {
  description = "Map of secret name → ARN for all Secrets Manager secrets"
  type        = map(string)
}

variable "task_cpu" {
  description = "Fargate task CPU units (256, 512, 1024, 2048, 4096)"
  type        = number
  default     = 512
}

variable "task_memory" {
  description = "Fargate task memory in MiB"
  type        = number
  default     = 1024
}

variable "service_desired_count" {
  description = "Desired running task count for each service"
  type        = number
  default     = 1
}

variable "log_retention_days" {
  description = "CloudWatch log retention in days"
  type        = number
  default     = 30
}

variable "ops_api_container_port" {
  description = "Container port exposed by ops-api"
  type        = number
  default     = 8000
}

variable "container_image_tag" {
  description = "Default Docker image tag to use in task definitions"
  type        = string
  default     = "latest"
}
