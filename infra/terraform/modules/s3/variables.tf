variable "project_name" {
  description = "Short project identifier used in bucket names"
  type        = string
}

variable "environment" {
  description = "Deployment environment"
  type        = string
}

variable "ecs_task_role_arn" {
  description = "ARN of the ECS task IAM role — granted read/write on the bronze bucket"
  type        = string
}

variable "athena_results_prefix" {
  description = "S3 prefix for Athena query results within the bronze bucket"
  type        = string
  default     = "athena-results/"
}

variable "transition_to_ia_days" {
  description = "Days after object creation before transitioning to STANDARD_IA"
  type        = number
  default     = 30
}

variable "transition_to_glacier_days" {
  description = "Days after object creation before transitioning to GLACIER"
  type        = number
  default     = 90
}

variable "expiration_days" {
  description = "Days after object creation before expiring (deleting) objects"
  type        = number
  default     = 365
}
