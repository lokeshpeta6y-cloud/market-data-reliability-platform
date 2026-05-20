variable "project_name" {
  description = "Short project identifier"
  type        = string
}

variable "environment" {
  description = "Deployment environment"
  type        = string
}

variable "ecs_cluster_arn" {
  description = "ARN of the ECS cluster where scheduled tasks run"
  type        = string
}

variable "replay_task_def_arn" {
  description = "ARN of the replay-engine ECS task definition"
  type        = string
}

variable "ops_api_task_def_arn" {
  description = "ARN of the ops-api ECS task definition"
  type        = string
}

variable "private_subnet_ids" {
  description = "Private subnet IDs for scheduled ECS task network config"
  type        = list(string)
}

variable "ecs_security_group_id" {
  description = "Security group ID for ECS tasks"
  type        = string
}

variable "ecs_task_role_arn" {
  description = "ARN of the ECS task IAM role"
  type        = string
}

variable "ecs_execution_role_arn" {
  description = "ARN of the ECS task execution IAM role"
  type        = string
}

variable "daily_replay_schedule" {
  description = "Cron expression for the daily bronze replay check (UTC)"
  type        = string
  default     = "cron(0 6 * * ? *)"
}

variable "dlq_replay_schedule" {
  description = "Cron expression for the DLQ replay job (UTC)"
  type        = string
  default     = "cron(0 2 * * ? *)"
}
