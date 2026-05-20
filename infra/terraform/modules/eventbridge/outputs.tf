output "daily_replay_check_schedule_arn" {
  description = "ARN of the daily bronze replay check EventBridge schedule"
  value       = aws_scheduler_schedule.daily_replay_check.arn
}

output "dlq_replay_schedule_arn" {
  description = "ARN of the DLQ replay EventBridge schedule"
  value       = aws_scheduler_schedule.dlq_replay.arn
}

output "eventbridge_ecs_role_arn" {
  description = "ARN of the IAM role used by EventBridge to run ECS tasks"
  value       = aws_iam_role.eventbridge_ecs.arn
}
