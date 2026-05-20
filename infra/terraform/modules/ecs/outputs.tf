output "ecs_cluster_arn" {
  description = "ARN of the ECS cluster"
  value       = aws_ecs_cluster.main.arn
}

output "ecs_cluster_name" {
  description = "Name of the ECS cluster"
  value       = aws_ecs_cluster.main.name
}

output "ecr_repository_urls" {
  description = "Map of service name → ECR repository URL"
  value       = { for k, v in aws_ecr_repository.services : k => v.repository_url }
}

output "ecr_repository_arns" {
  description = "Map of service name → ECR repository ARN"
  value       = { for k, v in aws_ecr_repository.services : k => v.arn }
}

output "ecs_task_role_arn" {
  description = "ARN of the ECS task IAM role"
  value       = aws_iam_role.ecs_task.arn
}

output "ecs_task_role_name" {
  description = "Name of the ECS task IAM role"
  value       = aws_iam_role.ecs_task.name
}

output "ecs_execution_role_arn" {
  description = "ARN of the ECS task execution IAM role"
  value       = aws_iam_role.ecs_execution.arn
}

output "ecs_execution_role_name" {
  description = "Name of the ECS task execution IAM role"
  value       = aws_iam_role.ecs_execution.name
}

output "cloudwatch_log_group_name" {
  description = "Name of the shared CloudWatch log group"
  value       = aws_cloudwatch_log_group.mdrp.name
}

output "cloudwatch_log_group_arn" {
  description = "ARN of the shared CloudWatch log group"
  value       = aws_cloudwatch_log_group.mdrp.arn
}

output "task_definition_arns" {
  description = "Map of service name → latest task definition ARN"
  value       = { for k, v in aws_ecs_task_definition.services : k => v.arn }
}

output "ecs_security_group_id" {
  description = "ID of the ECS tasks security group"
  value       = aws_security_group.ecs_tasks.id
}

output "ops_api_alb_dns_name" {
  description = "DNS name of the ops-api ALB"
  value       = aws_lb.ops_api.dns_name
}

output "ops_api_alb_arn" {
  description = "ARN of the ops-api ALB"
  value       = aws_lb.ops_api.arn
}

output "ops_api_target_group_arn" {
  description = "ARN of the ops-api ALB target group"
  value       = aws_lb_target_group.ops_api.arn
}
