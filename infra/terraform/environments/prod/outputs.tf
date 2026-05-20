output "s3_bronze_bucket_name" {
  description = "Name of the S3 bronze-layer bucket"
  value       = module.s3.bronze_bucket_name
}

output "ecr_repository_urls" {
  description = "Map of service name → ECR repository URL"
  value       = module.ecs.ecr_repository_urls
}

output "ecs_cluster_arn" {
  description = "ARN of the ECS cluster"
  value       = module.ecs.ecs_cluster_arn
}

output "vpc_id" {
  description = "ID of the application VPC"
  value       = module.networking.vpc_id
}

output "private_subnet_ids" {
  description = "List of private subnet IDs (ECS tasks)"
  value       = module.networking.private_subnet_ids
}

output "cloudwatch_log_group_name" {
  description = "CloudWatch log group name shared by all services"
  value       = module.ecs.cloudwatch_log_group_name
}

output "ops_api_alb_dns_name" {
  description = "DNS name of the ops-api Application Load Balancer"
  value       = module.ecs.ops_api_alb_dns_name
}

output "secret_arns" {
  description = "Map of secret name → Secrets Manager ARN"
  value       = module.secrets.secret_arns
  sensitive   = true
}
