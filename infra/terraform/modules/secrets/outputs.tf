output "secret_arns" {
  description = "Map of logical secret name → Secrets Manager ARN (used by ECS task execution role)"
  sensitive   = true
  value = {
    "databento-api-key"     = aws_secretsmanager_secret.databento_api_key.arn
    "snowflake-account"     = aws_secretsmanager_secret.snowflake_account.arn
    "snowflake-user"        = aws_secretsmanager_secret.snowflake_user.arn
    "snowflake-password"    = aws_secretsmanager_secret.snowflake_password.arn
    "snowflake-pat-token"   = aws_secretsmanager_secret.snowflake_pat_token.arn
    "smtp-credentials"      = aws_secretsmanager_secret.smtp_credentials.arn
    "smtp-password"         = aws_secretsmanager_secret.smtp_password.arn
    "teams-webhook-url"     = aws_secretsmanager_secret.teams_webhook_url.arn
  }
}

output "databento_api_key_secret_arn" {
  description = "ARN of the Databento API key secret"
  sensitive   = true
  value       = aws_secretsmanager_secret.databento_api_key.arn
}

output "snowflake_password_secret_arn" {
  description = "ARN of the Snowflake password secret"
  sensitive   = true
  value       = aws_secretsmanager_secret.snowflake_password.arn
}

output "snowflake_pat_token_secret_arn" {
  description = "ARN of the Snowflake PAT token secret (shared with eval user)"
  sensitive   = true
  value       = aws_secretsmanager_secret.snowflake_pat_token.arn
}
