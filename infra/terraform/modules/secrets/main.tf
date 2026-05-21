###############################################################################
# Secrets Manager Module
###############################################################################

locals {
  name_prefix = "${var.project_name}/${var.environment}"
}

###############################################################################
# Databento API key
###############################################################################

resource "aws_secretsmanager_secret" "databento_api_key" {
  name                    = "${local.name_prefix}/databento-api-key"
  description             = "Databento market-data API key for ${var.environment}"
  recovery_window_in_days = var.recovery_window_days

  tags = {
    Name      = "${local.name_prefix}/databento-api-key"
    SecretFor = "databento"
  }
}

resource "aws_secretsmanager_secret_version" "databento_api_key" {
  secret_id     = aws_secretsmanager_secret.databento_api_key.id
  secret_string = var.databento_api_key
}

###############################################################################
# Snowflake credentials (stored as a JSON object)
###############################################################################

resource "aws_secretsmanager_secret" "snowflake_account" {
  name                    = "${local.name_prefix}/snowflake-account"
  description             = "Snowflake account identifier for ${var.environment}"
  recovery_window_in_days = var.recovery_window_days

  tags = {
    Name      = "${local.name_prefix}/snowflake-account"
    SecretFor = "snowflake"
  }
}

resource "aws_secretsmanager_secret_version" "snowflake_account" {
  secret_id     = aws_secretsmanager_secret.snowflake_account.id
  secret_string = var.snowflake_account
}

resource "aws_secretsmanager_secret" "snowflake_user" {
  name                    = "${local.name_prefix}/snowflake-user"
  description             = "Snowflake service user name for ${var.environment}"
  recovery_window_in_days = var.recovery_window_days

  tags = {
    Name      = "${local.name_prefix}/snowflake-user"
    SecretFor = "snowflake"
  }
}

resource "aws_secretsmanager_secret_version" "snowflake_user" {
  secret_id     = aws_secretsmanager_secret.snowflake_user.id
  secret_string = var.snowflake_user
}

resource "aws_secretsmanager_secret" "snowflake_password" {
  name                    = "${local.name_prefix}/snowflake-password"
  description             = "Snowflake service user password for ${var.environment} (fallback — prefer PAT)"
  recovery_window_in_days = var.recovery_window_days

  tags = {
    Name      = "${local.name_prefix}/snowflake-password"
    SecretFor = "snowflake"
  }
}

resource "aws_secretsmanager_secret_version" "snowflake_password" {
  secret_id     = aws_secretsmanager_secret.snowflake_password.id
  secret_string = var.snowflake_password
}

###############################################################################
# Snowflake Programmatic Access Token (PAT) — preferred auth method
###############################################################################

resource "aws_secretsmanager_secret" "snowflake_pat_token" {
  name                    = "${local.name_prefix}/snowflake-pat-token"
  description             = "Snowflake PAT token for ${var.environment} — takes precedence over password"
  recovery_window_in_days = var.recovery_window_days

  tags = {
    Name      = "${local.name_prefix}/snowflake-pat-token"
    SecretFor = "snowflake"
  }
}

resource "aws_secretsmanager_secret_version" "snowflake_pat_token" {
  secret_id     = aws_secretsmanager_secret.snowflake_pat_token.id
  secret_string = var.snowflake_pat_token
}

###############################################################################
# SMTP credentials
###############################################################################

resource "aws_secretsmanager_secret" "smtp_credentials" {
  name                    = "${local.name_prefix}/smtp-credentials"
  description             = "SMTP relay credentials for ops alerting in ${var.environment}"
  recovery_window_in_days = var.recovery_window_days

  tags = {
    Name      = "${local.name_prefix}/smtp-credentials"
    SecretFor = "smtp"
  }
}

resource "aws_secretsmanager_secret_version" "smtp_credentials" {
  secret_id = aws_secretsmanager_secret.smtp_credentials.id
  secret_string = jsonencode({
    host     = var.smtp_host
    username = var.smtp_username
    password = var.smtp_password
  })
}

# Individual password secret for ECS container injection
resource "aws_secretsmanager_secret" "smtp_password" {
  name                    = "${local.name_prefix}/smtp-password"
  description             = "SMTP password injected into ECS containers in ${var.environment}"
  recovery_window_in_days = var.recovery_window_days

  tags = {
    Name      = "${local.name_prefix}/smtp-password"
    SecretFor = "smtp"
  }
}

resource "aws_secretsmanager_secret_version" "smtp_password" {
  secret_id     = aws_secretsmanager_secret.smtp_password.id
  secret_string = var.smtp_password
}

###############################################################################
# Microsoft Teams webhook
###############################################################################

resource "aws_secretsmanager_secret" "teams_webhook_url" {
  name                    = "${local.name_prefix}/teams-webhook-url"
  description             = "Microsoft Teams incoming-webhook URL for ops alerts in ${var.environment}"
  recovery_window_in_days = var.recovery_window_days

  tags = {
    Name      = "${local.name_prefix}/teams-webhook-url"
    SecretFor = "teams"
  }
}

resource "aws_secretsmanager_secret_version" "teams_webhook_url" {
  secret_id     = aws_secretsmanager_secret.teams_webhook_url.id
  secret_string = var.teams_webhook_url
}
