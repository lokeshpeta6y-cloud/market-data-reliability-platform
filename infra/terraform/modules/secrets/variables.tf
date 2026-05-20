variable "project_name" {
  description = "Short project identifier"
  type        = string
}

variable "environment" {
  description = "Deployment environment"
  type        = string
}

variable "databento_api_key" {
  description = "Databento API key"
  type        = string
  sensitive   = true
}

variable "snowflake_account" {
  description = "Snowflake account identifier"
  type        = string
}

variable "snowflake_user" {
  description = "Snowflake service user"
  type        = string
}

variable "snowflake_password" {
  description = "Snowflake service user password"
  type        = string
  sensitive   = true
}

variable "smtp_host" {
  description = "SMTP relay hostname"
  type        = string
  default     = ""
}

variable "smtp_username" {
  description = "SMTP username"
  type        = string
  default     = ""
}

variable "smtp_password" {
  description = "SMTP password"
  type        = string
  sensitive   = true
  default     = ""
}

variable "teams_webhook_url" {
  description = "Microsoft Teams incoming-webhook URL"
  type        = string
  sensitive   = true
  default     = ""
}

variable "recovery_window_days" {
  description = "Days before a deleted secret is permanently removed (0 to force-delete)"
  type        = number
  default     = 7
}
