###############################################################################
# Eval User Module
#
# Creates a time-limited IAM user for external evaluators.
# Permissions are intentionally minimal:
#   - Read Bronze S3 data (mdrp-bronze bucket)
#   - Read the Snowflake PAT from Secrets Manager
#
# The access key output must be shared with the evaluator out-of-band
# (e.g. encrypted email / 1Password).  Never commit credentials.
###############################################################################

locals {
  name_prefix = "${var.project_name}/${var.environment}"
}

###############################################################################
# IAM User
###############################################################################

resource "aws_iam_user" "eval" {
  name = "${var.project_name}-eval-${var.environment}"
  path = "/eval/"

  tags = {
    Purpose   = "evaluation"
    ExpiresOn = var.expiry_date
  }
}

###############################################################################
# Scoped policy — read-only access to Bronze data + Snowflake PAT
###############################################################################

data "aws_iam_policy_document" "eval" {
  statement {
    sid    = "BronzeRead"
    effect = "Allow"

    actions = [
      "s3:GetObject",
      "s3:ListBucket",
      "s3:GetBucketLocation",
    ]

    resources = [
      "arn:aws:s3:::${var.bronze_bucket_name}",
      "arn:aws:s3:::${var.bronze_bucket_name}/*",
    ]
  }

  statement {
    sid    = "SnowflakePATRead"
    effect = "Allow"

    actions = ["secretsmanager:GetSecretValue"]

    resources = [
      "arn:aws:secretsmanager:${var.aws_region}:${var.aws_account_id}:secret:${local.name_prefix}/snowflake-pat-token*",
    ]
  }

  # Deny everything not in the allow list above (belt-and-braces)
  statement {
    sid    = "DenyConsoleAccess"
    effect = "Deny"

    actions = [
      "iam:*",
      "ec2:*",
      "rds:*",
      "ecs:*",
      "logs:*",
    ]

    resources = ["*"]
  }
}

resource "aws_iam_user_policy" "eval" {
  name   = "${var.project_name}-eval-policy"
  user   = aws_iam_user.eval.name
  policy = data.aws_iam_policy_document.eval.json
}

###############################################################################
# Programmatic access key
###############################################################################

resource "aws_iam_access_key" "eval" {
  user = aws_iam_user.eval.name
}
