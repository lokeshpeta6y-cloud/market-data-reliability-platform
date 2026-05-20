###############################################################################
# S3 Module — Bronze bucket + Athena workgroup
###############################################################################

locals {
  bucket_name = "${var.project_name}-bronze-${var.environment}"
}

###############################################################################
# Bronze bucket
###############################################################################

resource "aws_s3_bucket" "bronze" {
  bucket        = local.bucket_name
  force_destroy = false

  tags = {
    Name = local.bucket_name
    Layer = "bronze"
  }
}

resource "aws_s3_bucket_versioning" "bronze" {
  bucket = aws_s3_bucket.bronze.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "bronze" {
  bucket = aws_s3_bucket.bronze.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "bronze" {
  bucket = aws_s3_bucket.bronze.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "bronze" {
  bucket = aws_s3_bucket.bronze.id

  rule {
    id     = "bronze-tiering"
    status = "Enabled"

    filter {
      prefix = "events/"
    }

    transition {
      days          = var.transition_to_ia_days
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = var.transition_to_glacier_days
      storage_class = "GLACIER"
    }

    expiration {
      days = var.expiration_days
    }

    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }

  rule {
    id     = "athena-results-cleanup"
    status = "Enabled"

    filter {
      prefix = var.athena_results_prefix
    }

    expiration {
      days = 7
    }
  }
}

###############################################################################
# Bucket policy — restrict access to the ECS task role only
###############################################################################

data "aws_iam_policy_document" "bronze_bucket_policy" {
  statement {
    sid    = "DenyNonTaskRoleAccess"
    effect = "Deny"

    principals {
      type        = "AWS"
      identifiers = ["*"]
    }

    actions = ["s3:*"]

    resources = [
      aws_s3_bucket.bronze.arn,
      "${aws_s3_bucket.bronze.arn}/*",
    ]

    condition {
      test     = "StringNotLike"
      variable = "aws:PrincipalArn"
      values   = [var.ecs_task_role_arn]
    }

    # Allow the account root to retain emergency access
    condition {
      test     = "StringNotEquals"
      variable = "aws:PrincipalType"
      values   = ["Service"]
    }
  }

  statement {
    sid    = "AllowTaskRoleAccess"
    effect = "Allow"

    principals {
      type        = "AWS"
      identifiers = [var.ecs_task_role_arn]
    }

    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
      "s3:GetBucketLocation",
    ]

    resources = [
      aws_s3_bucket.bronze.arn,
      "${aws_s3_bucket.bronze.arn}/*",
    ]
  }

  statement {
    sid    = "AllowSSLOnly"
    effect = "Deny"

    principals {
      type        = "AWS"
      identifiers = ["*"]
    }

    actions = ["s3:*"]

    resources = [
      aws_s3_bucket.bronze.arn,
      "${aws_s3_bucket.bronze.arn}/*",
    ]

    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "bronze" {
  bucket = aws_s3_bucket.bronze.id
  policy = data.aws_iam_policy_document.bronze_bucket_policy.json

  depends_on = [aws_s3_bucket_public_access_block.bronze]
}

###############################################################################
# Athena workgroup for ad-hoc Bronze queries
###############################################################################

resource "aws_athena_workgroup" "bronze_adhoc" {
  name        = "${var.project_name}-bronze-adhoc-${var.environment}"
  description = "Ad-hoc Athena queries against Bronze S3 data"
  state       = "ENABLED"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.bronze.bucket}/${var.athena_results_prefix}"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }

  tags = {
    Name = "${var.project_name}-bronze-adhoc-${var.environment}"
  }
}
