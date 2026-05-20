###############################################################################
# EventBridge Module — Scheduled ECS task triggers
###############################################################################

locals {
  name_prefix = "${var.project_name}-${var.environment}"
}

###############################################################################
# IAM Role — allows EventBridge to run ECS tasks
###############################################################################

data "aws_iam_policy_document" "eventbridge_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "eventbridge_ecs" {
  name               = "${local.name_prefix}-eventbridge-ecs-role"
  assume_role_policy = data.aws_iam_policy_document.eventbridge_assume.json

  tags = {
    Name = "${local.name_prefix}-eventbridge-ecs-role"
  }
}

data "aws_iam_policy_document" "eventbridge_ecs_policy" {
  statement {
    sid    = "RunECSTasks"
    effect = "Allow"

    actions = [
      "ecs:RunTask",
    ]

    resources = [
      # Allow running any revision of both task families
      replace(var.replay_task_def_arn, "/:[0-9]+$/", ":*"),
      replace(var.ops_api_task_def_arn, "/:[0-9]+$/", ":*"),
    ]

    condition {
      test     = "ArnLike"
      variable = "ecs:cluster"
      values   = [var.ecs_cluster_arn]
    }
  }

  statement {
    sid    = "PassRolesToECS"
    effect = "Allow"

    actions = ["iam:PassRole"]

    resources = [
      var.ecs_task_role_arn,
      var.ecs_execution_role_arn,
    ]
  }
}

resource "aws_iam_role_policy" "eventbridge_ecs" {
  name   = "${local.name_prefix}-eventbridge-ecs-policy"
  role   = aws_iam_role.eventbridge_ecs.id
  policy = data.aws_iam_policy_document.eventbridge_ecs_policy.json
}

###############################################################################
# EventBridge Scheduler — daily bronze replay check (06:00 UTC)
###############################################################################

resource "aws_scheduler_schedule" "daily_replay_check" {
  name        = "${local.name_prefix}-daily-replay-check"
  description = "Triggers replay-engine at 06:00 UTC daily to check Bronze layer completeness"
  group_name  = "default"

  schedule_expression          = var.daily_replay_schedule
  schedule_expression_timezone = "UTC"

  flexible_time_window {
    mode                      = "FLEXIBLE"
    maximum_window_in_minutes = 10
  }

  target {
    arn      = var.ecs_cluster_arn
    role_arn = aws_iam_role.eventbridge_ecs.arn

    ecs_parameters {
      task_definition_arn = var.replay_task_def_arn
      launch_type         = "FARGATE"
      task_count          = 1

      network_configuration {
        aws_vpc_configuration {
          subnets          = var.private_subnet_ids
          security_groups  = [var.ecs_security_group_id]
          assign_public_ip = "DISABLED"
        }
      }

      # Override the container command to run the replay check sub-command
      overrides {
        container_override {
          name    = "replay-engine"
          command = ["python", "-m", "replay_engine", "check-bronze"]
        }
      }
    }

    retry_policy {
      maximum_event_age_in_seconds = 3600
      maximum_retry_attempts       = 2
    }
  }
}

###############################################################################
# EventBridge Scheduler — DLQ replay (02:00 UTC daily)
###############################################################################

resource "aws_scheduler_schedule" "dlq_replay" {
  name        = "${local.name_prefix}-dlq-replay"
  description = "Triggers replay-engine at 02:00 UTC daily to re-process DLQ events"
  group_name  = "default"

  schedule_expression          = var.dlq_replay_schedule
  schedule_expression_timezone = "UTC"

  flexible_time_window {
    mode                      = "FLEXIBLE"
    maximum_window_in_minutes = 15
  }

  target {
    arn      = var.ecs_cluster_arn
    role_arn = aws_iam_role.eventbridge_ecs.arn

    ecs_parameters {
      task_definition_arn = var.replay_task_def_arn
      launch_type         = "FARGATE"
      task_count          = 1

      network_configuration {
        aws_vpc_configuration {
          subnets          = var.private_subnet_ids
          security_groups  = [var.ecs_security_group_id]
          assign_public_ip = "DISABLED"
        }
      }

      overrides {
        container_override {
          name    = "replay-engine"
          command = ["python", "-m", "replay_engine", "replay-dlq"]
        }
      }
    }

    retry_policy {
      maximum_event_age_in_seconds = 7200
      maximum_retry_attempts       = 3
    }
  }
}
