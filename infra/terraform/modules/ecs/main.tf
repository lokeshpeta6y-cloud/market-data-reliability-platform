###############################################################################
# ECS Module — Cluster, ECR, IAM roles, Task Definitions, Services, ALB
###############################################################################

data "aws_caller_identity" "current" {}

locals {
  name_prefix    = "${var.project_name}-${var.environment}"
  log_group_name = "/mdrp/${var.environment}"
  account_id     = data.aws_caller_identity.current.account_id
}

###############################################################################
# CloudWatch Log Group
###############################################################################

resource "aws_cloudwatch_log_group" "mdrp" {
  name              = local.log_group_name
  retention_in_days = var.log_retention_days

  tags = {
    Name = local.log_group_name
  }
}

###############################################################################
# ECR Repositories
###############################################################################

resource "aws_ecr_repository" "services" {
  for_each = toset(var.ecr_repository_names)

  name                 = "${var.project_name}/${each.key}"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  encryption_configuration {
    encryption_type = "AES256"
  }

  tags = {
    Name    = "${var.project_name}/${each.key}"
    Service = each.key
  }
}

resource "aws_ecr_lifecycle_policy" "services" {
  for_each   = aws_ecr_repository.services
  repository = each.value.name

  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Keep last 10 tagged images"
        selection = {
          tagStatus     = "tagged"
          tagPrefixList = ["v"]
          countType     = "imageCountMoreThan"
          countNumber   = 10
        }
        action = { type = "expire" }
      },
      {
        rulePriority = 2
        description  = "Remove untagged images older than 7 days"
        selection = {
          tagStatus   = "untagged"
          countType   = "sinceImagePushed"
          countUnit   = "days"
          countNumber = 7
        }
        action = { type = "expire" }
      },
    ]
  })
}

###############################################################################
# ECS Cluster
###############################################################################

resource "aws_ecs_cluster" "main" {
  name = "${local.name_prefix}-cluster"

  setting {
    name  = "containerInsights"
    value = "enabled"
  }

  tags = {
    Name = "${local.name_prefix}-cluster"
  }
}

resource "aws_ecs_cluster_capacity_providers" "main" {
  cluster_name       = aws_ecs_cluster.main.name
  capacity_providers = ["FARGATE", "FARGATE_SPOT"]

  default_capacity_provider_strategy {
    capacity_provider = "FARGATE"
    weight            = 1
    base              = 1
  }
}

###############################################################################
# IAM — Task Execution Role (pull images, write logs, read secrets)
###############################################################################

data "aws_iam_policy_document" "ecs_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "ecs_execution" {
  name               = "${local.name_prefix}-ecs-execution-role"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json

  tags = {
    Name = "${local.name_prefix}-ecs-execution-role"
  }
}

resource "aws_iam_role_policy_attachment" "ecs_execution_managed" {
  role       = aws_iam_role.ecs_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

data "aws_iam_policy_document" "ecs_execution_extras" {
  statement {
    sid    = "SecretsManagerRead"
    effect = "Allow"

    actions = [
      "secretsmanager:GetSecretValue",
      "secretsmanager:DescribeSecret",
    ]

    resources = values(var.secret_arns)
  }

  statement {
    sid    = "ECRPull"
    effect = "Allow"

    actions = [
      "ecr:GetDownloadUrlForLayer",
      "ecr:BatchGetImage",
      "ecr:BatchCheckLayerAvailability",
      "ecr:GetAuthorizationToken",
    ]

    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "ecs_execution_extras" {
  name   = "${local.name_prefix}-ecs-execution-extras"
  role   = aws_iam_role.ecs_execution.id
  policy = data.aws_iam_policy_document.ecs_execution_extras.json
}

###############################################################################
# IAM — Task Role (S3 bronze read/write, CloudWatch metrics)
###############################################################################

resource "aws_iam_role" "ecs_task" {
  name               = "${local.name_prefix}-ecs-task-role"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json

  tags = {
    Name = "${local.name_prefix}-ecs-task-role"
  }
}

data "aws_iam_policy_document" "ecs_task_policy" {
  statement {
    sid    = "S3BronzeReadWrite"
    effect = "Allow"

    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
      "s3:GetBucketLocation",
      "s3:GetObjectVersion",
    ]

    resources = [
      var.s3_bronze_bucket_arn,
      "${var.s3_bronze_bucket_arn}/*",
    ]
  }

  statement {
    sid    = "CloudWatchMetrics"
    effect = "Allow"

    actions = [
      "cloudwatch:PutMetricData",
      "cloudwatch:GetMetricStatistics",
      "cloudwatch:ListMetrics",
    ]

    resources = ["*"]
  }

  statement {
    sid    = "XRayTracing"
    effect = "Allow"

    actions = [
      "xray:PutTraceSegments",
      "xray:PutTelemetryRecords",
    ]

    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "ecs_task" {
  name   = "${local.name_prefix}-ecs-task-policy"
  role   = aws_iam_role.ecs_task.id
  policy = data.aws_iam_policy_document.ecs_task_policy.json
}

###############################################################################
# Security Groups
###############################################################################

resource "aws_security_group" "ecs_tasks" {
  name        = "${local.name_prefix}-ecs-tasks-sg"
  description = "Allow outbound traffic from ECS tasks; no inbound from internet"
  vpc_id      = var.vpc_id

  egress {
    description = "Allow all outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${local.name_prefix}-ecs-tasks-sg"
  }
}

resource "aws_security_group" "alb" {
  name        = "${local.name_prefix}-alb-sg"
  description = "Allow HTTP inbound to ops-api ALB"
  vpc_id      = var.vpc_id

  ingress {
    description = "HTTP from anywhere"
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description = "ops-api port from anywhere"
    from_port   = 8000
    to_port     = 8000
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    description = "Allow all outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${local.name_prefix}-alb-sg"
  }
}

# Allow ALB to reach ops-api container
resource "aws_security_group_rule" "alb_to_ecs" {
  type                     = "ingress"
  from_port                = var.ops_api_container_port
  to_port                  = var.ops_api_container_port
  protocol                 = "tcp"
  source_security_group_id = aws_security_group.alb.id
  security_group_id        = aws_security_group.ecs_tasks.id
  description              = "Allow ALB to reach ops-api"
}

###############################################################################
# Application Load Balancer — ops-api only
###############################################################################

resource "aws_lb" "ops_api" {
  name               = "${local.name_prefix}-ops-api-alb"
  internal           = false
  load_balancer_type = "application"
  security_groups    = [aws_security_group.alb.id]
  subnets            = var.public_subnet_ids

  enable_deletion_protection = false

  tags = {
    Name    = "${local.name_prefix}-ops-api-alb"
    Service = "ops-api"
  }
}

resource "aws_lb_target_group" "ops_api" {
  name        = "${local.name_prefix}-ops-api-tg"
  port        = var.ops_api_container_port
  protocol    = "HTTP"
  vpc_id      = var.vpc_id
  target_type = "ip"

  health_check {
    enabled             = true
    path                = "/health"
    healthy_threshold   = 2
    unhealthy_threshold = 3
    timeout             = 5
    interval            = 30
    matcher             = "200"
  }

  tags = {
    Name    = "${local.name_prefix}-ops-api-tg"
    Service = "ops-api"
  }
}

resource "aws_lb_listener" "ops_api_http" {
  load_balancer_arn = aws_lb.ops_api.arn
  port              = 80
  protocol          = "HTTP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.ops_api.arn
  }
}

###############################################################################
# ECS Task Definitions
###############################################################################

# Secrets injected into all containers via Secrets Manager references
locals {
  # Build a list of secret injection objects for the task definition.
  # Each entry maps an environment variable name to a Secrets Manager ARN.
  common_secrets = [
    {
      name      = "DATABENTO_API_KEY"
      valueFrom = lookup(var.secret_arns, "databento-api-key", "")
    },
    {
      name      = "SNOWFLAKE_ACCOUNT"
      valueFrom = lookup(var.secret_arns, "snowflake-account", "")
    },
    {
      name      = "SNOWFLAKE_USER"
      valueFrom = lookup(var.secret_arns, "snowflake-user", "")
    },
    {
      name      = "SNOWFLAKE_PASSWORD"
      valueFrom = lookup(var.secret_arns, "snowflake-password", "")
    },
    {
      name      = "SMTP_PASSWORD"
      valueFrom = lookup(var.secret_arns, "smtp-password", "")
    },
    {
      name      = "TEAMS_WEBHOOK_URL"
      valueFrom = lookup(var.secret_arns, "teams-webhook-url", "")
    },
  ]

  # Filter out entries where the ARN is empty (secret not provisioned)
  injected_secrets = [for s in local.common_secrets : s if s.valueFrom != ""]

  common_environment = [
    { name = "ENVIRONMENT", value = var.environment },
    { name = "AWS_REGION", value = var.aws_region },
    { name = "LOG_LEVEL", value = "INFO" },
    { name = "S3_BRONZE_BUCKET", value = split(":::", var.s3_bronze_bucket_arn)[1] },
  ]
}

resource "aws_ecs_task_definition" "services" {
  for_each = toset(var.ecr_repository_names)

  family                   = "${local.name_prefix}-${each.key}"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.task_cpu
  memory                   = var.task_memory
  execution_role_arn       = aws_iam_role.ecs_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([
    {
      name      = each.key
      image     = "${local.account_id}.dkr.ecr.${var.aws_region}.amazonaws.com/${var.project_name}/${each.key}:${var.container_image_tag}"
      essential = true
      cpu       = var.task_cpu
      memory    = var.task_memory

      portMappings = each.key == "ops-api" ? [
        {
          containerPort = var.ops_api_container_port
          hostPort      = var.ops_api_container_port
          protocol      = "tcp"
        }
      ] : []

      environment = local.common_environment
      secrets     = local.injected_secrets

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.mdrp.name
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = each.key
        }
      }

      healthCheck = {
        command     = ["CMD-SHELL", "exit 0"]
        interval    = 30
        timeout     = 5
        retries     = 3
        startPeriod = 60
      }
    }
  ])

  tags = {
    Name    = "${local.name_prefix}-${each.key}"
    Service = each.key
  }
}

###############################################################################
# ECS Services
###############################################################################

resource "aws_ecs_service" "services" {
  for_each = toset(var.ecr_repository_names)

  name            = "${local.name_prefix}-${each.key}"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.services[each.key].arn
  desired_count   = var.service_desired_count
  launch_type     = "FARGATE"

  # Allow Fargate to replace tasks without waiting for draining when
  # updating task definitions during deployments.
  deployment_minimum_healthy_percent = 50
  deployment_maximum_percent         = 200

  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = [aws_security_group.ecs_tasks.id]
    assign_public_ip = false
  }

  dynamic "load_balancer" {
    for_each = each.key == "ops-api" ? [1] : []
    content {
      target_group_arn = aws_lb_target_group.ops_api.arn
      container_name   = "ops-api"
      container_port   = var.ops_api_container_port
    }
  }

  deployment_circuit_breaker {
    enable   = true
    rollback = true
  }

  lifecycle {
    # Ignore task definition changes driven by CI/CD — Terraform manages infra,
    # not the active image tag.
    ignore_changes = [task_definition, desired_count]
  }

  tags = {
    Name    = "${local.name_prefix}-${each.key}"
    Service = each.key
  }

  depends_on = [
    aws_iam_role_policy_attachment.ecs_execution_managed,
    aws_iam_role_policy.ecs_execution_extras,
    aws_iam_role_policy.ecs_task,
  ]
}
