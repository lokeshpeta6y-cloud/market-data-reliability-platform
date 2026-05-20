# =============================================================================
# Market Data Reliability Platform — Makefile
# =============================================================================
#
# Usage:
#   make up               Start the full docker-compose stack
#   make down             Stop the stack
#   make test             Run unit tests
#   make test-integration Run integration tests (requires running stack)
#   make test-chaos       Run chaos tests (requires running stack)
#   make lint             Run ruff + mypy
#   make format           Auto-format code with ruff
#   make replay           Trigger a Bronze S3 replay for the last hour
#   make dlq-replay       Trigger a DLQ replay for the last hour
#   make health           Check ops-api health
#   make build            Build all Docker images
#   make push             Push images to ECR (requires AWS_ACCOUNT_ID)
#   make tf-init          Terraform init
#   make tf-plan          Terraform plan
#   make tf-apply         Terraform apply
#   make clean            Remove build artefacts and caches
#
# =============================================================================

SHELL := /bin/bash
.DEFAULT_GOAL := help

# ---------------------------------------------------------------------------
# Configuration — override via environment or command line
# ---------------------------------------------------------------------------

# Docker Compose
COMPOSE_FILE        ?= docker-compose.yml
COMPOSE             := docker compose -f $(COMPOSE_FILE)

# ops-api
OPS_API_URL         ?= http://localhost:8010
REPLAY_LOOKBACK_H   ?= 1

# AWS / ECR
AWS_ACCOUNT_ID      ?= $(error AWS_ACCOUNT_ID must be set for push target)
AWS_REGION          ?= eu-west-1
ECR_REGISTRY        := $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com
IMAGE_TAG           ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo latest)

# Terraform
TF_DIR              := infra/terraform/environments/prod
TF_VARS             ?=

# Python tooling
PYTHON              ?= python
PYTEST              := $(PYTHON) -m pytest
RUFF                := $(PYTHON) -m ruff
MYPY                := $(PYTHON) -m mypy

# Source roots for linting
SRC_PATHS           := libs/common/src services tests

# ---------------------------------------------------------------------------
# Phony targets
# ---------------------------------------------------------------------------

.PHONY: help up down logs build push \
        test test-integration test-chaos \
        lint format \
        replay dlq-replay health \
        tf-init tf-plan tf-apply \
        clean

# ---------------------------------------------------------------------------
# Help
# ---------------------------------------------------------------------------

help: ## Show this help message
	@echo ""
	@echo "Market Data Reliability Platform"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'
	@echo ""

# ---------------------------------------------------------------------------
# Docker Compose
# ---------------------------------------------------------------------------

up: ## Start the full stack in the background (build images first)
	$(COMPOSE) up -d --build

down: ## Stop and remove all containers
	$(COMPOSE) down

logs: ## Tail all service logs (Ctrl-C to stop)
	$(COMPOSE) logs -f

build: ## Build all Docker images without starting containers
	$(COMPOSE) build

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

test: ## Run unit tests (no external dependencies required)
	$(PYTEST) -m unit tests/unit/ -v

test-integration: ## Run integration tests (requires running stack: make up first)
	INTEGRATION_TESTS=true $(PYTEST) -m integration tests/integration/ -v

test-chaos: ## Run chaos tests (requires running stack: make up first)
	CHAOS_TESTS=true $(PYTEST) -m chaos tests/chaos/ -v -s

# ---------------------------------------------------------------------------
# Code quality
# ---------------------------------------------------------------------------

lint: ## Run ruff linter and mypy type checker
	$(RUFF) check $(SRC_PATHS)
	$(MYPY) $(SRC_PATHS) \
		--ignore-missing-imports \
		--no-error-summary

format: ## Auto-format code with ruff
	$(RUFF) format $(SRC_PATHS)
	$(RUFF) check --fix $(SRC_PATHS)

# ---------------------------------------------------------------------------
# Operational commands
# ---------------------------------------------------------------------------

replay: ## Trigger a Bronze S3 replay for the last $(REPLAY_LOOKBACK_H) hour(s)
	@echo "Triggering Bronze replay (last $(REPLAY_LOOKBACK_H)h)..."
	@START_TIME=$$(date -u -d "-$(REPLAY_LOOKBACK_H) hour" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null \
		|| date -u -v -$(REPLAY_LOOKBACK_H)H +%Y-%m-%dT%H:%M:%SZ); \
	END_TIME=$$(date -u +%Y-%m-%dT%H:%M:%SZ); \
	curl -s -X POST "$(OPS_API_URL)/api/v1/replay/bronze" \
		-H "Content-Type: application/json" \
		-d "{\"source\":\"bronze_s3\",\"start_time\":\"$$START_TIME\",\"end_time\":\"$$END_TIME\"}" \
		| python -m json.tool

dlq-replay: ## Trigger a DLQ replay for the last $(REPLAY_LOOKBACK_H) hour(s)
	@echo "Triggering DLQ replay (last $(REPLAY_LOOKBACK_H)h)..."
	@START_TIME=$$(date -u -d "-$(REPLAY_LOOKBACK_H) hour" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null \
		|| date -u -v -$(REPLAY_LOOKBACK_H)H +%Y-%m-%dT%H:%M:%SZ); \
	END_TIME=$$(date -u +%Y-%m-%dT%H:%M:%SZ); \
	curl -s -X POST "$(OPS_API_URL)/api/v1/replay/dlq" \
		-H "Content-Type: application/json" \
		-d "{\"source\":\"dlq\",\"start_time\":\"$$START_TIME\",\"end_time\":\"$$END_TIME\"}" \
		| python -m json.tool

health: ## Query ops-api /api/v1/status
	@curl -s "$(OPS_API_URL)/api/v1/status" | python -m json.tool

# ---------------------------------------------------------------------------
# ECR push
# ---------------------------------------------------------------------------

push: build ## Build images and push to ECR (requires AWS_ACCOUNT_ID)
	aws ecr get-login-password --region $(AWS_REGION) \
		| docker login --username AWS --password-stdin $(ECR_REGISTRY)
	@for svc in \
		provider-emulator \
		validation-service \
		bronze-writer \
		normalization-service \
		redis-writer \
		silver-loader \
		gold-loader \
		replay-engine \
		ops-api; do \
		echo "Pushing $$svc:$(IMAGE_TAG)..."; \
		docker tag mdrp-$$svc:latest $(ECR_REGISTRY)/mdrp-$$svc:$(IMAGE_TAG); \
		docker tag mdrp-$$svc:latest $(ECR_REGISTRY)/mdrp-$$svc:latest; \
		docker push $(ECR_REGISTRY)/mdrp-$$svc:$(IMAGE_TAG); \
		docker push $(ECR_REGISTRY)/mdrp-$$svc:latest; \
	done

# ---------------------------------------------------------------------------
# Terraform
# ---------------------------------------------------------------------------

tf-init: ## Initialise Terraform in $(TF_DIR)
	terraform -chdir=$(TF_DIR) init $(TF_VARS)

tf-plan: ## Show Terraform plan
	terraform -chdir=$(TF_DIR) plan $(TF_VARS)

tf-apply: ## Apply Terraform plan (prompts for confirmation)
	terraform -chdir=$(TF_DIR) apply $(TF_VARS)

# ---------------------------------------------------------------------------
# Clean
# ---------------------------------------------------------------------------

clean: ## Remove __pycache__, .pytest_cache, build artefacts, coverage data
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	find . -type f -name "*.pyo" -delete 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".ruff_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".eggs" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "dist" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "build" -exec rm -rf {} + 2>/dev/null || true
	rm -f .coverage
	rm -rf htmlcov/
	@echo "Clean complete."
