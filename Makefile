.PHONY: help install dev-install clean test test-unit test-integration test-cov lint lint-fix format format-check type-check
.PHONY: run run-dev run-dry build quality-gate docker-build docker-push podman-build podman-push
.DEFAULT_GOAL := help

UV := uv
PROJECT_NAME := platspec-operator
NAMESPACE := platspec-system
IMAGE_REPO := ghcr.io/platformspec/platspec-operator
IMAGE_TAG  := $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
REPO_ROOT  := $(shell git rev-parse --show-toplevel 2>/dev/null || realpath .)

help:
	@grep -E '^[a-zA-Z_-]+.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install production dependencies
	$(UV) sync --no-dev

dev-install: ## Install all dependencies including dev tools
	$(UV) sync --extra dev

clean: ## Clean build artifacts and caches
	rm -rf build/ dist/ *.egg-info/ .pytest_cache/ .coverage htmlcov/ .mypy_cache/ .ruff_cache/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete

test: ## Run all tests (unit only; use test-integration for k8s tests)
	$(UV) run pytest tests/ -v

test-unit: ## Run unit tests only (no k8s cluster required)
	$(UV) run pytest tests/unit/ -v

test-integration: ## Run integration tests (requires a live k8s cluster)
	$(UV) run pytest tests/integration/ -v -m integration

test-cov: ## Run unit tests with coverage report
	$(UV) run pytest tests/unit/ -v --cov=platspec_operator --cov-report=term-missing --cov-report=html

lint: ## Run linting checks
	$(UV) run ruff check src/

lint-fix: ## Run linting with auto-fix
	$(UV) run ruff check src/ --fix

format: ## Format code
	$(UV) run ruff format src/

format-check: ## Check code formatting
	$(UV) run ruff format src/ --check

type-check: ## Run type checking
	$(UV) run mypy src/platspec_operator/

run: ## Run the operator (production mode)
	$(UV) run platspec-operator

run-dev: ## Run the operator in development mode
	$(UV) run platspec-operator --dev --log-level DEBUG --config-file config/dev.yaml

run-dry: ## Run the operator in dry-run mode
	$(UV) run platspec-operator --dev --dry-run --log-level DEBUG

build: ## Build the Python package (wheel + sdist)
	$(UV) build

docker-build: ## Build the container image (context: repo root)
	docker build \
		-f Dockerfile \
		-t $(IMAGE_REPO):$(IMAGE_TAG) \
		-t $(IMAGE_REPO):latest \
		$(REPO_ROOT)

docker-push: ## Push the container image to the registry
	docker push $(IMAGE_REPO):$(IMAGE_TAG)
	docker push $(IMAGE_REPO):latest

podman-build: ## Build the container image with podman (context: repo root)
	podman build \
		-f Dockerfile \
		-t $(IMAGE_REPO):$(IMAGE_TAG) \
		-t $(IMAGE_REPO):latest \
		$(REPO_ROOT)

podman-push: ## Push the container image to the registry with podman
	podman push $(IMAGE_REPO):$(IMAGE_TAG)
	podman push $(IMAGE_REPO):latest

quality-gate: lint format-check type-check test ## Run all quality checks
	@echo "All quality checks passed!"

logs: ## Show operator logs
	kubectl logs -f deployment/$(PROJECT_NAME) -n $(NAMESPACE)

crds-install: ## Install Custom Resource Definitions
	kubectl apply -f deploy/crds/

crds-uninstall: ## Remove Custom Resource Definitions
	kubectl delete -f deploy/crds/ --ignore-not-found=true

deploy: ## Deploy operator to Kubernetes cluster
	kubectl apply -f deploy/

undeploy: ## Remove operator from Kubernetes cluster
	kubectl delete -f deploy/ --ignore-not-found=true
