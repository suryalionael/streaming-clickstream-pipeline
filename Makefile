.PHONY: help install dev-install lint format typecheck test test-cov
.PHONY: compose-up compose-down compose-build compose-logs
.PHONY: clean setup pre-commit ci

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install production dependencies
	pip install -r requirements.txt

dev-install: ## Install dev dependencies
	pip install -r requirements-dev.txt
	pre-commit install

lint: ## Run Ruff linter
	ruff check producer/ spark/ dashboard/ storage/ tests/

format: ## Run formatters
	black producer/ spark/ dashboard/ storage/ tests/
	isort producer/ spark/ dashboard/ storage/ tests/
	ruff check --fix producer/ spark/ dashboard/ storage/ tests/

typecheck: ## Run mypy type checking
	mypy producer/ spark/ dashboard/ storage/

test: ## Run tests
	pytest tests/ -m "not docker" --timeout=30

test-cov: ## Run tests with coverage
	pytest tests/ -m "not docker" --cov=producer --cov=spark --cov=dashboard --cov=storage \
		--cov-report=term-missing --cov-report=html --timeout=30

compose-build: ## Build Docker images
	docker compose build

compose-up: ## Start all services
	docker compose up -d

compose-down: ## Stop all services
	docker compose down -v

compose-logs: ## Follow logs
	docker compose logs -f

compose-smoke: ## Run Docker smoke tests
	docker compose up -d
	@sleep 30
	pytest tests/test_docker_smoke.py -v --timeout=120
	docker compose down -v

clean: ## Clean build artifacts
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .ruff_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .mypy_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	rm -rf htmlcov/ .coverage
	rm -rf data/

setup: dev-install ## Full development setup

pre-commit: ## Run pre-commit on all files
	pre-commit run --all-files

ci: lint typecheck test ## Run CI checks locally
