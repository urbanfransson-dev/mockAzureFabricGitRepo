# =============================================================================
# Makefile — Contoso Fabric Data Platform
# Common developer tasks. Run `make help` for a full list.
# =============================================================================

.PHONY: help install install-dev lint format test test-unit test-integration \
        test-coverage check clean deploy-dev deploy-bicep-dev

PYTHON      := python3
PIP         := pip
PYTEST      := pytest
RUFF        := ruff
BLACK       := black
MYPY        := mypy
BANDIT      := bandit
SQLFLUFF    := sqlfluff

SRC_DIRS    := src tests
SQL_DIR     := sql

# ── Help ───────────────────────────────────────────────────────────────────────
help:
	@echo ""
	@echo "Contoso Fabric Data Platform — Available Make targets:"
	@echo "──────────────────────────────────────────────────────"
	@echo "  install           Install production dependencies"
	@echo "  install-dev       Install development dependencies"
	@echo "  lint              Run all linting checks (ruff, black, mypy)"
	@echo "  format            Auto-format code with black and ruff"
	@echo "  test              Run all tests"
	@echo "  test-unit         Run unit tests only"
	@echo "  test-integration  Run integration tests (requires env vars)"
	@echo "  test-coverage     Run tests with coverage report"
	@echo "  sql-lint          Lint SQL files with sqlfluff"
	@echo "  security-scan     Run bandit security scan"
	@echo "  check             Run all checks (lint + test)"
	@echo "  clean             Remove build artifacts and caches"
	@echo "  deploy-bicep-dev  Deploy Bicep to dev resource group"
	@echo ""

# ── Install ────────────────────────────────────────────────────────────────────
install:
	$(PIP) install --upgrade pip
	$(PIP) install -e .

install-dev:
	$(PIP) install --upgrade pip
	$(PIP) install -e ".[dev]"

# ── Lint & Format ──────────────────────────────────────────────────────────────
lint:
	$(RUFF) check $(SRC_DIRS)
	$(BLACK) --check $(SRC_DIRS)
	$(MYPY) src/ --ignore-missing-imports

format:
	$(BLACK) $(SRC_DIRS)
	$(RUFF) check --fix $(SRC_DIRS)

sql-lint:
	$(SQLFLUFF) lint $(SQL_DIR) --dialect tsql

security-scan:
	$(BANDIT) -r src/ -c pyproject.toml --severity-level medium

# ── Tests ──────────────────────────────────────────────────────────────────────
test:
	$(PYTEST) tests/ -v

test-unit:
	$(PYTEST) tests/unit/ -v

test-integration:
	RUN_INTEGRATION_TESTS=true $(PYTEST) tests/integration/ -v --timeout=120

test-coverage:
	$(PYTEST) tests/unit/ \
		--cov=src \
		--cov-report=html:htmlcov \
		--cov-report=term-missing \
		-v
	@echo "Coverage report: htmlcov/index.html"

# ── Combined checks ────────────────────────────────────────────────────────────
check: lint test-unit sql-lint security-scan
	@echo "All checks passed."

# ── Infra deployment ───────────────────────────────────────────────────────────
deploy-bicep-dev:
	az deployment group create \
		--resource-group rg-contoso-fabric-dev \
		--template-file infra/bicep/main.bicep \
		--parameters @infra/bicep/parameters/dev.parameters.json \
		--name deploy-$$(date +%Y%m%d-%H%M%S)

# ── Clean ──────────────────────────────────────────────────────────────────────
clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "htmlcov" -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
	find . -name "coverage.xml" -delete 2>/dev/null || true
	find . -name ".coverage" -delete 2>/dev/null || true
	@echo "Clean complete."
