# One-command local workflow. `make check` runs what the required `ci` gate runs (except actionlint).
# Needs uv (https://docs.astral.sh/uv/) and Java 17 for PySpark, or open the repo in the devcontainer.
.DEFAULT_GOAL := help
UV ?= uv

# Homebrew's openjdk@17 is keg-only: use it when JAVA_HOME is not set.
ifeq ($(JAVA_HOME),)
  ifneq ($(wildcard /opt/homebrew/opt/openjdk@17),)
    export JAVA_HOME := /opt/homebrew/opt/openjdk@17
  endif
endif

.PHONY: help setup lint fmt java test check

help: ## List the targets
	@grep -E '^[a-z]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "  make %-6s %s\n", $$1, $$2}'

setup: ## Install Python 3.11 and the locked dev dependencies
	$(UV) sync --group dev --locked

lint: setup ## Ruff lint and format check
	$(UV) run ruff check .
	$(UV) run ruff format --check .

fmt: setup ## Format and auto-fix
	$(UV) run ruff format .
	$(UV) run ruff check --fix .

java:
	@"$${JAVA_HOME:+$$JAVA_HOME/bin/}java" -version 2>&1 | grep -q 'version "17' || { \
	  echo "PySpark tests need Java 17: set JAVA_HOME (macOS: brew install openjdk@17) or use the devcontainer."; exit 1; }

test: lint java ## Lint + unit tests (local Spark + Delta)
	$(UV) run pytest

check: test ## Everything the `ci` gate checks: tests, Fabric item validation, wheel builds
	$(UV) run python tools/validate_fabric_items.py fabric
	$(UV) build --wheel --out-dir dist
