# arbitrum-grant-pipelines — operator entry points.
#
# Everything runs out of ./.venv; `make install` builds it. The pipeline
# schedule itself lives in scripts/run_all.py, which these targets wrap — pass
# extra flags through ARGS, e.g. `make incremental ARGS="--only clanker_tokens"`.

PY      := .venv/bin/python
RUN_ALL := $(PY) scripts/run_all.py

# Smoke-test cap, fanned out to every pipeline as --limit.
LIMIT ?= 50
# Extra flags appended to the run_all targets.
ARGS ?=

# Every pipeline except hyperliquid_activity, which reads a *sealed* arb_cohort
# run and a dry run seals nothing — so it can only be smoked once a real cohort
# exists: make smoke ARGS="--only hyperliquid_activity".
SMOKE_PIPELINES := linked_wallets,contract_deployers,miniapp_builders,brand_engagement,clanker_tokens,bankr_tokens,token_buyers,popular_tokens,token_evangelists,arb_cohort

.DEFAULT_GOAL := help
.PHONY: help install test smoke plan backfill incremental constraints clean clean-runs

help: ## Show this help
	@echo "arbitrum-grant-pipelines"
	@grep -hE '^[a-z][a-z-]*:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN{FS=":.*?## "}{printf "  %-13s %s\n", $$1, $$2}'
	@echo ""
	@echo "  vars: LIMIT=$(LIMIT)  ARGS='$(ARGS)'"

install: ## Create .venv, install requirements, copy .env.example to .env
	@test -d .venv || python3 -m venv .venv
	$(PY) -m pip install --quiet --upgrade pip
	$(PY) -m pip install -r requirements.txt
	@test -f .env || { cp .env.example .env; \
		echo "created .env — fill in DUNE_API_KEY, NEYNAR_API_KEY, NEO4J_*"; }
	@echo "seeds: fill in seeds/miniapp_builders.csv and seeds/brand_accounts.csv (see seeds/README.md)"

preflight: ## Check credentials, seed files and layout before a long run. Spends nothing.
	@$(PY) -m scripts.preflight $(ARGS)

preflight-full: ## preflight, plus one live call to Neo4j and Neynar
	@$(PY) -m scripts.preflight --check-connections $(ARGS)

test: ## Run the unit tests
	@$(PY) -m pytest -q $(ARGS); status=$$?; \
		if [ $$status -eq 5 ]; then echo "no tests collected"; exit 0; fi; \
		exit $$status

plan: ## Print the stage plan and which ingestion module each pipeline resolves to
	$(RUN_ALL) --list

smoke: ## Cheap check: plan every pipeline with --dry-run --limit LIMIT. Spends nothing.
	$(RUN_ALL) --backfill --dry-run --limit $(LIMIT) --skip-ingest --continue-on-error \
		--only $(SMOKE_PIPELINES) $(ARGS)

backfill: ## Full historical load from BACKFILL_START, pipelines + ingestion. Hours; spends Dune credits.
	$(RUN_ALL) --backfill $(ARGS)

incremental: ## Watermark-driven update, pipelines + ingestion. The scheduled run.
	$(RUN_ALL) $(ARGS)

constraints: ## Create the Neo4j constraints and indexes (idempotent)
	$(PY) -m ingestion.constraints $(ARGS)

clean: ## Remove caches (bytecode, pytest, the Dune result cache). Keeps data/ and state/.
	find . -path ./.venv -prune -o -name '__pycache__' -type d -print -exec rm -rf {} +
	rm -rf .pytest_cache data/.dune_cache

clean-runs: ## DESTRUCTIVE: delete every CSV run and every watermark. Needs CONFIRM=yes
	@test "$(CONFIRM)" = "yes" || { \
		echo "refusing: this deletes data/<type>/ and state/*.json."; \
		echo "re-run as: make clean-runs CONFIRM=yes"; exit 1; }
	rm -rf data/* state/*.json
	@echo "runs and watermarks cleared; the next run must be a --backfill"
