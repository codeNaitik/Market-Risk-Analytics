# Market & Macro Risk Analytics Platform - developer commands
# Run `make help` for the list.

PY      ?= python
export PYTHONPATH := $(CURDIR)/src
export LAKE_ROOT  ?= $(CURDIR)/lake
DBT      = cd dbt && dbt
DBT_ARGS = --profiles-dir .

.DEFAULT_GOAL := help
.PHONY: help install demo pipeline ingest sample silver seeds gold risk export test lint up down stream-produce stream-simulate stream-consume docs clean

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

install:  ## Install Python dependencies (use a virtualenv)
	$(PY) -m pip install -r requirements-dev.txt

# --- end-to-end -------------------------------------------------------------
demo: sample silver gold risk export  ## Full pipeline on SYNTHETIC data (no keys, no internet)
	@echo "\nDone. Gold tables for Power BI: $(LAKE_ROOT)/gold_export/*.parquet"

pipeline: ingest silver gold risk export  ## Full pipeline on REAL data (needs .env keys)

# --- layers -----------------------------------------------------------------
ingest:  ## Bronze: pull real data from SEC, FRED, Bank of Canada, Yahoo Finance
	$(PY) -m mra.ingestion.prices
	$(PY) -m mra.ingestion.sec_edgar
	$(PY) -m mra.ingestion.fred
	$(PY) -m mra.ingestion.boc_valet

sample:  ## Bronze: generate 3 years of synthetic data instead
	$(PY) -m mra.ingestion.sample_data --years 3

silver:  ## Silver: Spark bronze -> silver jobs
	$(PY) -m mra.processing.run_silver

seeds:  ## Regenerate dbt seed CSVs from config/settings.yaml
	$(PY) -m mra.export_seeds

gold: seeds  ## Gold: dbt seed + build (models and tests)
	$(DBT) build $(DBT_ARGS)

risk:  ## Gold: Python VaR backtest, Monte Carlo, efficient frontier
	$(PY) -m mra.analytics.run_risk

export:  ## Export gold tables to Parquet for Power BI Desktop
	$(PY) -m mra.export_gold

docs:  ## Generate and serve dbt docs (lineage graph) on :8082
	cd dbt && dbt docs generate $(DBT_ARGS) && dbt docs serve $(DBT_ARGS) --port 8082

# --- streaming --------------------------------------------------------------
up:  ## Start Kafka, Kafka UI (:8081) and Airflow (:8080)
	docker compose up -d --build

down:  ## Stop the local stack
	docker compose down

stream-produce:  ## Live Coinbase trades -> Kafka
	$(PY) -m mra.streaming.crypto_producer

stream-simulate:  ## Simulated trades -> Kafka (offline)
	$(PY) -m mra.streaming.crypto_producer --simulate

stream-consume:  ## Kafka -> Spark Structured Streaming -> bronze + 1-min bars
	$(PY) -m mra.streaming.stream_consumer

# --- quality ----------------------------------------------------------------
test:  ## Unit + integration tests
	$(PY) -m pytest -q

lint:  ## Ruff lint
	ruff check src tests airflow

clean:  ## Delete the local lake and build artefacts
	rm -rf lake dbt/target dbt/logs .pytest_cache spark-warehouse
