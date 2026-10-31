export GCP_PROJECT_ID ?= amtab-elt
export GOOGLE_APPLICATION_CREDENTIALS ?= keys/sa-key.json
export BQ_DATASET ?= amtab_transit

DBT_FLAGS = --project-dir dbt --profiles-dir dbt

STREAMING = ./streaming
REDPANDA_VERSION = v26.2.1
REDPANDA_CONSOLE_VERSION = v3.10.0
KAFKA_TOPICS = vehicle-positions trip-updates \
	       vehicle-positions.dlq trip-updates.dlq

.PHONY: setup airflow-up airflow-down dbt dbt-docs dashboard \
	kafka-up kafka-down kafka-topics

setup:
	uv sync

airflow-up:
	cd airflow && docker compose up -d

airflow-down:
	cd airflow && docker compose down

dbt-parse:
	uv run dbt deps $(DBT_FLAGS)
	uv run dbt parse $(DBT_FLAGS)

dbt:
	uv run dbt deps $(DBT_FLAGS)
	uv run dbt seed $(DBT_FLAGS)
	uv run dbt build $(DBT_FLAGS)

dbt-docs:
	uv run dbt docs generate $(DBT_FLAGS)
	uv run dbt docs serve $(DBT_FLAGS) --port 8081

dashboard:
	uv run streamlit run streamlit/app.py

kafka-up:
	@echo "Starting redpanda $(REDPANDA_VERSION)"
	docker compose --project-directory $(STREAMING) up -d

kafka-topics:
	@echo "Creating topics"
	docker exec redpanda-0 rpk topic create $(KAFKA_TOPICS)

kafka-down:
	@echo "Stopping redpanda"
	docker compose --project-directory $(STREAMING) down -v
