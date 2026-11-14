export GCP_PROJECT_ID ?= amtab-elt
export GOOGLE_APPLICATION_CREDENTIALS ?= keys/sa-key.json
export BQ_DATASET ?= amtab_transit

DBT_FLAGS = --project-dir dbt --profiles-dir dbt

STREAMING = ./streaming

.PHONY: setup airflow-up airflow-down dbt dbt-docs dashboard \
	kafka-up kafka-topics down up

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
	docker compose --project-directory $(STREAMING) up --wait console

kafka-topics:
	docker compose --project-directory $(STREAMING) up topic-init

down:
	docker compose --project-directory $(STREAMING) down

up:
	docker compose --project-directory $(STREAMING) up --build --wait
