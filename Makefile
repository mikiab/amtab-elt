export GCP_PROJECT_ID ?= amtab-elt
export GOOGLE_APPLICATION_CREDENTIALS ?= keys/sa-key.json
export BQ_DATASET ?= amtab_transit

DBT_FLAGS = --project-dir dbt --profiles-dir dbt

.PHONY: setup airflow-up airflow-down dbt dbt-docs dashboard

setup:
	uv sync

airflow-up:
	cd airflow && docker compose up -d

airflow-down:
	cd airflow && docker compose down

dbt:
	uv run dbt deps $(DBT_FLAGS)
	uv run dbt seed $(DBT_FLAGS)
	uv run dbt build $(DBT_FLAGS)

dbt-docs:
	uv run dbt docs generate $(DBT_FLAGS)
	uv run dbt docs serve $(DBT_FLAGS) --port 8081

dashboard:
	uv run streamlit run streamlit/app.py
