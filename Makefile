.PHONY: setup lint test airflow-up airflow-logs airflow-down ddl

setup:
	pipenv install --skip-lock -r requirements.txt

lint:
	pipenv run ruff check src

test:
	pipenv run pytest -q

airflow-up:
	docker compose -f docker-compose.airflow.yaml up -d

airflow-logs:
	docker compose -f docker-compose.airflow.yaml logs -f

airflow-down:
	docker compose -f docker-compose.airflow.yaml down -v

ddl:
	pipenv run python -m src.pipeline.jobs.load_to_snowflake --apply-ddl
