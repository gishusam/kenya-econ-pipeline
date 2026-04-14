.PHONY: up down migrate ingest transform stream dashboard

up:
	docker-compose up postgres zookeeper kafka -d
	sleep 10

down:
	docker-compose down

migrate:
	python -m db.migrate

ingest:
	python -m ingestion.worldbank
	python -m ingestion.fx_rates
	python -m db.loaders.worldbank_loader
	python -m db.loaders.fx_loader

transform:
	cd kenya_econ_dbt && dbt run && dbt test

stream:
	python -m streaming.topic_setup

dashboard:
	streamlit run dashboard/app.py

setup: up migrate ingest transform
	@echo "Pipeline ready. Run 'make dashboard' to view."
