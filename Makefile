.PHONY: test compile dbt-parse docker-build bootstrap-gcp

test:
	pytest -q

compile:
	python -m compileall -q pipeline dashboard

dbt-parse:
	GCP_PROJECT_ID=$${GCP_PROJECT_ID:?Set GCP_PROJECT_ID} \
	BQ_LOCATION=$${BQ_LOCATION:-africa-south1} \
	dbt parse --project-dir kenya_econ_dbt --profiles-dir kenya_econ_dbt --target prod

docker-build:
	docker build -t kenya-econ-refresh:local .

bootstrap-gcp:
	bash infra/gcp/bootstrap.sh
