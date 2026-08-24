#!/usr/bin/env bash
set -euo pipefail

: "${PROJECT_ID:?Set PROJECT_ID to the target Google Cloud project ID}"
GITHUB_REPO="${GITHUB_REPO:-gishusam/kenya-econ-pipeline}"
REGION="${REGION:-africa-south1}"
SCHEDULER_REGION="${SCHEDULER_REGION:-europe-west1}"
BQ_LOCATION="${BQ_LOCATION:-africa-south1}"
AR_REPOSITORY="${AR_REPOSITORY:-kenya-econ}"
WIF_POOL="${WIF_POOL:-github}"
WIF_PROVIDER="${WIF_PROVIDER:-kenya-econ}"

PIPELINE_SA="kenya-econ-pipeline@${PROJECT_ID}.iam.gserviceaccount.com"
SCHEDULER_SA="kenya-econ-scheduler@${PROJECT_ID}.iam.gserviceaccount.com"
DEPLOY_SA="kenya-econ-deploy@${PROJECT_ID}.iam.gserviceaccount.com"
DASHBOARD_SA="kenya-econ-dashboard@${PROJECT_ID}.iam.gserviceaccount.com"

gcloud config set project "${PROJECT_ID}" >/dev/null
PROJECT_NUMBER="$(gcloud projects describe "${PROJECT_ID}" --format='value(projectNumber)')"

echo "Enabling APIs..."
gcloud services enable \
  run.googleapis.com \
  artifactregistry.googleapis.com \
  bigquery.googleapis.com \
  cloudscheduler.googleapis.com \
  iamcredentials.googleapis.com \
  sts.googleapis.com

if ! gcloud artifacts repositories describe "${AR_REPOSITORY}" --location="${REGION}" >/dev/null 2>&1; then
  gcloud artifacts repositories create "${AR_REPOSITORY}" \
    --repository-format=docker \
    --location="${REGION}" \
    --description="Kenya economic pipeline images"
fi

create_sa() {
  local id="$1"
  local display="$2"
  if ! gcloud iam service-accounts describe "${id}@${PROJECT_ID}.iam.gserviceaccount.com" >/dev/null 2>&1; then
    gcloud iam service-accounts create "${id}" --display-name="${display}"
  fi
}

create_sa kenya-econ-pipeline "Kenya Econ Cloud Run Job"
create_sa kenya-econ-scheduler "Kenya Econ Scheduler"
create_sa kenya-econ-deploy "Kenya Econ GitHub Deploy"
create_sa kenya-econ-dashboard "Kenya Econ Streamlit Read Only"

# Pipeline runtime: BigQuery jobs + data mutation. No broader compute/admin role.
for role in roles/bigquery.jobUser roles/bigquery.dataEditor; do
  gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${PIPELINE_SA}" --role="${role}" >/dev/null
 done

# Dashboard can query and read data but cannot mutate it.
for role in roles/bigquery.jobUser roles/bigquery.dataViewer; do
  gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${DASHBOARD_SA}" --role="${role}" >/dev/null
 done

# GitHub deployment identity.
for role in roles/artifactregistry.writer roles/run.admin roles/cloudscheduler.admin; do
  gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${DEPLOY_SA}" --role="${role}" >/dev/null
 done

for target_sa in "${PIPELINE_SA}" "${SCHEDULER_SA}"; do
  gcloud iam service-accounts add-iam-policy-binding "${target_sa}" \
    --member="serviceAccount:${DEPLOY_SA}" \
    --role="roles/iam.serviceAccountUser" >/dev/null
 done

if ! gcloud iam workload-identity-pools describe "${WIF_POOL}" --location=global >/dev/null 2>&1; then
  gcloud iam workload-identity-pools create "${WIF_POOL}" \
    --location=global \
    --display-name="GitHub Actions"
fi

if ! gcloud iam workload-identity-pools providers describe "${WIF_PROVIDER}" \
  --workload-identity-pool="${WIF_POOL}" --location=global >/dev/null 2>&1; then
  gcloud iam workload-identity-pools providers create-oidc "${WIF_PROVIDER}" \
    --workload-identity-pool="${WIF_POOL}" \
    --location=global \
    --issuer-uri="https://token.actions.githubusercontent.com" \
    --attribute-mapping="google.subject=assertion.sub,attribute.repository=assertion.repository,attribute.ref=assertion.ref" \
    --attribute-condition="assertion.repository=='${GITHUB_REPO}'"
fi

POOL_NAME="$(gcloud iam workload-identity-pools describe "${WIF_POOL}" --location=global --format='value(name)')"
PROVIDER_NAME="$(gcloud iam workload-identity-pools providers describe "${WIF_PROVIDER}" \
  --workload-identity-pool="${WIF_POOL}" --location=global --format='value(name)')"

gcloud iam service-accounts add-iam-policy-binding "${DEPLOY_SA}" \
  --role="roles/iam.workloadIdentityUser" \
  --member="principalSet://iam.googleapis.com/${POOL_NAME}/attribute.repository/${GITHUB_REPO}" >/dev/null

TMP_SQL="$(mktemp)"
sed -e "s/__PROJECT_ID__/${PROJECT_ID}/g" -e "s/__BQ_LOCATION__/${BQ_LOCATION}/g" \
  infra/bigquery/bootstrap.sql > "${TMP_SQL}"
bq query --project_id="${PROJECT_ID}" --location="${BQ_LOCATION}" --use_legacy_sql=false < "${TMP_SQL}"
rm -f "${TMP_SQL}"

cat <<OUT

Bootstrap complete.

Set these GitHub repository variables:
  GCP_PROJECT_ID=${PROJECT_ID}
  GCP_REGION=${REGION}
  GCP_SCHEDULER_REGION=${SCHEDULER_REGION}
  BQ_LOCATION=${BQ_LOCATION}
  GCP_WIF_PROVIDER=${PROVIDER_NAME}
  GCP_DEPLOY_SERVICE_ACCOUNT=${DEPLOY_SA}

Cloud Run runtime service account:
  ${PIPELINE_SA}

Scheduler identity:
  ${SCHEDULER_SA}

Streamlit read-only service account:
  ${DASHBOARD_SA}

The Streamlit credential is intentionally NOT created here. If Community Cloud requires a key,
create one only for the read-only dashboard service account and store it in Streamlit Secrets.
OUT
