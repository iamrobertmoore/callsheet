#!/usr/bin/env bash
set -eo pipefail

echo "=== Deploying Callsheet to Google Cloud Run ==="

# Load local environment variables for deployment configuration
if [ -f .env ]; then
    set -a
    source .env
    set +a
fi

PROJECT_ID="${GOOGLE_CLOUD_PROJECT:-agent-attest-2026}"
REGION="${GOOGLE_CLOUD_REGION:-us-central1}"
SERVICE_NAME="callsheet"
SECRET_NAME="callsheet-grafana-sa-token"

echo "Target Project: $PROJECT_ID"
echo "Target Region: $REGION"
echo "Service Name: $SERVICE_NAME"

# 1. Provision Secret Manager secret for Grafana Service Account Token if needed
echo "Verifying Secret Manager secret: $SECRET_NAME..."
if ! gcloud secrets describe "$SECRET_NAME" --project "$PROJECT_ID" >/dev/null 2>&1; then
    echo "Creating secret $SECRET_NAME in Secret Manager..."
    gcloud secrets create "$SECRET_NAME" --replication-policy="automatic" --project "$PROJECT_ID" --quiet
    echo -n "$GRAFANA_SERVICE_ACCOUNT_TOKEN" | gcloud secrets versions add "$SECRET_NAME" --data-file=- --project "$PROJECT_ID" --quiet
else
    echo "Secret $SECRET_NAME already exists. Updating version..."
    echo -n "$GRAFANA_SERVICE_ACCOUNT_TOKEN" | gcloud secrets versions add "$SECRET_NAME" --data-file=- --project "$PROJECT_ID" --quiet || true
fi

# 2. Build and submit container image via Cloud Build
echo "Submitting build to Cloud Build..."
gcloud builds submit --project "$PROJECT_ID" --tag "gcr.io/${PROJECT_ID}/${SERVICE_NAME}:latest" --quiet .

# 3. Deploy to Cloud Run with always-on background CPU and Secret Manager injection
echo "Deploying to Cloud Run with --no-cpu-throttling, --min-instances=1, and Secret Manager..."
gcloud run deploy "$SERVICE_NAME" \
    --project "$PROJECT_ID" \
    --region "$REGION" \
    --image "gcr.io/${PROJECT_ID}/${SERVICE_NAME}:latest" \
    --platform managed \
    --allow-unauthenticated \
    --min-instances 1 \
    --max-instances 2 \
    --no-cpu-throttling \
    --memory 512Mi \
    --cpu 1 \
    --quiet \
    --set-env-vars "GOOGLE_CLOUD_PROJECT=${PROJECT_ID},GOOGLE_CLOUD_REGION=${REGION},VERTEX_AI_LOCATION=global,GRAFANA_URL=${GRAFANA_URL},GRAFANA_MCP_SERVER_URL=http://127.0.0.1:8000/mcp,OTEL_EXPORTER_OTLP_ENDPOINT=${OTEL_EXPORTER_OTLP_ENDPOINT},OTEL_EXPORTER_OTLP_HEADERS=${OTEL_EXPORTER_OTLP_HEADERS}" \
    --set-secrets "GRAFANA_SERVICE_ACCOUNT_TOKEN=${SECRET_NAME}:latest"

SERVICE_URL=$(gcloud run services describe "$SERVICE_NAME" --project "$PROJECT_ID" --region "$REGION" --format 'value(status.url)')
echo "=== Deployment Successful ==="
echo "Live Service URL: $SERVICE_URL"
