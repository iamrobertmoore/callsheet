#!/usr/bin/env bash
set -eo pipefail

echo "=== Deploying Callsheet to Google Cloud Run ==="

PROJECT_ID="${GOOGLE_CLOUD_PROJECT:-agent-attest-2026}"
REGION="${GOOGLE_CLOUD_REGION:-us-central1}"
SERVICE_NAME="callsheet"

echo "Target Project: $PROJECT_ID"
echo "Target Region: $REGION"
echo "Service Name: $SERVICE_NAME"

# Build and submit container image via Cloud Build
echo "Submitting build to Cloud Build..."
gcloud builds submit --project "$PROJECT_ID" --tag "gcr.io/${PROJECT_ID}/${SERVICE_NAME}:latest" .

# Deploy to Cloud Run with always-on CPU allocation and minimum instances
echo "Deploying to Cloud Run with --no-cpu-throttling and --min-instances=1..."
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
    --set-env-vars "GOOGLE_CLOUD_PROJECT=${PROJECT_ID},GOOGLE_CLOUD_REGION=${REGION}"

SERVICE_URL=$(gcloud run services describe "$SERVICE_NAME" --project "$PROJECT_ID" --region "$REGION" --format 'value(status.url)')
echo "=== Deployment Successful ==="
echo "Live Service URL: $SERVICE_URL"
