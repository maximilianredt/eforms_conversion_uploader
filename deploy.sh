#!/bin/bash
set -e

PROJECT_ID="datawarehouse-412318"
IMAGE_NAME="conversion-uploader"
REGION="us-central1"
SCHEDULE_NAME="conversion-uploader-daily"

echo "==================================================="
echo "Conversion Uploader - Cloud Run Job Deployment"
echo "==================================================="

# Load environment variables from .env
if [ ! -f .env ]; then
    echo "Error: .env file not found! Copy .env.example to .env and fill in values."
    exit 1
fi

# Check that service account key file exists
if [ ! -f google_ads_sa_key.json ]; then
    echo "Error: google_ads_sa_key.json not found!"
    echo "Copy it from dbt: cp ../dbt/bigquery_service_key.json ./google_ads_sa_key.json"
    exit 1
fi

set -a && source .env && set +a

# Generate env vars YAML file (handles values with spaces correctly)
ENV_VARS_FILE=$(mktemp /tmp/env-vars-XXXXXX.yaml)
trap "rm -f ${ENV_VARS_FILE}" EXIT

cat > "${ENV_VARS_FILE}" <<EOF
BQ_PROJECT: "${BQ_PROJECT}"
BQ_DATASET: "${BQ_DATASET}"
GOOGLE_ADS_DEVELOPER_TOKEN: "${GOOGLE_ADS_DEVELOPER_TOKEN}"
GOOGLE_ADS_SA_KEY_PATH: "${GOOGLE_ADS_SA_KEY_PATH}"
GOOGLE_ADS_SA_EMAIL: "${GOOGLE_ADS_SA_EMAIL}"
GOOGLE_ADS_CUSTOMER_ID: "${GOOGLE_ADS_CUSTOMER_ID}"
GOOGLE_ADS_LOGIN_CUSTOMER_ID: "${GOOGLE_ADS_LOGIN_CUSTOMER_ID}"
GADS_TRIAL_START_ACTION: "${GADS_TRIAL_START_ACTION}"
GADS_MONTHLY_SUB_ACTION: "${GADS_MONTHLY_SUB_ACTION}"
GADS_YEARLY_SUB_ACTION: "${GADS_YEARLY_SUB_ACTION}"
GADS_DOC_PURCHASE_ACTION: "${GADS_DOC_PURCHASE_ACTION}"
GADS_CHAT_PURCHASE_ACTION: "${GADS_CHAT_PURCHASE_ACTION}"
MS_DEV_TOKEN: "${MS_DEV_TOKEN}"
MS_CLIENT_ID: "${MS_CLIENT_ID}"
MS_CLIENT_SECRET: "${MS_CLIENT_SECRET}"
MS_REFRESH_TOKEN: "${MS_REFRESH_TOKEN}"
MS_ACCOUNT_ID: "${MS_ACCOUNT_ID}"
MS_CUSTOMER_ID: "${MS_CUSTOMER_ID}"
MSADS_TRIAL_START_GOAL: "${MSADS_TRIAL_START_GOAL}"
MSADS_MONTHLY_SUB_GOAL: "${MSADS_MONTHLY_SUB_GOAL}"
MSADS_YEARLY_SUB_GOAL: "${MSADS_YEARLY_SUB_GOAL}"
MSADS_DOC_PURCHASE_GOAL: "${MSADS_DOC_PURCHASE_GOAL}"
MSADS_CHAT_PURCHASE_GOAL: "${MSADS_CHAT_PURCHASE_GOAL}"
SEND_RENEWAL_PAYMENTS: "${SEND_RENEWAL_PAYMENTS}"
LOOKBACK_DAYS: "${LOOKBACK_DAYS}"
DRY_RUN: "false"
CURRENCY_CODE: "${CURRENCY_CODE}"
MAX_RETRIES: "${MAX_RETRIES}"
ENABLE_ENHANCED_CONVERSIONS: "${ENABLE_ENHANCED_CONVERSIONS}"
EOF

# Build container image
echo ""
echo "Building container image..."
gcloud builds submit --tag gcr.io/${PROJECT_ID}/${IMAGE_NAME}

# Deploy Cloud Run Job
echo ""
echo "Deploying Cloud Run Job..."
gcloud run jobs deploy ${IMAGE_NAME} \
  --image gcr.io/${PROJECT_ID}/${IMAGE_NAME} \
  --region ${REGION} \
  --env-vars-file "${ENV_VARS_FILE}" \
  --max-retries 2 \
  --task-timeout 15m \
  --memory 512Mi

echo ""
echo "==================================================="
echo "Deployment complete!"
echo "==================================================="
echo ""
echo "To test immediately:"
echo "  gcloud run jobs execute ${IMAGE_NAME} --region ${REGION} --wait"
echo ""
echo "To create/update the daily schedule (8:30 AM CET):"
echo "  gcloud scheduler jobs create http ${SCHEDULE_NAME} \\"
echo "    --location ${REGION} \\"
echo "    --schedule '30 7 * * *' \\"
echo "    --time-zone 'Europe/Berlin' \\"
echo "    --uri 'https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${IMAGE_NAME}:run' \\"
echo "    --http-method POST \\"
echo "    --oauth-service-account-email 405662004024-compute@developer.gserviceaccount.com"
echo ""
echo "To check logs:"
echo "  gcloud logging read 'resource.labels.job_name=${IMAGE_NAME}' --limit 50"
