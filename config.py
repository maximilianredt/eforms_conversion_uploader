import os
import sys
import logging

logger = logging.getLogger(__name__)

# --- BigQuery ---
BQ_PROJECT = os.environ.get('BQ_PROJECT', 'datawarehouse-412318')
BQ_DATASET = os.environ.get('BQ_DATASET', 'raw_ad_conversions')
BQ_LOG_TABLE = 'ad_conversion_log'

# dbt dataset names (derived from dbt_project.yml: dataset=attribution_dev, schema=staging/marts)
DBT_STAGING_DATASET = 'attribution_dev_staging'
DBT_MARTS_DATASET = 'attribution_dev_marts'

# --- Google Ads ---
GOOGLE_ADS_DEVELOPER_TOKEN = os.environ.get('GOOGLE_ADS_DEVELOPER_TOKEN')
GOOGLE_ADS_SA_KEY_PATH = os.environ.get('GOOGLE_ADS_SA_KEY_PATH', 'google_ads_sa_key.json')
GOOGLE_ADS_SA_EMAIL = os.environ.get('GOOGLE_ADS_SA_EMAIL')
GOOGLE_ADS_CUSTOMER_ID = os.environ.get('GOOGLE_ADS_CUSTOMER_ID', '5057327942')
GOOGLE_ADS_LOGIN_CUSTOMER_ID = os.environ.get('GOOGLE_ADS_LOGIN_CUSTOMER_ID', '2064270947')

# Google Ads conversion action names
GADS_TRIAL_START_ACTION = os.environ.get('GADS_TRIAL_START_ACTION', 'Trial Start DWH')
GADS_MONTHLY_SUB_ACTION = os.environ.get('GADS_MONTHLY_SUB_ACTION', 'Monthly Subscription DWH')
GADS_YEARLY_SUB_ACTION = os.environ.get('GADS_YEARLY_SUB_ACTION', 'Yearly Subscription DWH')
GADS_DOC_PURCHASE_ACTION = os.environ.get('GADS_DOC_PURCHASE_ACTION', 'Document Purchase DWH')
GADS_CHAT_PURCHASE_ACTION = os.environ.get('GADS_CHAT_PURCHASE_ACTION', 'Chat Purchase DWH')

# --- Microsoft Ads ---
# Same credentials as ../bing_ads_importer/
MS_DEV_TOKEN = os.environ.get('MS_DEV_TOKEN')
MS_CLIENT_ID = os.environ.get('MS_CLIENT_ID')
MS_CLIENT_SECRET = os.environ.get('MS_CLIENT_SECRET')
MS_REFRESH_TOKEN = os.environ.get('MS_REFRESH_TOKEN')
MS_ACCOUNT_ID = os.environ.get('MS_ACCOUNT_ID', '180267661')
MS_CUSTOMER_ID = os.environ.get('MS_CUSTOMER_ID', '251167100')

# Microsoft Ads conversion goal names
MSADS_TRIAL_START_GOAL = os.environ.get('MSADS_TRIAL_START_GOAL', 'Trial Start DWH')
MSADS_MONTHLY_SUB_GOAL = os.environ.get('MSADS_MONTHLY_SUB_GOAL', 'Monthly Subscription DWH')
MSADS_YEARLY_SUB_GOAL = os.environ.get('MSADS_YEARLY_SUB_GOAL', 'Yearly Subscription DWH')
MSADS_DOC_PURCHASE_GOAL = os.environ.get('MSADS_DOC_PURCHASE_GOAL', 'Document Purchase DWH')
MSADS_CHAT_PURCHASE_GOAL = os.environ.get('MSADS_CHAT_PURCHASE_GOAL', 'Chat Purchase DWH')

# --- Options ---
SEND_RENEWAL_PAYMENTS = os.environ.get('SEND_RENEWAL_PAYMENTS', 'false').lower() == 'true'
LOOKBACK_DAYS = int(os.environ.get('LOOKBACK_DAYS', '30'))
DRY_RUN = os.environ.get('DRY_RUN', 'false').lower() == 'true'
CURRENCY_CODE = os.environ.get('CURRENCY_CODE', 'USD')
MAX_RETRIES = int(os.environ.get('MAX_RETRIES', '3'))

# --- Mappings ---
# Event type -> Google Ads conversion action name
GADS_ACTION_MAP = {
    'trial_start': GADS_TRIAL_START_ACTION,
    'monthly_subscription': GADS_MONTHLY_SUB_ACTION,
    'yearly_subscription': GADS_YEARLY_SUB_ACTION,
    'document_purchase': GADS_DOC_PURCHASE_ACTION,
    'chat_purchase': GADS_CHAT_PURCHASE_ACTION,
}

# Event type -> Microsoft Ads conversion goal name
MSADS_GOAL_MAP = {
    'trial_start': MSADS_TRIAL_START_GOAL,
    'monthly_subscription': MSADS_MONTHLY_SUB_GOAL,
    'yearly_subscription': MSADS_YEARLY_SUB_GOAL,
    'document_purchase': MSADS_DOC_PURCHASE_GOAL,
    'chat_purchase': MSADS_CHAT_PURCHASE_GOAL,
}


def validate_env_vars():
    """Validate that all required environment variables are set."""
    required_vars = {
        'BQ_PROJECT': BQ_PROJECT,
        'GOOGLE_ADS_DEVELOPER_TOKEN': GOOGLE_ADS_DEVELOPER_TOKEN,
        'GOOGLE_ADS_SA_EMAIL': GOOGLE_ADS_SA_EMAIL,
        'GOOGLE_ADS_CUSTOMER_ID': GOOGLE_ADS_CUSTOMER_ID,
        'GOOGLE_ADS_LOGIN_CUSTOMER_ID': GOOGLE_ADS_LOGIN_CUSTOMER_ID,
        'MS_DEV_TOKEN': MS_DEV_TOKEN,
        'MS_CLIENT_ID': MS_CLIENT_ID,
        'MS_CLIENT_SECRET': MS_CLIENT_SECRET,
        'MS_REFRESH_TOKEN': MS_REFRESH_TOKEN,
        'MS_ACCOUNT_ID': MS_ACCOUNT_ID,
    }

    missing_vars = [var for var, value in required_vars.items() if not value]

    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        sys.exit(1)

    # Validate service account key file exists
    if not os.path.exists(GOOGLE_ADS_SA_KEY_PATH):
        logger.error(f"Google Ads service account key file not found: {GOOGLE_ADS_SA_KEY_PATH}")
        logger.error("Copy your service account key: cp ../dbt/bigquery_service_key.json ./google_ads_sa_key.json")
        sys.exit(1)

    logger.info("All required environment variables are set")
    logger.info(f"Config: SEND_RENEWAL_PAYMENTS={SEND_RENEWAL_PAYMENTS}, "
                f"LOOKBACK_DAYS={LOOKBACK_DAYS}, DRY_RUN={DRY_RUN}")
