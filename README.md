# Conversion Uploader

Syncs offline conversion data from BigQuery to Google Ads and Microsoft Ads. Runs as a Google Cloud Run Job triggered daily by Cloud Scheduler.

## Events Tracked

| Event | Value | Conversion Action |
|-------|-------|-------------------|
| Trial Start | $0 | Trial Start DWH |
| Monthly Subscription | Actual payment | Monthly Subscription DWH |
| Yearly Subscription | Actual payment | Yearly Subscription DWH |
| Document Purchase | Actual payment | Document Purchase DWH |
| Chat Purchase | Actual payment | Chat Purchase DWH |
| Refunds | Retraction on original action | (original action) |

## How It Works

1. Queries BigQuery for new events with click IDs (GCLID/MSCLKID)
2. Checks `ad_conversion_log` table to skip already-sent events (idempotent)
3. Uploads conversions to Google Ads (via GCLID) and Microsoft Ads (via MSCLKID)
4. Logs results to `ad_conversion_log` for tracking and deduplication
5. Sends refund retractions for previously-uploaded conversions

## Prerequisites

### 1. Google Ads: Create Conversion Actions

In Google Ads UI:

1. Go to **Goals > Conversions > Summary**
2. Click **+ New conversion action**
3. Select **Import > Other data sources or CRMs > Track conversions from clicks**
4. Create these conversion actions (names must match exactly):
   - `Trial Start DWH` (Category: Other, Value: Use the value from the conversion)
   - `Monthly Subscription DWH` (Category: Purchase, Value: Use the value from the conversion)
   - `Yearly Subscription DWH` (Category: Purchase, Value: Use the value from the conversion)
   - `Document Purchase DWH` (Category: Purchase, Value: Use the value from the conversion)
   - `Chat Purchase DWH` (Category: Purchase, Value: Use the value from the conversion)
5. For each action, ensure **Include in "Conversions"** is set appropriately

### 2. Google Ads: Service Account Access

This project uses a **service account** for Google Ads API authentication (same key as the dbt project).

1. Get a **Developer Token** from your [Google Ads API Center](https://ads.google.com/aw/apicenter)
2. Copy the service account key: `cp ../dbt/bigquery_service_key.json ./google_ads_sa_key.json`
3. Add the service account email as a user in Google Ads:
   - Go to **Admin > Access and security**
   - Click **+** to add a new user
   - Paste the service account email (find it in `google_ads_sa_key.json` under `client_email`)
   - Grant **Standard** access level
4. Set `GOOGLE_ADS_SA_EMAIL` in `.env` to the email of the Google Ads user the service account should impersonate (typically the admin email that has access to the account)

### 3. Microsoft Ads: Create Offline Conversion Goals

In Microsoft Ads UI:

1. Go to **Tools > Conversion tracking > Conversion goals**
2. Click **Create conversion goal**
3. Select **Offline conversions** as the type
4. Create these goals (names must match exactly):
   - `Trial Start DWH`
   - `Monthly Subscription DWH`
   - `Yearly Subscription DWH`
   - `Document Purchase DWH`
   - `Chat Purchase DWH`
5. Wait **at least 2 hours** after creating goals before uploading conversions

### 4. Microsoft Ads: OAuth Credentials

This project uses the **same OAuth credentials** as `../bing_ads_importer/`. Copy the values for `MS_DEV_TOKEN`, `MS_CLIENT_ID`, `MS_CLIENT_SECRET`, and `MS_REFRESH_TOKEN` from the bing_ads_importer `.env` file.

## Setup

```bash
# 1. Copy and fill in credentials
cp .env.example .env
# Edit .env with your actual credentials

# 2. Copy the service account key
cp ../dbt/bigquery_service_key.json ./google_ads_sa_key.json

# 3. Test locally with dry run
set -a && source .env && set +a && DRY_RUN=true python main.py

# 4. Test locally (live)
set -a && source .env && set +a && python main.py

# 5. Deploy to Cloud Run
./deploy.sh
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `SEND_RENEWAL_PAYMENTS` | `false` | Upload monthly/yearly renewal payments (not just initial) |
| `LOOKBACK_DAYS` | `30` | How far back to scan for unsent events |
| `DRY_RUN` | `false` | Query events but don't call ad platform APIs |
| `MAX_RETRIES` | `3` | Max retry attempts for failed events |
| `CURRENCY_CODE` | `USD` | Currency for all conversion values |

## Monitoring

```bash
# Check recent logs
gcloud logging read "resource.labels.job_name=conversion-uploader" --limit 50

# Check ad_conversion_log in BigQuery
SELECT status, platform, event_type, COUNT(*)
FROM `datawarehouse-412318.raw_ad_conversions.ad_conversion_log`
WHERE sent_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3

# Check for failures
SELECT event_id, event_type, platform, error_message, sent_at
FROM `datawarehouse-412318.raw_ad_conversions.ad_conversion_log`
WHERE status = 'failed'
ORDER BY sent_at DESC
LIMIT 20
```

## Architecture

```
Cloud Scheduler (daily 9 AM ET)
  -> Cloud Run Job: conversion-uploader
       -> BigQuery: query unsent events
       -> Google Ads API: upload conversions (via GCLID, service account auth)
       -> Microsoft Ads API: upload conversions (via MSCLKID, OAuth refresh token)
       -> BigQuery: log results to ad_conversion_log
```
