# Conversion Uploader

Syncs offline conversion data from BigQuery to Google Ads and Microsoft Ads. Runs as a Google Cloud Run Job triggered daily by Cloud Scheduler.

## Events Tracked

| Event | Value | Conversion Action |
|-------|-------|-------------------|
| Trial Start | $0 | Trial Start |
| Monthly Subscription | Actual payment | Monthly Subscription |
| Yearly Subscription | Actual payment | Yearly Subscription |
| Document Purchase | Actual payment | Document Purchase |
| Chat Purchase | Actual payment | Chat Purchase |
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
4. Create these conversion actions (names must match env vars exactly):
   - `Trial Start` (Category: Other, Value: Use the value from the conversion)
   - `Monthly Subscription` (Category: Purchase, Value: Use the value from the conversion)
   - `Yearly Subscription` (Category: Purchase, Value: Use the value from the conversion)
   - `Document Purchase` (Category: Purchase, Value: Use the value from the conversion)
   - `Chat Purchase` (Category: Purchase, Value: Use the value from the conversion)
5. For each action, ensure **Include in "Conversions"** is set appropriately

### 2. Google Ads: OAuth Credentials

1. Go to [Google Cloud Console > APIs & Services > Credentials](https://console.cloud.google.com/apis/credentials)
2. Create an OAuth 2.0 Client ID (Desktop application type)
3. Note the **Client ID** and **Client Secret**
4. Get a **Developer Token** from your [Google Ads API Center](https://ads.google.com/aw/apicenter)
5. Generate a **Refresh Token** using the [OAuth Playground](https://developers.google.com/oauthplayground/) or the `google-ads` library's authentication helper

### 3. Microsoft Ads: Create Offline Conversion Goals

In Microsoft Ads UI:

1. Go to **Tools > Conversion tracking > Conversion goals**
2. Click **Create conversion goal**
3. Select **Offline conversions** as the type
4. Create these goals (names must match env vars exactly):
   - `Trial Start`
   - `Monthly Subscription`
   - `Yearly Subscription`
   - `Document Purchase`
   - `Chat Purchase`
5. Wait **at least 2 hours** after creating goals before uploading conversions

### 4. Microsoft Ads: OAuth Credentials

1. Register an app in [Azure Portal > App registrations](https://portal.azure.com/#blade/Microsoft_AAD_RegisteredApps/ApplicationsListBlade)
2. Note the **Client ID** and create a **Client Secret**
3. Get a **Developer Token** from [Microsoft Advertising Developer Portal](https://developers.ads.microsoft.com/)
4. Generate a **Refresh Token** via the OAuth flow
5. Find your **Customer ID** and **Account ID** in Microsoft Ads

## Setup

```bash
# 1. Copy and fill in credentials
cp .env.example .env
# Edit .env with your actual credentials

# 2. Test locally (requires gcloud auth for BigQuery)
export $(cat .env | xargs)
python main.py

# 3. Deploy to Cloud Run
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
       -> Google Ads API: upload conversions (via GCLID)
       -> Microsoft Ads API: upload conversions (via MSCLKID)
       -> BigQuery: log results to ad_conversion_log
```
