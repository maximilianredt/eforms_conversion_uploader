# Conversion Uploader - Developer Context

## What This Project Does

Cloud Run Job that syncs offline conversion data from BigQuery to Google Ads and Microsoft Ads APIs. Runs daily via Cloud Scheduler. Tracks trial starts, subscriptions, purchases, and refunds — attributing them back to ad clicks via GCLID/MSCLKID.

## Architecture

```
Cloud Scheduler (daily 9 AM ET)
  → Cloud Run Job (python:3.9-slim, 512MB, 15min timeout)
    → BigQuery: query unsent events from dbt models
    → Google Ads API: upload via GCLID (ConversionUploadService)
    → Microsoft Ads API: upload via MSCLKID (CampaignManagementService)
    → BigQuery: log results to ad_conversion_log
```

## File Structure

- `main.py` — Entry point. Orchestrates: validate config → ensure log table → process each event type → process refunds → summary.
- `config.py` — All env vars, validation, action/goal name mappings.
- `queries.py` — SQL query templates for each event type. All queries use the same click ID resolution pattern via `dim_users` + `dim_attribution`.
- `bq_client.py` — BigQuery client, table creation, query execution, result logging via `insert_rows_json`.
- `google_ads_client.py` — Google Ads client. Uses **service account auth** (JSON key file from `../dbt/bigquery_service_key.json`). Looks up conversion action resource names by name (cached). Handles partial failures per-event.
- `microsoft_ads_client.py` — Microsoft Ads client. Same OAuth credentials as `../bing_ads_importer/main.py`. Uses SOAP factory for OfflineConversion objects.
- `deploy.sh` — Builds container + deploys Cloud Run Job with env vars from `.env`.

## Key Design Decisions

### Click ID Resolution (Unified)
All event types use the same two-table join on `user_id`:
1. Primary: `dim_users.conversion_gclid` / `conversion_msclkid` (coalesces subscription + trial click IDs)
2. Fallback: `dim_attribution.first_touch_gclid` / `first_touch_msclkid` (full attribution model)

No event-level click ID resolution — simpler and leverages dbt attribution logic.

### Idempotency
`ad_conversion_log` table with composite key `(event_id, platform)`. Queries LEFT anti-join against it to find unsent events. Events with both GCLID and MSCLKID get separate rows (sent to both platforms).

### Refund Handling
Refunds are sent as **retractions on the original conversion action** (not a separate action). Matched to original via `user_id` in `ad_conversion_log`. Google Ads uses `order_id` (set to `event_id` on initial upload). Microsoft Ads matches by `msclkid` + `ConversionTime`.

### Chat vs Document Purchases
Both are `payment_source='order'` in `fct_payments`. Chat has `plan_code='10'`, everything else is a document purchase.

## BigQuery Tables

| Dataset | Table | Description |
|---------|-------|-------------|
| `attribution_dev_staging` | `stg_php_prod__trial_started` | Trial start events (source for trial uploads) |
| `attribution_dev_marts` | `fct_payments` | All revenue events (subscriptions, orders, refunds) |
| `attribution_dev_marts` | `dim_users` | User dimension with `conversion_gclid`/`conversion_msclkid` |
| `attribution_dev_marts` | `dim_attribution` | First-touch/last-touch attribution with `first_touch_gclid`/`first_touch_msclkid` |
| `raw_ad_conversions` | `ad_conversion_log` | Tracking table for sent/failed conversions (created by this script) |

## Event Types

| Event Type | Source Filter | Value |
|------------|--------------|-------|
| `trial_start` | `stg_php_prod__trial_started` | $0 |
| `monthly_subscription` | `fct_payments` where `payment_source='subscription'`, `billing_frequency!='annual'` | Actual amount |
| `yearly_subscription` | `fct_payments` where `payment_source='subscription'`, `billing_frequency='annual'` | Actual amount |
| `document_purchase` | `fct_payments` where `payment_source='order'`, `plan_code != '10'` | Actual amount |
| `chat_purchase` | `fct_payments` where `payment_source='order'`, `plan_code = '10'` | Actual amount |
| `refund` | `fct_payments` where `payment_type IN ('refund', 'order_refund')` | Retraction |

## Configuration

Key env vars (see `.env.example` for full list):
- `SEND_RENEWAL_PAYMENTS` (default `false`) — whether to upload renewal payments, not just initial subscriptions
- `DRY_RUN` (default `false`) — queries run but no API calls made
- `LOOKBACK_DAYS` (default `30`) — how far back to scan for unsent events
- `MAX_RETRIES` (default `3`) — failed events retried up to this many times

## Related Projects

- `../bing_ads_importer/` — Bing Ads campaign performance data importer (same Cloud Run pattern, same MS Ads auth)
- `../dbt/` — dbt project that builds all source tables (`fct_payments`, `dim_users`, `dim_attribution`, etc.)

## Common Tasks

### Test locally
```bash
set -a && source .env && set +a && python main.py
```

### Test with dry run
```bash
set -a && source .env && set +a && DRY_RUN=true python main.py
```

### Deploy
```bash
./deploy.sh
```

### Check what would be sent (BigQuery)
```sql
-- Unsent trial starts with click IDs
SELECT ts.event_id, ts.user_id, ts.trial_started_at,
       COALESCE(du.conversion_gclid, da.first_touch_gclid) AS gclid,
       COALESCE(du.conversion_msclkid, da.first_touch_msclkid) AS msclkid
FROM `datawarehouse-412318.attribution_dev_staging.stg_php_prod__trial_started` ts
LEFT JOIN `datawarehouse-412318.attribution_dev_marts.dim_users` du ON ts.user_id = du.user_id
LEFT JOIN `datawarehouse-412318.attribution_dev_marts.dim_attribution` da ON ts.user_id = da.user_id
WHERE ts.trial_started_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
  AND (COALESCE(du.conversion_gclid, da.first_touch_gclid) IS NOT NULL
       OR COALESCE(du.conversion_msclkid, da.first_touch_msclkid) IS NOT NULL)
LIMIT 10
```

### Monitor results
```sql
SELECT status, platform, event_type, COUNT(*)
FROM `datawarehouse-412318.raw_ad_conversions.ad_conversion_log`
WHERE sent_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
GROUP BY 1, 2, 3
```

## Known Limitations

- GCLID expires after 90 days — events older than that cannot be uploaded to Google Ads
- Conversion actions/goals must be pre-created in both ad platform UIs before first run
- Microsoft Ads goals need at least 2 hours after creation before accepting uploads
- Refund matching uses `user_id` — if a user has multiple sent conversions, the most recent one is retracted first
