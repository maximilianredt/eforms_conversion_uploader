"""SQL query templates for fetching unsent conversion events from BigQuery."""

from config import BQ_PROJECT, BQ_DATASET, BQ_LOG_TABLE, DBT_STAGING_DATASET, DBT_MARTS_DATASET

# Fully qualified table references
LOG_TABLE = f"`{BQ_PROJECT}.{BQ_DATASET}.{BQ_LOG_TABLE}`"
TRIAL_TABLE = f"`{BQ_PROJECT}.{DBT_STAGING_DATASET}.stg_php_prod__trial_started`"
PAYMENTS_TABLE = f"`{BQ_PROJECT}.{DBT_MARTS_DATASET}.fct_payments`"
DIM_USERS_TABLE = f"`{BQ_PROJECT}.{DBT_MARTS_DATASET}.dim_users`"
DIM_ATTRIBUTION_TABLE = f"`{BQ_PROJECT}.{DBT_MARTS_DATASET}.dim_attribution`"


def _dedup_cte(event_type_filter: str, skip_dedup: bool) -> str:
    """Generate the failed_counts CTE, or empty string if skipping dedup."""
    if skip_dedup:
        return ""
    return f"""
    WITH failed_counts AS (
        SELECT event_id, platform, COUNT(*) AS fail_count
        FROM {LOG_TABLE}
        WHERE event_type {event_type_filter} AND status = 'failed'
        GROUP BY event_id, platform
    )"""


def _dedup_joins(id_col: str, max_retries: int, skip_dedup: bool) -> str:
    """Generate the LEFT JOINs for dedup against the log table."""
    if skip_dedup:
        return ""
    return f"""
    -- Exclude already sent to Google Ads
    LEFT JOIN {LOG_TABLE} log_g
        ON {id_col} = log_g.event_id
        AND log_g.platform = 'google_ads'
        AND log_g.status = 'sent'
    -- Exclude already sent to Microsoft Ads
    LEFT JOIN {LOG_TABLE} log_m
        ON {id_col} = log_m.event_id
        AND log_m.platform = 'microsoft_ads'
        AND log_m.status = 'sent'
    -- Exclude over-retried for Google Ads
    LEFT JOIN failed_counts fc_g
        ON {id_col} = fc_g.event_id
        AND fc_g.platform = 'google_ads'
        AND fc_g.fail_count >= {max_retries}
    -- Exclude over-retried for Microsoft Ads
    LEFT JOIN failed_counts fc_m
        ON {id_col} = fc_m.event_id
        AND fc_m.platform = 'microsoft_ads'
        AND fc_m.fail_count >= {max_retries}"""


def _dedup_where(skip_dedup: bool) -> str:
    """Generate the WHERE clause for dedup filtering."""
    if skip_dedup:
        return ""
    return """
        AND (
            (COALESCE(du.conversion_gclid, da.first_touch_gclid) IS NOT NULL
             AND log_g.event_id IS NULL AND fc_g.event_id IS NULL)
            OR
            (COALESCE(du.conversion_msclkid, da.first_touch_msclkid) IS NOT NULL
             AND log_m.event_id IS NULL AND fc_m.event_id IS NULL)
        )"""


def get_unsent_trial_starts_query(lookback_days: int, max_retries: int, skip_dedup: bool = False) -> str:
    """Query for trial start events not yet sent to ad platforms."""
    cte = _dedup_cte("= 'trial_start'", skip_dedup)
    joins = _dedup_joins("ts.event_id", max_retries, skip_dedup)
    where = _dedup_where(skip_dedup)

    return f"""
    {cte}
    SELECT
        ts.event_id,
        'trial_start' AS event_type,
        ts.user_id,
        ts.trial_started_at AS conversion_time,
        0.0 AS conversion_value,
        COALESCE(du.conversion_gclid, da.first_touch_gclid) AS gclid,
        COALESCE(du.conversion_msclkid, da.first_touch_msclkid) AS msclkid,
        du.email,
        du.first_name,
        du.last_name,
        du.city,
        du.state,
        du.country,
        du.zip_code
    FROM {TRIAL_TABLE} ts
    LEFT JOIN {DIM_USERS_TABLE} du ON ts.user_id = du.user_id
    LEFT JOIN {DIM_ATTRIBUTION_TABLE} da ON ts.user_id = da.user_id
    {joins}
    WHERE
        ts.trial_started_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {lookback_days} DAY)
        AND (
            COALESCE(du.conversion_gclid, da.first_touch_gclid) IS NOT NULL
            OR COALESCE(du.conversion_msclkid, da.first_touch_msclkid) IS NOT NULL
        )
        {where}
    """


def get_unsent_subscriptions_query(lookback_days: int, max_retries: int, include_renewals: bool, skip_dedup: bool = False) -> str:
    """Query for subscription payment events not yet sent to ad platforms."""
    payment_type_filter = (
        "p.payment_type IN ('initial_subscription', 'renewal')"
        if include_renewals
        else "p.payment_type = 'initial_subscription'"
    )
    cte = _dedup_cte("IN ('monthly_subscription', 'yearly_subscription')", skip_dedup)
    joins = _dedup_joins("p.payment_id", max_retries, skip_dedup)
    where = _dedup_where(skip_dedup)

    return f"""
    {cte}
    SELECT
        p.payment_id AS event_id,
        CASE
            WHEN p.billing_frequency = 'annual' THEN 'yearly_subscription'
            ELSE 'monthly_subscription'
        END AS event_type,
        p.user_id,
        p.payment_at AS conversion_time,
        p.amount AS conversion_value,
        COALESCE(du.conversion_gclid, da.first_touch_gclid) AS gclid,
        COALESCE(du.conversion_msclkid, da.first_touch_msclkid) AS msclkid,
        du.email,
        du.first_name,
        du.last_name,
        du.city,
        du.state,
        du.country,
        du.zip_code
    FROM {PAYMENTS_TABLE} p
    LEFT JOIN {DIM_USERS_TABLE} du ON p.user_id = du.user_id
    LEFT JOIN {DIM_ATTRIBUTION_TABLE} da ON p.user_id = da.user_id
    {joins}
    WHERE
        p.payment_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {lookback_days} DAY)
        AND {payment_type_filter}
        AND p.payment_source = 'subscription'
        AND p.payment_status = 'completed'
        AND p.amount > 0
        AND (
            COALESCE(du.conversion_gclid, da.first_touch_gclid) IS NOT NULL
            OR COALESCE(du.conversion_msclkid, da.first_touch_msclkid) IS NOT NULL
        )
        {where}
    """


def get_unsent_document_purchases_query(lookback_days: int, max_retries: int, skip_dedup: bool = False) -> str:
    """Query for document purchase events not yet sent to ad platforms."""
    cte = _dedup_cte("= 'document_purchase'", skip_dedup)
    joins = _dedup_joins("p.payment_id", max_retries, skip_dedup)
    where = _dedup_where(skip_dedup)

    return f"""
    {cte}
    SELECT
        p.payment_id AS event_id,
        'document_purchase' AS event_type,
        p.user_id,
        p.payment_at AS conversion_time,
        p.amount AS conversion_value,
        COALESCE(du.conversion_gclid, da.first_touch_gclid) AS gclid,
        COALESCE(du.conversion_msclkid, da.first_touch_msclkid) AS msclkid,
        du.email,
        du.first_name,
        du.last_name,
        du.city,
        du.state,
        du.country,
        du.zip_code
    FROM {PAYMENTS_TABLE} p
    LEFT JOIN {DIM_USERS_TABLE} du ON p.user_id = du.user_id
    LEFT JOIN {DIM_ATTRIBUTION_TABLE} da ON p.user_id = da.user_id
    {joins}
    WHERE
        p.payment_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {lookback_days} DAY)
        AND p.payment_type = 'order'
        AND p.payment_source = 'order'
        AND p.payment_status = 'completed'
        AND p.amount > 0
        AND (p.plan_code IS NULL OR p.plan_code != '10')
        AND (
            COALESCE(du.conversion_gclid, da.first_touch_gclid) IS NOT NULL
            OR COALESCE(du.conversion_msclkid, da.first_touch_msclkid) IS NOT NULL
        )
        {where}
    """


def get_unsent_chat_purchases_query(lookback_days: int, max_retries: int, skip_dedup: bool = False) -> str:
    """Query for chat purchase events not yet sent to ad platforms."""
    cte = _dedup_cte("= 'chat_purchase'", skip_dedup)
    joins = _dedup_joins("p.payment_id", max_retries, skip_dedup)
    where = _dedup_where(skip_dedup)

    return f"""
    {cte}
    SELECT
        p.payment_id AS event_id,
        'chat_purchase' AS event_type,
        p.user_id,
        p.payment_at AS conversion_time,
        p.amount AS conversion_value,
        COALESCE(du.conversion_gclid, da.first_touch_gclid) AS gclid,
        COALESCE(du.conversion_msclkid, da.first_touch_msclkid) AS msclkid,
        du.email,
        du.first_name,
        du.last_name,
        du.city,
        du.state,
        du.country,
        du.zip_code
    FROM {PAYMENTS_TABLE} p
    LEFT JOIN {DIM_USERS_TABLE} du ON p.user_id = du.user_id
    LEFT JOIN {DIM_ATTRIBUTION_TABLE} da ON p.user_id = da.user_id
    {joins}
    WHERE
        p.payment_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {lookback_days} DAY)
        AND p.payment_type = 'order'
        AND p.payment_source = 'order'
        AND p.payment_status = 'completed'
        AND p.amount > 0
        AND p.plan_code = '10'
        AND (
            COALESCE(du.conversion_gclid, da.first_touch_gclid) IS NOT NULL
            OR COALESCE(du.conversion_msclkid, da.first_touch_msclkid) IS NOT NULL
        )
        {where}
    """


def get_unsent_refunds_query(lookback_days: int, max_retries: int, skip_dedup: bool = False) -> str:
    """Query for refund events matched to their original sent conversions.

    Note: When skip_dedup=True (log table doesn't exist), refunds cannot be
    processed because they require matching against previously-sent conversions
    in the log table. Returns an empty result in this case.
    """
    if skip_dedup:
        # Refunds require the log table to match originals - return empty
        return "SELECT * FROM UNNEST([]) WHERE FALSE"

    return f"""
    WITH failed_counts AS (
        SELECT event_id, platform, COUNT(*) AS fail_count
        FROM {LOG_TABLE}
        WHERE event_type = 'refund' AND status = 'failed'
        GROUP BY event_id, platform
    ),
    -- Find the original sent conversion for each refunded user+platform
    -- Rank by most recent conversion time to match the right one
    ranked_originals AS (
        SELECT
            orig_log.*,
            ROW_NUMBER() OVER (
                PARTITION BY orig_log.user_id, orig_log.platform
                ORDER BY orig_log.conversion_time DESC
            ) AS rn
        FROM {LOG_TABLE} orig_log
        WHERE orig_log.status = 'sent'
            AND orig_log.event_type != 'refund'
    )
    SELECT
        p.payment_id AS event_id,
        'refund' AS event_type,
        p.user_id,
        p.payment_at AS conversion_time,
        p.amount AS conversion_value,
        ro.event_id AS original_event_id,
        ro.platform,
        ro.click_id,
        ro.conversion_time AS original_conversion_time,
        ro.conversion_action AS original_conversion_action
    FROM {PAYMENTS_TABLE} p
    INNER JOIN ranked_originals ro
        ON p.user_id = ro.user_id
        AND ro.rn = 1
    -- Exclude already-sent retractions
    LEFT JOIN {LOG_TABLE} refund_log
        ON p.payment_id = refund_log.event_id
        AND refund_log.platform = ro.platform
        AND refund_log.status IN ('sent', 'retracted')
    -- Exclude over-retried
    LEFT JOIN failed_counts fc
        ON p.payment_id = fc.event_id
        AND fc.platform = ro.platform
        AND fc.fail_count >= {max_retries}
    WHERE
        p.payment_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {lookback_days} DAY)
        AND p.payment_type IN ('refund', 'order_refund')
        AND p.amount < 0
        AND refund_log.event_id IS NULL
        AND fc.event_id IS NULL
    """


def get_create_log_table_query() -> str:
    """DDL to create the ad_conversion_log table if it doesn't exist."""
    return f"""
    CREATE TABLE IF NOT EXISTS {LOG_TABLE} (
        event_id STRING NOT NULL,
        event_type STRING NOT NULL,
        platform STRING NOT NULL,
        click_id STRING,
        conversion_time TIMESTAMP NOT NULL,
        conversion_value FLOAT64,
        conversion_action STRING,
        currency_code STRING DEFAULT 'USD',
        status STRING NOT NULL,
        api_response STRING,
        error_message STRING,
        original_event_id STRING,
        user_id STRING,
        sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    )
    PARTITION BY DATE(sent_at)
    CLUSTER BY event_type, platform, status
    """
