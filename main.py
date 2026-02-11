"""Conversion Uploader - Syncs offline conversions from BigQuery to Google Ads and Microsoft Ads.

Cloud Run Job entry point. Triggered daily via Cloud Scheduler.
"""

import sys
import logging

from config import (
    validate_env_vars,
    LOOKBACK_DAYS,
    MAX_RETRIES,
    SEND_RENEWAL_PAYMENTS,
    DRY_RUN,
    GADS_ACTION_MAP,
    MSADS_GOAL_MAP,
    CURRENCY_CODE,
)
from bq_client import get_client, ensure_log_table, run_query, log_conversion_results
from queries import (
    get_unsent_trial_starts_query,
    get_unsent_subscriptions_query,
    get_unsent_document_purchases_query,
    get_unsent_chat_purchases_query,
    get_unsent_refunds_query,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def split_by_platform(events: list[dict]) -> tuple[list[dict], list[dict]]:
    """Split events into Google Ads (gclid) and Microsoft Ads (msclkid) lists.

    Events with both click IDs appear in both lists.
    """
    google_events = []
    microsoft_events = []

    for event in events:
        if event.get('gclid'):
            google_events.append(event)
        if event.get('msclkid'):
            microsoft_events.append(event)

    return google_events, microsoft_events


def process_event_type(
    bq_client,
    event_type_label: str,
    query: str,
    gads_client,
    msads_service,
    gads_action_cache: dict,
    summary: dict,
):
    """Process a single event type: query, upload to platforms, log results."""
    logger.info(f"--- {event_type_label} ---")

    events = run_query(bq_client, query)
    if not events:
        logger.info(f"No unsent {event_type_label} events found")
        return

    google_events, microsoft_events = split_by_platform(events)
    logger.info(
        f"Found {len(events)} unsent {event_type_label} events "
        f"({len(google_events)} Google Ads, {len(microsoft_events)} Microsoft Ads)"
    )

    # --- Google Ads ---
    if google_events:
        if DRY_RUN:
            logger.info(f"[DRY RUN] Would send {len(google_events)} events to Google Ads")
            for e in google_events[:3]:
                logger.info(f"  event_id={e['event_id']}, gclid={e['gclid']}, value={e.get('conversion_value', 0)}")
        else:
            try:
                from google_ads_client import upload_click_conversions

                gads_conversions = [
                    {
                        'event_id': e['event_id'],
                        'gclid': e['gclid'],
                        'conversion_time': e['conversion_time'],
                        'value': e.get('conversion_value', 0),
                        'event_type': e['event_type'],
                    }
                    for e in google_events
                ]
                gads_results = upload_click_conversions(gads_client, gads_conversions, gads_action_cache)

                # Log results to BigQuery
                log_rows = []
                for event, (event_id, success, message) in zip(google_events, gads_results):
                    status = 'sent' if success else 'failed'
                    log_rows.append({
                        'event_id': event_id,
                        'event_type': event['event_type'],
                        'platform': 'google_ads',
                        'click_id': event['gclid'],
                        'conversion_time': event['conversion_time'],
                        'conversion_value': event.get('conversion_value', 0),
                        'conversion_action': GADS_ACTION_MAP.get(event['event_type']),
                        'currency_code': CURRENCY_CODE,
                        'status': status,
                        'api_response': message if success else None,
                        'error_message': message if not success else None,
                        'user_id': event.get('user_id'),
                    })
                    if success:
                        summary['google_ads']['sent'] += 1
                    else:
                        summary['google_ads']['failed'] += 1

                log_conversion_results(bq_client, log_rows)

                sent = sum(1 for _, s, _ in gads_results if s)
                failed = sum(1 for _, s, _ in gads_results if not s)
                logger.info(f"Google Ads: {sent} sent, {failed} failed")
            except Exception as e:
                logger.error(f"Google Ads upload failed for {event_type_label}: {e}")

    # --- Microsoft Ads ---
    if microsoft_events:
        if DRY_RUN:
            logger.info(f"[DRY RUN] Would send {len(microsoft_events)} events to Microsoft Ads")
            for e in microsoft_events[:3]:
                logger.info(f"  event_id={e['event_id']}, msclkid={e['msclkid']}, value={e.get('conversion_value', 0)}")
        else:
            try:
                from microsoft_ads_client import upload_offline_conversions

                msads_conversions = [
                    {
                        'event_id': e['event_id'],
                        'msclkid': e['msclkid'],
                        'conversion_time': e['conversion_time'],
                        'value': e.get('conversion_value', 0),
                        'conversion_goal_name': MSADS_GOAL_MAP.get(e['event_type']),
                    }
                    for e in microsoft_events
                ]
                msads_results = upload_offline_conversions(msads_service, msads_conversions)

                log_rows = []
                for event, (event_id, success, message) in zip(microsoft_events, msads_results):
                    status = 'sent' if success else 'failed'
                    log_rows.append({
                        'event_id': event_id,
                        'event_type': event['event_type'],
                        'platform': 'microsoft_ads',
                        'click_id': event['msclkid'],
                        'conversion_time': event['conversion_time'],
                        'conversion_value': event.get('conversion_value', 0),
                        'conversion_action': MSADS_GOAL_MAP.get(event['event_type']),
                        'currency_code': CURRENCY_CODE,
                        'status': status,
                        'api_response': message if success else None,
                        'error_message': message if not success else None,
                        'user_id': event.get('user_id'),
                    })
                    if success:
                        summary['microsoft_ads']['sent'] += 1
                    else:
                        summary['microsoft_ads']['failed'] += 1

                log_conversion_results(bq_client, log_rows)

                sent = sum(1 for _, s, _ in msads_results if s)
                failed = sum(1 for _, s, _ in msads_results if not s)
                logger.info(f"Microsoft Ads: {sent} sent, {failed} failed")
            except Exception as e:
                logger.error(f"Microsoft Ads upload failed for {event_type_label}: {e}")


def process_refunds(
    bq_client,
    gads_client,
    msads_service,
    gads_action_cache: dict,
    summary: dict,
    skip_dedup: bool = False,
):
    """Process refund events: send retractions to the platform the original was sent to."""
    logger.info("--- Refunds ---")

    query = get_unsent_refunds_query(LOOKBACK_DAYS, MAX_RETRIES, skip_dedup)
    refunds = run_query(bq_client, query)

    if not refunds:
        logger.info("No unsent refund events found")
        return

    logger.info(f"Found {len(refunds)} unsent refund events")

    # Split by platform (platform comes from the original conversion log entry)
    google_refunds = [r for r in refunds if r['platform'] == 'google_ads']
    microsoft_refunds = [r for r in refunds if r['platform'] == 'microsoft_ads']

    # --- Google Ads Retractions ---
    if google_refunds:
        if DRY_RUN:
            logger.info(f"[DRY RUN] Would send {len(google_refunds)} retractions to Google Ads")
        else:
            try:
                from google_ads_client import upload_conversion_retractions

                gads_adjustments = [
                    {
                        'event_id': r['event_id'],
                        'original_event_id': r['original_event_id'],
                        'original_conversion_action': r['original_conversion_action'],
                        'conversion_time': r['conversion_time'],
                        'original_conversion_time': r['original_conversion_time'],
                        'click_id': r['click_id'],
                    }
                    for r in google_refunds
                ]
                gads_results = upload_conversion_retractions(gads_client, gads_adjustments, gads_action_cache)

                log_rows = []
                for refund, (event_id, success, message) in zip(google_refunds, gads_results):
                    status = 'retracted' if success else 'failed'
                    log_rows.append({
                        'event_id': event_id,
                        'event_type': 'refund',
                        'platform': 'google_ads',
                        'click_id': refund['click_id'],
                        'conversion_time': refund['conversion_time'],
                        'conversion_value': refund.get('conversion_value', 0),
                        'conversion_action': refund['original_conversion_action'],
                        'currency_code': CURRENCY_CODE,
                        'status': status,
                        'api_response': message if success else None,
                        'error_message': message if not success else None,
                        'original_event_id': refund['original_event_id'],
                        'user_id': refund.get('user_id'),
                    })
                    if success:
                        summary['google_ads']['retracted'] += 1
                    else:
                        summary['google_ads']['failed'] += 1

                log_conversion_results(bq_client, log_rows)

                sent = sum(1 for _, s, _ in gads_results if s)
                failed = sum(1 for _, s, _ in gads_results if not s)
                logger.info(f"Google Ads retractions: {sent} sent, {failed} failed")
            except Exception as e:
                logger.error(f"Google Ads retractions failed: {e}")

    # --- Microsoft Ads Retractions ---
    if microsoft_refunds:
        if DRY_RUN:
            logger.info(f"[DRY RUN] Would send {len(microsoft_refunds)} retractions to Microsoft Ads")
        else:
            try:
                from microsoft_ads_client import upload_conversion_retractions

                msads_adjustments = [
                    {
                        'event_id': r['event_id'],
                        'click_id': r['click_id'],
                        'original_conversion_action': r['original_conversion_action'],
                        'original_conversion_time': r['original_conversion_time'],
                        'conversion_time': r['conversion_time'],
                    }
                    for r in microsoft_refunds
                ]
                msads_results = upload_conversion_retractions(msads_service, msads_adjustments)

                log_rows = []
                for refund, (event_id, success, message) in zip(microsoft_refunds, msads_results):
                    status = 'retracted' if success else 'failed'
                    log_rows.append({
                        'event_id': event_id,
                        'event_type': 'refund',
                        'platform': 'microsoft_ads',
                        'click_id': refund['click_id'],
                        'conversion_time': refund['conversion_time'],
                        'conversion_value': refund.get('conversion_value', 0),
                        'conversion_action': refund['original_conversion_action'],
                        'currency_code': CURRENCY_CODE,
                        'status': status,
                        'api_response': message if success else None,
                        'error_message': message if not success else None,
                        'original_event_id': refund['original_event_id'],
                        'user_id': refund.get('user_id'),
                    })
                    if success:
                        summary['microsoft_ads']['retracted'] += 1
                    else:
                        summary['microsoft_ads']['failed'] += 1

                log_conversion_results(bq_client, log_rows)

                sent = sum(1 for _, s, _ in msads_results if s)
                failed = sum(1 for _, s, _ in msads_results if not s)
                logger.info(f"Microsoft Ads retractions: {sent} sent, {failed} failed")
            except Exception as e:
                logger.error(f"Microsoft Ads retractions failed: {e}")


def main():
    logger.info("=" * 60)
    logger.info("Starting Conversion Uploader")
    logger.info("=" * 60)

    try:
        # 1. Validate configuration
        validate_env_vars()

        # 2. Initialize BigQuery client
        bq = get_client()

        # 3. Ensure tracking table exists
        log_table_ready = ensure_log_table(bq, dry_run=DRY_RUN)
        # When log table doesn't exist, skip dedup (all events treated as unsent)
        skip_dedup = not log_table_ready

        if skip_dedup:
            logger.warning("Log table not available — skipping dedup (all events treated as unsent)")

        # 4. Initialize ad platform clients (lazy - only if needed)
        gads_client = None
        msads_service = None
        gads_action_cache = {}  # Caches conversion action resource names

        if not DRY_RUN:
            from google_ads_client import get_client as get_gads_client
            from microsoft_ads_client import get_campaign_service

            gads_client = get_gads_client()
            logger.info("Google Ads client initialized")

            msads_service = get_campaign_service()

        # 5. Summary tracking
        summary = {
            'google_ads': {'sent': 0, 'failed': 0, 'retracted': 0},
            'microsoft_ads': {'sent': 0, 'failed': 0, 'retracted': 0},
        }

        # 6. Process each event type
        event_queries = [
            ("Trial Starts", get_unsent_trial_starts_query(LOOKBACK_DAYS, MAX_RETRIES, skip_dedup)),
            ("Subscriptions", get_unsent_subscriptions_query(LOOKBACK_DAYS, MAX_RETRIES, SEND_RENEWAL_PAYMENTS, skip_dedup)),
            ("Document Purchases", get_unsent_document_purchases_query(LOOKBACK_DAYS, MAX_RETRIES, skip_dedup)),
            ("Chat Purchases", get_unsent_chat_purchases_query(LOOKBACK_DAYS, MAX_RETRIES, skip_dedup)),
        ]

        for label, query in event_queries:
            try:
                process_event_type(
                    bq, label, query,
                    gads_client, msads_service, gads_action_cache, summary,
                )
            except Exception as e:
                logger.error(f"Error processing {label}: {e}")
                # Continue to next event type

        # 7. Process refunds
        try:
            process_refunds(bq, gads_client, msads_service, gads_action_cache, summary, skip_dedup)
        except Exception as e:
            logger.error(f"Error processing refunds: {e}")

        # 8. Final summary
        logger.info("=" * 60)
        logger.info("Summary:")
        for platform, counts in summary.items():
            logger.info(
                f"  {platform}: {counts['sent']} sent, "
                f"{counts['retracted']} retracted, {counts['failed']} failed"
            )
        logger.info("=" * 60)

        total_failed = sum(c['failed'] for c in summary.values())
        if total_failed > 0:
            logger.warning(f"Completed with {total_failed} failures (will retry on next run)")

        logger.info("Conversion Uploader completed successfully")
        sys.exit(0)

    except Exception as e:
        logger.error("=" * 60)
        logger.error(f"Conversion Uploader failed: {str(e)}")
        logger.error("=" * 60)
        sys.exit(1)


if __name__ == "__main__":
    main()
