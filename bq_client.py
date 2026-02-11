"""BigQuery operations: table creation, querying, and conversion logging."""

import logging
from datetime import datetime, timezone

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from config import BQ_PROJECT, BQ_DATASET, BQ_LOG_TABLE
from queries import get_create_log_table_query

logger = logging.getLogger(__name__)


def get_client() -> bigquery.Client:
    """Create and return a BigQuery client using Application Default Credentials."""
    return bigquery.Client(project=BQ_PROJECT)


def ensure_log_table(client: bigquery.Client):
    """Create the ad_conversion_log table and dataset if they don't exist."""
    # Ensure dataset exists
    dataset_ref = client.dataset(BQ_DATASET)
    try:
        client.get_dataset(dataset_ref)
        logger.info(f"Dataset {BQ_DATASET} found")
    except NotFound:
        logger.info(f"Dataset {BQ_DATASET} not found, creating...")
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        client.create_dataset(dataset)
        logger.info(f"Dataset {BQ_DATASET} created")

    # Create table if not exists
    query = get_create_log_table_query()
    job = client.query(query)
    job.result()
    logger.info(f"Table {BQ_DATASET}.{BQ_LOG_TABLE} ready")


def run_query(client: bigquery.Client, query: str) -> list[dict]:
    """Execute a query and return results as a list of dicts."""
    job = client.query(query)
    results = []
    for row in job.result():
        results.append(dict(row))
    return results


def log_conversion_results(client: bigquery.Client, rows: list[dict]):
    """Batch insert conversion results into ad_conversion_log.

    Each row should have keys:
        event_id, event_type, platform, click_id, conversion_time,
        conversion_value, conversion_action, currency_code, status,
        api_response, error_message, original_event_id, user_id
    """
    if not rows:
        return

    table_ref = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_LOG_TABLE}"
    now = datetime.now(timezone.utc).isoformat()

    # Prepare rows for insert - ensure all fields present
    insert_rows = []
    for row in rows:
        insert_rows.append({
            'event_id': row['event_id'],
            'event_type': row['event_type'],
            'platform': row['platform'],
            'click_id': row.get('click_id'),
            'conversion_time': row['conversion_time'].isoformat() if hasattr(row['conversion_time'], 'isoformat') else str(row['conversion_time']),
            'conversion_value': float(row.get('conversion_value', 0)),
            'conversion_action': row.get('conversion_action'),
            'currency_code': row.get('currency_code', 'USD'),
            'status': row['status'],
            'api_response': _truncate(row.get('api_response'), 1000),
            'error_message': _truncate(row.get('error_message'), 2000),
            'original_event_id': row.get('original_event_id'),
            'user_id': row.get('user_id'),
            'sent_at': now,
            'created_at': now,
        })

    errors = client.insert_rows_json(table_ref, insert_rows)
    if errors:
        logger.error(f"Failed to insert {len(errors)} rows into {BQ_LOG_TABLE}: {errors[:3]}")
    else:
        logger.info(f"Logged {len(insert_rows)} results to {BQ_LOG_TABLE}")


def _truncate(value: str | None, max_len: int) -> str | None:
    """Truncate a string to max_len characters."""
    if value is None:
        return None
    value = str(value)
    if len(value) > max_len:
        return value[:max_len - 3] + '...'
    return value
