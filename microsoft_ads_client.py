"""Microsoft Ads CAPI (Conversions API) integration for uploading conversions.

Uses the UET Conversions API (REST/JSON) instead of the legacy SOAP
ApplyOfflineConversions endpoint. CAPI provides proper error feedback,
works with UET Event Goals, and supports enhanced conversions natively.

Endpoint: https://capi.uet.microsoft.com/v1/{tagID}/events
Docs: https://learn.microsoft.com/en-us/advertising/guides/uet-conversion-api-integration
"""

import logging

import requests

from hashing import normalize_and_hash_email
from config import (
    MS_CAPI_TAG_ID,
    MS_CAPI_TOKEN,
    CURRENCY_CODE,
)

logger = logging.getLogger(__name__)

CAPI_BASE_URL = "https://capi.uet.microsoft.com/v1"
BATCH_SIZE = 1000


def _to_epoch(dt) -> int:
    """Convert a datetime to UNIX epoch seconds (UTC)."""
    if hasattr(dt, 'timestamp'):
        return int(dt.timestamp())
    # If it's already a number
    return int(dt)


def _chunks(lst: list, n: int):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def _build_event(conv: dict) -> dict:
    """Build a single CAPI event payload from a conversion dict.

    Each conversion is sent as a 'custom' event. The eventName maps to
    the conversion goal's Action expression in MS Ads.
    """
    event = {
        "eventType": "custom",
        "eventId": conv['event_id'],
        "eventName": conv['conversion_goal_name'],
        "eventTime": _to_epoch(conv['conversion_time']),
        "adStorageConsent": "G",
        "userData": {},
        "customData": {
            "value": float(conv['value']),
            "currency": CURRENCY_CODE,
        },
    }

    # MSCLKID for click attribution
    if conv.get('msclkid'):
        event["userData"]["msclkid"] = conv['msclkid']

    # Enhanced conversions: hashed email
    hashed_email = normalize_and_hash_email(conv.get('email'))
    if hashed_email:
        event["userData"]["em"] = hashed_email

    return event


def upload_offline_conversions(
    _unused_service,
    conversions: list[dict],
) -> list[tuple[str, bool, str]]:
    """Upload conversions to Microsoft Ads via CAPI.

    Args:
        _unused_service: Kept for API compatibility with main.py (ignored).
        conversions: List of dicts with keys: event_id, msclkid, conversion_time,
                     value, conversion_goal_name, email (optional)

    Returns:
        List of (event_id, success: bool, message: str) tuples
    """
    if not conversions:
        return []

    url = f"{CAPI_BASE_URL}/{MS_CAPI_TAG_ID}/events"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {MS_CAPI_TOKEN}",
    }

    results = []

    for batch in _chunks(conversions, BATCH_SIZE):
        events = [_build_event(conv) for conv in batch]
        payload = {
            "data": events,
            "continueOnValidationError": True,
        }

        try:
            response = requests.post(url, json=payload, headers=headers, timeout=60)

            if response.status_code == 200:
                # With continueOnValidationError=true, 200 may contain partial errors
                resp_body = response.json() if response.text else {}
                error_details = resp_body.get('error', {}).get('details', [])
                received = resp_body.get('eventsReceived', len(batch))

                if error_details:
                    # Partial success: some events failed validation
                    failed_indices = {}
                    for detail in error_details:
                        idx = detail.get('index')
                        msg = detail.get('errorMessage', 'Unknown validation error')
                        prop = detail.get('propertyName', '')
                        if idx is not None:
                            failed_indices[idx] = f"{prop}: {msg}"

                    for i, conv in enumerate(batch):
                        if i in failed_indices:
                            results.append((conv['event_id'], False, failed_indices[i]))
                            logger.warning(
                                f"Microsoft Ads CAPI: Failed event_id={conv['event_id']}: "
                                f"{failed_indices[i]}"
                            )
                        else:
                            results.append((conv['event_id'], True, 'OK'))

                    logger.info(
                        f"Microsoft Ads CAPI: {received} accepted, "
                        f"{len(failed_indices)} failed in batch of {len(batch)}"
                    )
                else:
                    # All events accepted
                    for conv in batch:
                        results.append((conv['event_id'], True, 'OK'))

            elif response.status_code == 400:
                # Whole batch rejected (continueOnValidationError=false or structural error)
                error_body = response.json() if response.text else {}
                error_msg = error_body.get('error', {}).get('message', response.text[:200])
                logger.error(f"Microsoft Ads CAPI batch rejected: {error_msg}")
                for conv in batch:
                    results.append((conv['event_id'], False, error_msg))

            elif response.status_code == 401:
                error_msg = "CAPI authentication failed (invalid or expired token)"
                logger.error(f"Microsoft Ads CAPI: {error_msg}")
                for conv in batch:
                    results.append((conv['event_id'], False, error_msg))
            else:
                error_msg = f"HTTP {response.status_code}: {response.text[:200]}"
                logger.error(f"Microsoft Ads CAPI unexpected response: {error_msg}")
                for conv in batch:
                    results.append((conv['event_id'], False, error_msg))

        except requests.exceptions.Timeout:
            error_msg = "CAPI request timed out"
            logger.error(f"Microsoft Ads CAPI: {error_msg}")
            for conv in batch:
                results.append((conv['event_id'], False, error_msg))
        except Exception as ex:
            error_msg = str(ex)
            logger.error(f"Microsoft Ads CAPI upload failed: {error_msg}")
            for conv in batch:
                results.append((conv['event_id'], False, error_msg))

    return results


def upload_conversion_retractions(
    _unused_service,
    adjustments: list[dict],
) -> list[tuple[str, bool, str]]:
    """Upload conversion retractions (for refunds) to Microsoft Ads via CAPI.

    CAPI supports retractions by sending a custom event with a negative value
    and the same eventId/transactionId pattern. The retraction is attributed
    via the MSCLKID + conversion name combination.

    Args:
        _unused_service: Kept for API compatibility with main.py (ignored).
        adjustments: List of dicts with keys:
            event_id (refund payment_id), click_id (msclkid),
            original_conversion_action (goal name), original_conversion_time,
            conversion_time (refund/adjustment time)

    Returns:
        List of (event_id, success: bool, message: str) tuples
    """
    if not adjustments:
        return []

    url = f"{CAPI_BASE_URL}/{MS_CAPI_TAG_ID}/events"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {MS_CAPI_TOKEN}",
    }

    results = []

    for batch in _chunks(adjustments, BATCH_SIZE):
        events = []
        for adj in batch:
            event = {
                "eventType": "custom",
                "eventId": adj['event_id'],
                "eventName": adj['original_conversion_action'],
                "eventTime": _to_epoch(adj['conversion_time']),
                "adStorageConsent": "G",
                "userData": {},
                "customData": {
                    "value": 0,
                    "currency": CURRENCY_CODE,
                    "transactionId": f"retract_{adj['event_id']}",
                },
            }

            if adj.get('click_id'):
                event["userData"]["msclkid"] = adj['click_id']

            events.append(event)

        payload = {
            "data": events,
            "continueOnValidationError": True,
        }

        try:
            response = requests.post(url, json=payload, headers=headers, timeout=60)

            if response.status_code == 200:
                resp_body = response.json() if response.text else {}
                error_details = resp_body.get('error', {}).get('details', [])

                if error_details:
                    failed_indices = {}
                    for detail in error_details:
                        idx = detail.get('index')
                        msg = detail.get('errorMessage', 'Unknown validation error')
                        prop = detail.get('propertyName', '')
                        if idx is not None:
                            failed_indices[idx] = f"{prop}: {msg}"

                    for i, adj in enumerate(batch):
                        if i in failed_indices:
                            results.append((adj['event_id'], False, failed_indices[i]))
                            logger.warning(
                                f"Microsoft Ads CAPI retraction: Failed event_id={adj['event_id']}: "
                                f"{failed_indices[i]}"
                            )
                        else:
                            results.append((adj['event_id'], True, 'OK'))
                else:
                    for adj in batch:
                        results.append((adj['event_id'], True, 'OK'))

            elif response.status_code in (400, 401):
                error_body = response.json() if response.text else {}
                error_msg = error_body.get('error', {}).get('message', response.text[:200])
                logger.error(f"Microsoft Ads CAPI retraction failed: {error_msg}")
                for adj in batch:
                    results.append((adj['event_id'], False, error_msg))
            else:
                error_msg = f"HTTP {response.status_code}: {response.text[:200]}"
                logger.error(f"Microsoft Ads CAPI retraction unexpected response: {error_msg}")
                for adj in batch:
                    results.append((adj['event_id'], False, error_msg))

        except Exception as ex:
            error_msg = str(ex)
            logger.error(f"Microsoft Ads CAPI retraction failed: {error_msg}")
            for adj in batch:
                results.append((adj['event_id'], False, error_msg))

    return results


def get_campaign_service():
    """No-op: kept for API compatibility with main.py.

    CAPI uses bearer token auth, no service client needed.
    Returns None; upload functions accept this as _unused_service.
    """
    logger.info("Microsoft Ads CAPI configured (tag_id=%s)", MS_CAPI_TAG_ID)
    return None
