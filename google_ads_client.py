"""Google Ads API integration for uploading offline conversions and adjustments."""

import logging

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

from config import (
    GOOGLE_ADS_DEVELOPER_TOKEN,
    GOOGLE_ADS_SA_KEY_PATH,
    GOOGLE_ADS_SA_EMAIL,
    GOOGLE_ADS_CUSTOMER_ID,
    GOOGLE_ADS_LOGIN_CUSTOMER_ID,
    CURRENCY_CODE,
)
from hashing import normalize_and_hash_email, normalize_and_hash_name

logger = logging.getLogger(__name__)

BATCH_SIZE = 2000


def get_client() -> GoogleAdsClient:
    """Create a GoogleAdsClient using service account authentication."""
    credentials = {
        "developer_token": GOOGLE_ADS_DEVELOPER_TOKEN,
        "json_key_file_path": GOOGLE_ADS_SA_KEY_PATH,
        "impersonated_email": GOOGLE_ADS_SA_EMAIL,
        "login_customer_id": GOOGLE_ADS_LOGIN_CUSTOMER_ID,
        "use_proto_plus": True,
    }
    return GoogleAdsClient.load_from_dict(credentials)


def _format_datetime(dt) -> str:
    """Format a datetime for Google Ads API: 'yyyy-mm-dd HH:mm:ss+00:00'."""
    if hasattr(dt, 'strftime'):
        return dt.strftime('%Y-%m-%d %H:%M:%S+00:00')
    return str(dt)


def _chunks(lst: list, n: int):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def _get_conversion_action_resource(client: GoogleAdsClient, customer_id: str, action_name: str) -> str:
    """Look up a conversion action resource name by its name.

    Returns the resource_name string like:
    'customers/{customer_id}/conversionActions/{conversion_action_id}'
    """
    ga_service = client.get_service("GoogleAdsService")
    query = f"""
        SELECT conversion_action.resource_name, conversion_action.name
        FROM conversion_action
        WHERE conversion_action.name = '{action_name}'
    """
    response = ga_service.search(customer_id=customer_id, query=query)
    for row in response:
        return row.conversion_action.resource_name

    raise ValueError(f"Conversion action '{action_name}' not found in Google Ads account {customer_id}")


def _build_user_identifiers(client: GoogleAdsClient, conv: dict) -> list:
    """Build UserIdentifier objects from PII fields in the conversion dict.

    Returns a list of UserIdentifier proto objects for Enhanced Conversions.
    Returns empty list if no PII is available.
    """
    identifiers = []

    # Email identifier
    hashed_email = normalize_and_hash_email(conv.get('email'))
    if hashed_email:
        ui = client.get_type("UserIdentifier")
        ui.user_identifier_source = client.enums.UserIdentifierSourceEnum.FIRST_PARTY
        ui.hashed_email = hashed_email
        identifiers.append(ui)

    # Address-based identifier (name + address components)
    hashed_first = normalize_and_hash_name(conv.get('first_name'))
    hashed_last = normalize_and_hash_name(conv.get('last_name'))

    if hashed_first or hashed_last:
        ui = client.get_type("UserIdentifier")
        ui.user_identifier_source = client.enums.UserIdentifierSourceEnum.FIRST_PARTY
        address_info = client.get_type("OfflineUserAddressInfo")

        if hashed_first:
            address_info.hashed_first_name = hashed_first
        if hashed_last:
            address_info.hashed_last_name = hashed_last

        # These fields are NOT hashed per Google's spec
        city = conv.get('city')
        if city and str(city).strip():
            address_info.city = str(city).strip()

        state = conv.get('state')
        if state and str(state).strip():
            address_info.state = str(state).strip()

        country = conv.get('country')
        if country and str(country).strip():
            address_info.country_code = str(country).strip().upper()

        zip_code = conv.get('zip_code')
        if zip_code and str(zip_code).strip():
            address_info.postal_code = str(zip_code).strip()

        ui.address_info = address_info
        identifiers.append(ui)

    return identifiers


def upload_click_conversions(
    client: GoogleAdsClient,
    conversions: list[dict],
    conversion_action_cache: dict,
) -> list[tuple[str, bool, str]]:
    """Upload click conversions to Google Ads.

    Args:
        client: GoogleAdsClient instance
        conversions: List of dicts with keys: event_id, gclid, conversion_time, value, event_type
        conversion_action_cache: Dict mapping event_type -> resource_name (populated on first use)

    Returns:
        List of (event_id, success: bool, message: str) tuples
    """
    if not conversions:
        return []

    service = client.get_service("ConversionUploadService")
    customer_id = GOOGLE_ADS_CUSTOMER_ID
    results = []

    for batch in _chunks(conversions, BATCH_SIZE):
        request = client.get_type("UploadClickConversionsRequest")
        request.customer_id = customer_id
        request.partial_failure = True

        for conv in batch:
            event_type = conv['event_type']

            # Resolve conversion action resource name (cached)
            if event_type not in conversion_action_cache:
                from config import GADS_ACTION_MAP
                action_name = GADS_ACTION_MAP[event_type]
                conversion_action_cache[event_type] = _get_conversion_action_resource(
                    client, customer_id, action_name
                )

            click_conversion = client.get_type("ClickConversion")
            click_conversion.conversion_action = conversion_action_cache[event_type]
            click_conversion.gclid = conv['gclid']
            click_conversion.conversion_date_time = _format_datetime(conv['conversion_time'])
            click_conversion.conversion_value = float(conv['value'])
            click_conversion.currency_code = CURRENCY_CODE
            click_conversion.order_id = conv['event_id']

            # Enhanced conversions: attach user identifiers if PII available
            for uid in _build_user_identifiers(client, conv):
                click_conversion.user_identifiers.append(uid)

            request.conversions.append(click_conversion)

        try:
            response = service.upload_click_conversions(request=request)

            if response.partial_failure_error:
                # Parse partial failure errors
                failure_errors = _parse_partial_failures(client, response)
                for i, conv in enumerate(batch):
                    if i in failure_errors:
                        results.append((conv['event_id'], False, failure_errors[i]))
                        logger.warning(f"Google Ads: Failed event_id={conv['event_id']}: {failure_errors[i]}")
                    else:
                        results.append((conv['event_id'], True, 'OK'))
            else:
                for conv in batch:
                    results.append((conv['event_id'], True, 'OK'))

        except GoogleAdsException as ex:
            error_msg = str(ex.failure) if hasattr(ex, 'failure') else str(ex)
            logger.error(f"Google Ads batch upload failed: {error_msg}")
            for conv in batch:
                results.append((conv['event_id'], False, error_msg))
        except Exception as ex:
            error_msg = str(ex)
            logger.error(f"Google Ads unexpected error: {error_msg}")
            for conv in batch:
                results.append((conv['event_id'], False, error_msg))

    return results


def upload_conversion_retractions(
    client: GoogleAdsClient,
    adjustments: list[dict],
    conversion_action_cache: dict,
) -> list[tuple[str, bool, str]]:
    """Upload conversion retractions (for refunds) to Google Ads.

    Args:
        client: GoogleAdsClient instance
        adjustments: List of dicts with keys:
            event_id (refund payment_id), original_event_id, original_conversion_action,
            conversion_time (refund time), original_conversion_time, click_id
        conversion_action_cache: Dict mapping action_name -> resource_name

    Returns:
        List of (event_id, success: bool, message: str) tuples
    """
    if not adjustments:
        return []

    service = client.get_service("ConversionAdjustmentUploadService")
    customer_id = GOOGLE_ADS_CUSTOMER_ID
    results = []

    for batch in _chunks(adjustments, BATCH_SIZE):
        request = client.get_type("UploadConversionAdjustmentsRequest")
        request.customer_id = customer_id
        request.partial_failure = True

        for adj in batch:
            action_name = adj['original_conversion_action']

            # Resolve conversion action resource name (cached by action name)
            if action_name not in conversion_action_cache:
                conversion_action_cache[action_name] = _get_conversion_action_resource(
                    client, customer_id, action_name
                )

            adjustment = client.get_type("ConversionAdjustment")
            adjustment.conversion_action = conversion_action_cache[action_name]
            adjustment.adjustment_type = client.enums.ConversionAdjustmentTypeEnum.RETRACTION
            adjustment.order_id = adj['original_event_id']
            adjustment.adjustment_date_time = _format_datetime(adj['conversion_time'])
            request.conversion_adjustments.append(adjustment)

        try:
            response = service.upload_conversion_adjustments(request=request)

            if response.partial_failure_error:
                failure_errors = _parse_partial_failures(client, response)
                for i, adj in enumerate(batch):
                    if i in failure_errors:
                        results.append((adj['event_id'], False, failure_errors[i]))
                        logger.warning(f"Google Ads retraction: Failed event_id={adj['event_id']}: {failure_errors[i]}")
                    else:
                        results.append((adj['event_id'], True, 'OK'))
            else:
                for adj in batch:
                    results.append((adj['event_id'], True, 'OK'))

        except GoogleAdsException as ex:
            error_msg = str(ex.failure) if hasattr(ex, 'failure') else str(ex)
            logger.error(f"Google Ads retraction batch failed: {error_msg}")
            for adj in batch:
                results.append((adj['event_id'], False, error_msg))
        except Exception as ex:
            error_msg = str(ex)
            logger.error(f"Google Ads retraction unexpected error: {error_msg}")
            for adj in batch:
                results.append((adj['event_id'], False, error_msg))

    return results


def _parse_partial_failures(client: GoogleAdsClient, response) -> dict[int, str]:
    """Parse partial failure errors from a Google Ads API response.

    Returns a dict mapping operation index -> error message.
    """
    errors = {}
    if not response.partial_failure_error:
        return errors

    partial_failure = response.partial_failure_error
    for error_detail in partial_failure.details:
        try:
            failure_message = client.get_type("GoogleAdsFailure")
            # Parse the Any proto
            if error_detail.Is(failure_message.DESCRIPTOR):
                error_detail.Unpack(failure_message)
                for error in failure_message.errors:
                    # Extract the operation index from the field path
                    for path_element in error.location.field_path_elements:
                        if path_element.field_name in ('conversions', 'conversion_adjustments'):
                            errors[path_element.index] = error.message
                            break
        except Exception:
            pass

    return errors
