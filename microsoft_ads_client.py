"""Microsoft Ads API integration for uploading offline conversions and adjustments."""

import logging

from bingads.service_client import ServiceClient
from bingads.authorization import AuthorizationData, OAuthWebAuthCodeGrant

from hashing import normalize_and_hash_email
from config import (
    MS_DEV_TOKEN,
    MS_CLIENT_ID,
    MS_CLIENT_SECRET,
    MS_REFRESH_TOKEN,
    MS_ACCOUNT_ID,
    MS_CUSTOMER_ID,
    CURRENCY_CODE,
)

logger = logging.getLogger(__name__)

BATCH_SIZE = 1000


def get_campaign_service() -> ServiceClient:
    """Authenticate and return a CampaignManagementService client.

    Follows the same auth pattern as bing_ads_importer/main.py.
    """
    logger.info("Authenticating with Microsoft Ads...")

    authentication = OAuthWebAuthCodeGrant(
        client_id=MS_CLIENT_ID,
        client_secret=MS_CLIENT_SECRET,
        redirection_uri="http://localhost:8080"
    )
    authentication.request_oauth_tokens_by_refresh_token(MS_REFRESH_TOKEN)

    authorization_data = AuthorizationData(
        account_id=MS_ACCOUNT_ID,
        customer_id=MS_CUSTOMER_ID,
        authentication=authentication,
        developer_token=MS_DEV_TOKEN,
    )

    campaign_service = ServiceClient(
        service='CampaignManagementService',
        version=13,
        authorization_data=authorization_data,
    )

    logger.info("Microsoft Ads authentication successful")
    return campaign_service


def _format_datetime(dt) -> str:
    """Format a datetime for Microsoft Ads API (UTC ISO format)."""
    if hasattr(dt, 'strftime'):
        return dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')
    return str(dt)


def _chunks(lst: list, n: int):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def upload_offline_conversions(
    campaign_service: ServiceClient,
    conversions: list[dict],
) -> list[tuple[str, bool, str]]:
    """Upload offline conversions to Microsoft Ads.

    Args:
        campaign_service: Authenticated CampaignManagementService client
        conversions: List of dicts with keys: event_id, msclkid, conversion_time,
                     value, conversion_goal_name

    Returns:
        List of (event_id, success: bool, message: str) tuples
    """
    if not conversions:
        return []

    results = []

    for batch in _chunks(conversions, BATCH_SIZE):
        offline_conversions = campaign_service.factory.create('ArrayOfOfflineConversion')
        for conv in batch:
            oc = campaign_service.factory.create('OfflineConversion')
            oc.MicrosoftClickId = conv['msclkid']
            oc.ConversionName = conv['conversion_goal_name']
            oc.ConversionTime = _format_datetime(conv['conversion_time'])
            oc.ConversionValue = float(conv['value'])
            oc.ConversionCurrencyCode = CURRENCY_CODE

            # Enhanced conversions: set hashed email if available
            hashed_email = normalize_and_hash_email(conv.get('email'))
            if hashed_email:
                oc.HashedEmailAddress = hashed_email

            offline_conversions.OfflineConversion.append(oc)

        try:
            response = campaign_service.ApplyOfflineConversions(
                OfflineConversions=offline_conversions
            )

            # Check for partial errors
            failed_indices = set()
            error_map = {}
            if hasattr(response, 'PartialErrors') and response.PartialErrors is not None:
                batch_errors = response.PartialErrors.BatchError if hasattr(response.PartialErrors, 'BatchError') else []
                if batch_errors:
                    for error in batch_errors:
                        idx = error.Index
                        failed_indices.add(idx)
                        error_map[idx] = f"Code {error.Code}: {error.Message}"

            for i, conv in enumerate(batch):
                if i in failed_indices:
                    results.append((conv['event_id'], False, error_map[i]))
                    logger.warning(f"Microsoft Ads: Failed event_id={conv['event_id']}: {error_map[i]}")
                else:
                    results.append((conv['event_id'], True, 'OK'))

        except Exception as ex:
            error_msg = str(ex)
            if hasattr(ex, 'fault') and hasattr(ex.fault, 'detail'):
                error_msg = f"{error_msg} | SOAP: {ex.fault.detail}"
            logger.error(f"Microsoft Ads batch upload failed: {error_msg}")
            for conv in batch:
                results.append((conv['event_id'], False, error_msg))

    return results


def upload_conversion_retractions(
    campaign_service: ServiceClient,
    adjustments: list[dict],
) -> list[tuple[str, bool, str]]:
    """Upload conversion retractions (for refunds) to Microsoft Ads.

    Args:
        campaign_service: Authenticated CampaignManagementService client
        adjustments: List of dicts with keys:
            event_id (refund payment_id), click_id (msclkid),
            original_conversion_action (goal name), original_conversion_time,
            conversion_time (refund/adjustment time)

    Returns:
        List of (event_id, success: bool, message: str) tuples
    """
    if not adjustments:
        return []

    results = []

    for batch in _chunks(adjustments, BATCH_SIZE):
        adjustment_objects = campaign_service.factory.create('ArrayOfOfflineConversionAdjustment')
        for adj in batch:
            oca = campaign_service.factory.create('OfflineConversionAdjustment')
            oca.MicrosoftClickId = adj['click_id']
            oca.ConversionName = adj['original_conversion_action']
            oca.ConversionTime = _format_datetime(adj['original_conversion_time'])
            oca.AdjustmentType = 'Retract'
            oca.AdjustmentTime = _format_datetime(adj['conversion_time'])
            oca.AdjustmentValue = 0
            oca.AdjustmentCurrencyCode = CURRENCY_CODE
            adjustment_objects.OfflineConversionAdjustment.append(oca)

        try:
            response = campaign_service.ApplyOfflineConversionAdjustments(
                OfflineConversionAdjustments=adjustment_objects
            )

            # Check for partial errors
            failed_indices = set()
            error_map = {}
            if hasattr(response, 'PartialErrors') and response.PartialErrors is not None:
                batch_errors = response.PartialErrors.BatchError if hasattr(response.PartialErrors, 'BatchError') else []
                if batch_errors:
                    for error in batch_errors:
                        idx = error.Index
                        failed_indices.add(idx)
                        error_map[idx] = f"Code {error.Code}: {error.Message}"

            for i, adj in enumerate(batch):
                if i in failed_indices:
                    results.append((adj['event_id'], False, error_map[i]))
                    logger.warning(f"Microsoft Ads retraction: Failed event_id={adj['event_id']}: {error_map[i]}")
                else:
                    results.append((adj['event_id'], True, 'OK'))

        except Exception as ex:
            error_msg = str(ex)
            if hasattr(ex, 'fault') and hasattr(ex.fault, 'detail'):
                error_msg = f"{error_msg} | SOAP: {ex.fault.detail}"
            logger.error(f"Microsoft Ads retraction batch failed: {error_msg}")
            for adj in batch:
                results.append((adj['event_id'], False, error_msg))

    return results
