"""
Utility functions for IRP Integration module.

Provides common helper functions for response parsing, data extraction,
and reference data lookup operations.
"""

import base64
import hashlib
import json
import logging
from typing import Callable, Dict, List, Any, Optional
import requests
from .exceptions import IRPAPIError, IRPReferenceDataError

logger = logging.getLogger(__name__)

DEFAULT_PAGE_LIMIT = 100
MAX_PAGES = 1000


def get_location_header(
    response: requests.Response,
    error_context: str = "response"
) -> str:
    """
    Get Location header from response.

    Args:
        response: HTTP response object
        error_context: Context description for error message

    Returns:
        Location header value

    Raises:
        IRPAPIError: If the Location header is missing
    """
    if 'location' not in response.headers:
        raise IRPAPIError(
            f"Location header missing from {error_context}"
        )
    return response.headers.get('location', '')


def extract_id_from_location_header(
    response: requests.Response,
    error_context: str = "response",
) -> str:
    """
    Extract ID from Location header in HTTP response.

    Args:
        response: HTTP response object
        error_context: Context description for error message

    Returns:
        Extracted ID string

    Raises:
        IRPAPIError: If Location header is missing
    """
    location = get_location_header(response, error_context)
    resource_id = location.split('/')[-1]
    if not resource_id:
        raise IRPAPIError(
            f"Could not extract ID from Location header: {location}"
        )
    return resource_id


def decode_base64_field(encoded_value: str, field_name: str) -> str:
    """
    Decode a base64-encoded field value.

    Args:
        encoded_value: Base64-encoded string
        field_name: Field name for error message

    Returns:
        Decoded string

    Raises:
        IRPAPIError: If decoding fails
    """
    try:
        return base64.b64decode(encoded_value).decode("utf-8")
    except Exception as e:
        raise IRPAPIError(
            f"Failed to decode base64 field '{field_name}': {e}"
        )


def decode_presign_params(presign_params: Dict[str, Any]) -> Dict[str, str]:
    """
    Decode base64 credentials from MRI import file credentials response.

    Args:
        presign_params: Response JSON containing encoded credentials

    Returns:
        Dict with decoded credential fields

    Raises:
        IRPAPIError: If required fields missing or decoding fails
    """
    required_fields = ['accessKeyId', 'secretAccessKey', 'sessionToken', 'path', 'region']
    missing = [f for f in required_fields if f not in presign_params]
    if missing:
        raise IRPAPIError(
            f"Presign params response missing fields: {', '.join(missing)}"
        )

    try:
        return {
            'aws_access_key_id': decode_base64_field(presign_params['accessKeyId'], 'accessKeyId'),
            'aws_secret_access_key': decode_base64_field(presign_params['secretAccessKey'], 'secretAccessKey'),
            'aws_session_token': decode_base64_field(presign_params['sessionToken'], 'sessionToken'),
            's3_path': decode_base64_field(presign_params['path'], 'path'),
            's3_region': decode_base64_field(presign_params['region'], 'region')
        }
    except IRPAPIError:
        raise
    except Exception as e:
        raise IRPAPIError(f"Failed to decode MRI credentials: {e}")


def _fingerprint(value: Any) -> str:
    """
    Build a stable content hash of a record or a page of records.

    Used to tell whether a search has actually advanced, without needing to
    know which field of a given record type is its identifier.

    Args:
        value: Any JSON-serializable value

    Returns:
        Hex digest of the value's canonical JSON form
    """
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, default=str).encode('utf-8')
    ).hexdigest()


def paginate_search(
    fetch: Callable[[int, int], List[Any]],
    description: str,
    limit: int = DEFAULT_PAGE_LIMIT,
    max_pages: int = MAX_PAGES
) -> List[Any]:
    """
    Page through a limit/offset search operation until it is exhausted.

    The Risk Data API documents ``offset`` two ways: the operation reference
    calls it "number of the page ... starting at 0" while the filtering guide
    describes a record offset. The two readings agree on the first request and
    disagree on every one after it, so guessing risks returning one page out of
    many while looking like a clean finish. Instead of guessing, this helper
    asks the server. Once a full first page proves there is more to fetch, it
    requests ``offset=1`` and compares the result against the first page: a
    response repeating the first page's records means ``offset`` counts records
    (rows 1..limit), while one disjoint from it means ``offset`` counts pages
    (page 1). The answer is logged and used for the rest of the walk, so a
    single call against a live tenant also settles the question for good.

    Progress is tracked by hashing page content rather than by reading record
    IDs, so a page whose records carry no recognizable identifier still ends
    the walk instead of spinning. Any page identical to one already seen stops
    it with a warning, as does exceeding ``max_pages``, as does a page larger
    than ``limit`` — that last one means the operation ignored pagination
    altogether and the first response was already complete.

    Args:
        fetch: Callable taking (limit, offset) and returning one page of results
        description: Phrase naming the search, used in log messages
        limit: Page size to request (default: 100)
        max_pages: Hard ceiling on requests before giving up (default: 1000)

    Returns:
        Every record the operation returned, in page order

    Raises:
        Whatever ``fetch`` raises, unchanged
    """
    first_page = fetch(limit, 0)

    if len(first_page) > limit:
        logger.warning(
            "%s ignored pagination (%s rows for limit %s); treating that response as complete",
            description, len(first_page), limit
        )
        return first_page
    if len(first_page) < limit:
        return first_page

    all_results: List[Any] = list(first_page)
    seen_pages = {_fingerprint(first_page)}

    # Settle what offset means before trusting any later page
    probe = fetch(limit, 1)
    if not probe:
        # Empty under either reading, so there is nothing after the first page
        return all_results

    first_page_records = {_fingerprint(record) for record in first_page}
    probe_records = {_fingerprint(record) for record in probe}
    shared = len(first_page_records & probe_records)
    offset_is_page_number = shared * 2 < len(probe_records)

    logger.info(
        "%s treats offset as %s (offset=1 shared %s of %s records with the first page)",
        description,
        "a page number" if offset_is_page_number else "a record offset",
        shared, len(probe_records)
    )

    if offset_is_page_number:
        # The probe is page 1, so keep it and resume at page 2
        all_results.extend(probe)
        seen_pages.add(_fingerprint(probe))
        if len(probe) < limit:
            return all_results
        next_page_index = 2
    else:
        # The probe overlapped the first page, so discard it; the next unseen
        # records start one full page in
        next_page_index = 1

    for page_index in range(next_page_index, max_pages):
        offset = page_index if offset_is_page_number else page_index * limit
        page = fetch(limit, offset)
        if not page:
            break

        fingerprint = _fingerprint(page)
        if fingerprint in seen_pages:
            logger.warning(
                "%s returned an already-seen page at offset %s; stopping. Results may be incomplete.",
                description, offset
            )
            break
        seen_pages.add(fingerprint)

        all_results.extend(page)

        # A short page is the last page
        if len(page) < limit:
            break
    else:
        logger.warning(
            "%s hit the %s-page ceiling at limit %s; stopping. Results may be incomplete.",
            description, max_pages, limit
        )

    return all_results


def extract_analysis_id_from_workflow_response(workflow: Dict[str, Any]) -> Optional[str]:
    """
    Extract analysis ID from workflow response.

    Args:
        workflow: Workflow response dict

    Returns:
        Analysis ID if found, None otherwise

    Raises:
        IRPAPIError: If required fields are missing from workflow response
    """
    try:
        return workflow['output']['analysisId']
    except (KeyError, TypeError) as e:
        raise IRPAPIError(
            f"Missing required field in workflow response: {e}"
        ) from e
