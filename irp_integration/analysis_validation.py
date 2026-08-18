"""
Analysis classification and validation helpers.

The functions in this module operate only on values already retrieved from
Risk Modeler. They do not make API requests or require an ``IRPClient``.
"""

from typing import Literal, Optional


def classify_model_profile(software_version_code: str) -> Literal["DLM", "HD"]:
    """
    Classify a model profile as DLM or HD.

    Args:
        software_version_code: Model profile software version code.

    Returns:
        ``"HD"`` when ``software_version_code`` contains ``"HD"``;
        otherwise, ``"DLM"``.
    """
    if "HD" in software_version_code:
        return "HD"
    return "DLM"


def validate_analysis_settings(
    software_version_code: str,
    scheme_provided: bool,
    profile_peril_code: str,
    profile_model_region_code: str,
    scheme_peril_code: Optional[str] = None,
    scheme_model_region_code: Optional[str] = None,
) -> list[str]:
    """
    Validate event-rate-scheme settings for a model profile.

    Pair validation is skipped when either event-rate-scheme code is unknown.

    Args:
        software_version_code: Model profile software version code.
        scheme_provided: Whether an event-rate-scheme name was supplied.
        profile_peril_code: Model profile ``perilCode``.
        profile_model_region_code: Model profile ``modelRegionCode``.
        scheme_peril_code: Event rate scheme ``perilCode``, if known.
        scheme_model_region_code: Event rate scheme ``modelRegionCode``, if known.

    Returns:
        Validation error messages. An empty list means the settings are valid.
    """
    errors: list[str] = []

    if classify_model_profile(software_version_code) == "DLM" and not scheme_provided:
        errors.append("Event rate scheme is required for DLM analyses")

    scheme_pair_known = (
        scheme_peril_code is not None
        and scheme_model_region_code is not None
    )
    if (
        scheme_provided
        and scheme_pair_known
        and (
            scheme_peril_code != profile_peril_code
            or scheme_model_region_code != profile_model_region_code
        )
    ):
        errors.append(
            "Event rate scheme peril/region "
            f"({scheme_peril_code}, {scheme_model_region_code}) does not match "
            "model profile peril/region "
            f"({profile_peril_code}, {profile_model_region_code})"
        )

    return errors
