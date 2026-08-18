"""Tests for public analysis classification and validation helpers."""

import pytest

from irp_integration.analysis_validation import (
    classify_model_profile,
    validate_analysis_settings,
)


@pytest.mark.parametrize(
    ("software_version_code", "expected"),
    [
        *[(f"RL{version}", "DLM") for version in range(18, 26)],
        ("HDv1.0", "HD"),
        ("HDv2.0", "HD"),
        ("HDv3.0", "HD"),
        ("Open", "DLM"),
    ],
)
def test_classify_model_profile(software_version_code, expected):
    """Classify the observed Risk Modeler software version codes."""
    assert classify_model_profile(software_version_code) == expected


@pytest.mark.parametrize("software_version_code", ["RL18", "RL25", "Open"])
def test_dlm_requires_event_rate_scheme(software_version_code):
    """Require an event rate scheme for every DLM software version."""
    assert validate_analysis_settings(
        software_version_code,
        scheme_provided=False,
        profile_peril_code="WS",
        profile_model_region_code="NAWS",
    ) == ["Event rate scheme is required for DLM analyses"]


@pytest.mark.parametrize("scheme_provided", [False, True])
def test_event_rate_scheme_is_optional_for_hd(scheme_provided):
    """Allow an HD analysis with or without an event rate scheme."""
    assert validate_analysis_settings(
        "HDv3.0",
        scheme_provided=scheme_provided,
        profile_peril_code="WS",
        profile_model_region_code="NAWS",
    ) == []


def test_matching_event_rate_scheme_pair_is_valid():
    """Accept matching model profile and event rate scheme pairs."""
    assert validate_analysis_settings(
        "RL25",
        scheme_provided=True,
        profile_peril_code="WS",
        profile_model_region_code="NAWS",
        scheme_peril_code="WS",
        scheme_model_region_code="NAWS",
    ) == []


def test_mismatched_event_rate_scheme_pair_returns_error():
    """Name both pairs when an event rate scheme does not match."""
    assert validate_analysis_settings(
        "RL25",
        scheme_provided=True,
        profile_peril_code="WS",
        profile_model_region_code="NAWS",
        scheme_peril_code="EQ",
        scheme_model_region_code="USEQ",
    ) == [
        "Event rate scheme peril/region (EQ, USEQ) does not match "
        "model profile peril/region (WS, NAWS)"
    ]


@pytest.mark.parametrize(
    ("scheme_peril_code", "scheme_model_region_code"),
    [(None, "NAWS"), ("WS", None), (None, None)],
)
def test_unknown_event_rate_scheme_pair_skips_pair_validation(
    scheme_peril_code,
    scheme_model_region_code,
):
    """Skip pair validation when either event rate scheme code is unknown."""
    assert validate_analysis_settings(
        "RL25",
        scheme_provided=True,
        profile_peril_code="WS",
        profile_model_region_code="NAWS",
        scheme_peril_code=scheme_peril_code,
        scheme_model_region_code=scheme_model_region_code,
    ) == []
