"""Offline contract tests for rules-based grouping inspection and submission."""

from dataclasses import replace
from types import SimpleNamespace
from typing import Any, Dict, List, Optional

import pytest

from conftest import FakeClient, FakeResponse
from irp_integration.exceptions import IRPAPIError, IRPGroupingValidationError, IRPValidationError
from irp_integration.grouping import (
    EventRateSelection,
    GroupingCurrency,
    GroupingManager,
    GroupingPartitionKey,
    GroupingSettings,
)


def analysis(
    analysis_id: int,
    *,
    framework: str = "ELT",
    engine_type: str = "DLM",
    scheme_id: Optional[int] = 101,
    scheme_name: Optional[str] = None,
    is_group: bool = False,
) -> Dict[str, Any]:
    """Build one invented analysis-detail fixture."""
    return {
        "analysisId": analysis_id,
        "analysisFramework": framework,
        "engineType": "Group" if is_group else engine_type,
        "isGroup": is_group,
        "engineVersion": "HDv2.0" if framework == "PLT" else "RL23",
        "perilCode": "WF" if framework == "PLT" else "WS",
        "regionCode": "NA",
        "eventRateSchemeId": scheme_id,
        "eventRateSchemeName": scheme_name,
    }


def region(
    analysis_id: int,
    *,
    framework: str = "ELT",
    scheme_id: Optional[int] = 101,
    pet_id: Optional[int] = None,
    periods: Optional[int] = None,
    sub_region: str = "FL",
    apply_contract: bool = False,
) -> Dict[str, Any]:
    """Build one invented analysis-region fixture."""
    return {
        "analysisId": analysis_id,
        "framework": framework,
        "engineVersion": "HDv2.0" if framework == "PLT" else "RL23",
        "peril": "WF" if framework == "PLT" else "WS",
        "region": "NA",
        "subRegion": sub_region,
        "rateSchemeId": scheme_id,
        "petId": pet_id,
        "periods": periods,
        "applyContractFlag": apply_contract,
    }


class FakeAnalysisManager:
    """Return mutable analysis and region fixtures by exact ID."""

    def __init__(self, details: Dict[int, Dict[str, Any]], regions: Dict[int, List[Dict[str, Any]]]):
        self.details = details
        self.regions = regions

    def get_analysis_by_id(self, analysis_id: int) -> Dict[str, Any]:
        """Return an exact-ID detail response."""
        return self.details.get(analysis_id, {})

    def get_regions(self, analysis_id: int) -> List[Dict[str, Any]]:
        """Return exact-ID region rows."""
        return self.regions.get(analysis_id, [])


class FakeReferenceDataManager:
    """Resolve fixture model versions, PETs, and exact simulation mappings."""

    def __init__(self) -> None:
        self.pet_metadata = {
            50: {
                "id": 50,
                "modelRegionCode": "NAWF",
                "modelVersionCode": "2.0",
                "numberOfPeriods": 100000,
            },
            51: {
                "id": 51,
                "modelRegionCode": "NAWF",
                "modelVersionCode": "2.0",
                "numberOfPeriods": 100000,
            },
            60: {
                "id": 60,
                "modelRegionCode": "USEQ",
                "modelVersionCode": "23.0",
                "numberOfPeriods": 50000,
            },
        }
        self.simulation_error: Optional[IRPAPIError] = None
        self.simulation_calls: List[Dict[str, Any]] = []
        self.model_version_error: Optional[IRPAPIError] = None

    def get_model_version_by_engine_region_peril(
        self, engine_version: str, region_code: str, peril_code: str
    ) -> str:
        """Return the exact fixture model version."""
        if self.model_version_error:
            raise self.model_version_error
        return "11.0" if engine_version == "RL23" else "2.0"

    def get_pet_metadata_by_id(self, pet_id: int) -> Dict[str, Any]:
        """Return one PET fixture or an exact-lookup failure."""
        if pet_id not in self.pet_metadata:
            raise IRPAPIError(f"No PET metadata found for PET ID {pet_id}")
        return self.pet_metadata[pet_id]

    def get_simulation_set_exact(self, **kwargs: Any) -> Dict[str, Any]:
        """Return one exact simulation mapping for each offered scheme."""
        self.simulation_calls.append(kwargs)
        if self.simulation_error:
            raise self.simulation_error
        scheme_id = kwargs["event_rate_scheme_id"]
        return {
            "id": 900 + scheme_id,
            "eventRateSchemeId": scheme_id,
            "modelRegionCode": kwargs["model_region_code"],
            "modelVersionCode": kwargs["model_version"],
            "defaultPeriods": 100000,
        }


def make_manager(
    details: Dict[int, Dict[str, Any]],
    regions: Dict[int, List[Dict[str, Any]]],
    *,
    post: bool = False,
) -> tuple[GroupingManager, FakeClient, FakeReferenceDataManager]:
    """Build a grouping manager over offline test doubles."""
    responses = []
    if post:
        responses.append(FakeResponse(
            201,
            headers={"location": "/platform/grouping/v1/jobs/7001"},
            has_body=False,
        ))
    client = FakeClient(responses)
    reference_data = FakeReferenceDataManager()
    irp = SimpleNamespace(
        client=client,
        analysis=FakeAnalysisManager(details, regions),
        reference_data=reference_data,
    )
    return GroupingManager(irp), client, reference_data


def settings() -> GroupingSettings:
    """Return explicit caller settings with no optional fields."""
    return GroupingSettings(
        analysis_name="example_group",
        currency=GroupingCurrency(
            code="USD", scheme="RMS", vintage="RL25", as_of_date="2026-01-01"
        ),
        propagate_detailed_losses=True,
        num_of_simulations=1,
    )


def pure_elt_fixtures(conflicting: bool = False):
    """Return two pure-ELT members in one partition."""
    second_scheme = 102 if conflicting else 101
    details = {
        1: analysis(1, scheme_id=101, scheme_name="Historical"),
        2: analysis(2, scheme_id=second_scheme, scheme_name="Stochastic"),
    }
    regions = {
        1: [region(1, scheme_id=101)],
        2: [region(2, scheme_id=second_scheme)],
    }
    return details, regions


def test_pure_elt_inspection_reports_one_observed_scheme():
    """Classify pure ELT without inventing an event-rate choice."""
    manager, client, _ = make_manager(*pure_elt_fixtures())

    result = manager.inspect(analysis_ids=[1, 2])

    assert result.output_loss_table == "ELT"
    assert result.simulate_to_plt is False
    assert result.blocking_problems == ()
    assert result.partitions[0].event_rate_selection_required is False
    assert [option.event_rate_scheme_id for option in result.partitions[0].event_rate_scheme_options] == [101]
    assert client.calls == []


def test_conflicting_pure_elt_requires_one_of_the_observed_schemes():
    """Return both observed choices without selecting one."""
    manager, _, _ = make_manager(*pure_elt_fixtures(conflicting=True))

    result = manager.inspect(analysis_ids=[1, 2])

    partition = result.partitions[0]
    assert partition.event_rate_selection_required is True
    assert [option.event_rate_scheme_id for option in partition.event_rate_scheme_options] == [101, 102]


def test_display_name_region_rows_resolve_to_the_detail_codes():
    """Platform region rows carry ``peril`` as a display name and the detail
    lists the scheme name under ``eventRateSchemeNames``; both members must
    land in one coded partition with labelled choices and no problems."""
    details, regions = pure_elt_fixtures(conflicting=True)
    for detail in details.values():
        detail.update({
            "peril": "Windstorm",
            "region": "North Atlantic (including Hawaii)",
            "eventRateSchemeName": None,
            "eventRateSchemeNames": [
                {"id": 0, "code": "0", "name": f"Scheme {detail['eventRateSchemeId']}"}
            ],
        })
    for rows in regions.values():
        for row in rows:
            row["peril"] = "Windstorm"
    manager, _, _ = make_manager(details, regions)

    result = manager.inspect(analysis_ids=[1, 2])

    assert result.blocking_problems == ()
    assert [partition.key for partition in result.partitions] == [
        GroupingPartitionKey("WS", "NA", "11.0")
    ]
    assert [
        (option.event_rate_scheme_id, option.label)
        for option in result.partitions[0].event_rate_scheme_options
    ] == [(101, "Scheme 101"), (102, "Scheme 102")]


def test_reversing_members_keeps_partition_and_choice_order():
    """Normalize partitions independently of member order."""
    manager, _, _ = make_manager(*pure_elt_fixtures(conflicting=True))

    forward = manager.inspect(analysis_ids=[1, 2])
    reverse = manager.inspect(analysis_ids=[2, 1])

    assert forward.partitions == reverse.partitions


def test_conflicting_pure_elt_submission_emits_verified_zero_simulation_fields():
    """Use explicit zeros only for the documented pure-ELT RPS encoding."""
    manager, client, _ = make_manager(*pure_elt_fixtures(conflicting=True), post=True)
    inspection = manager.inspect(analysis_ids=[1, 2])
    partition = inspection.partitions[0].key

    result = manager.submit(
        analysis_ids=[1, 2],
        settings=settings(),
        event_rate_selections=[EventRateSelection(partition, 102)],
        expected_inspection_fingerprint=inspection.fingerprint,
    )

    assert result.job_id == 7001
    assert result.request_body == client.calls[-1]["json"]
    request_settings = result.request_body["settings"]
    assert request_settings["simulateToPLT"] is False
    assert "description" not in request_settings
    assert "reportingWindowStart" not in request_settings
    assert request_settings["regionPerilSimulationSet"] == [{
        "engineVersion": "RL23",
        "eventRateSchemeId": 102,
        "modelRegionCode": "FLWS",
        "modelVersion": "11.0",
        "perilCode": "WS",
        "regionCode": "NA",
        "simulationPeriods": 0,
        "simulationSetId": 0,
    }]


@pytest.mark.parametrize(
    ("selections", "code"),
    [
        ([], "event_rate_selection_missing"),
        ([EventRateSelection(GroupingPartitionKey("WS", "NA", "11.0"), 999)],
         "event_rate_selection_not_offered"),
        ([EventRateSelection(GroupingPartitionKey("EQ", "US", "23.0"), 101)],
         "event_rate_selection_unknown_partition"),
    ],
)
def test_invalid_event_rate_selection_blocks_post(selections, code):
    """Return stable selection problems and perform no Platform POST."""
    manager, client, _ = make_manager(*pure_elt_fixtures(conflicting=True))
    inspection = manager.inspect(analysis_ids=[1, 2])

    with pytest.raises(IRPGroupingValidationError) as raised:
        manager.submit(
            analysis_ids=[1, 2],
            settings=settings(),
            event_rate_selections=selections,
            expected_inspection_fingerprint=inspection.fingerprint,
        )

    assert code in {problem.code for problem in raised.value.problems}
    assert client.calls == []


def test_duplicate_event_rate_selection_is_structured():
    """Reject duplicate choices for the same conflicting partition."""
    manager, client, _ = make_manager(*pure_elt_fixtures(conflicting=True))
    inspection = manager.inspect(analysis_ids=[1, 2])
    key = inspection.partitions[0].key

    with pytest.raises(IRPGroupingValidationError) as raised:
        manager.submit(
            analysis_ids=[1, 2],
            settings=settings(),
            event_rate_selections=[EventRateSelection(key, 101), EventRateSelection(key, 102)],
            expected_inspection_fingerprint=inspection.fingerprint,
        )

    assert "event_rate_selection_duplicate" in {p.code for p in raised.value.problems}
    assert client.calls == []


def test_fingerprint_ignores_timestamp_and_display_label():
    """Exclude inspection time and event-rate display labels from the fingerprint."""
    details, regions = pure_elt_fixtures(conflicting=True)
    manager, _, _ = make_manager(details, regions)
    first = manager.inspect(analysis_ids=[1, 2])
    details[1]["eventRateSchemeName"] = "Renamed label"

    second = manager.inspect(analysis_ids=[1, 2])

    assert first.inspected_at != second.inspected_at
    assert first.fingerprint == second.fingerprint


def test_changed_scheme_rejects_submission_before_post():
    """Detect a request-affecting reinspection change."""
    details, regions = pure_elt_fixtures(conflicting=True)
    manager, client, _ = make_manager(details, regions)
    inspection = manager.inspect(analysis_ids=[1, 2])
    details[2]["eventRateSchemeId"] = 103
    regions[2][0]["rateSchemeId"] = 103

    with pytest.raises(IRPGroupingValidationError) as raised:
        manager.submit(
            analysis_ids=[1, 2],
            settings=settings(),
            event_rate_selections=[],
            expected_inspection_fingerprint=inspection.fingerprint,
        )

    assert [problem.code for problem in raised.value.problems] == ["inspection_changed"]
    assert client.calls == []


def mixed_fixtures():
    """Return one ELT and one PLT member in the same partition."""
    details = {
        1: analysis(1, scheme_id=101),
        2: analysis(2, framework="PLT", engine_type="HD", scheme_id=None),
    }
    regions = {
        1: [region(1, scheme_id=101)],
        2: [region(2, framework="PLT", scheme_id=None, pet_id=50, periods=100000)],
    }
    return details, regions


def test_mixed_elt_plt_uses_exact_simulation_mapping_and_pet():
    """Build a group PLT without replacing the selected scheme or member PET."""
    manager, client, _ = make_manager(*mixed_fixtures(), post=True)
    inspection = manager.inspect(analysis_ids=[1, 2])

    result = manager.submit(
        analysis_ids=[1, 2],
        settings=settings(),
        event_rate_selections=[],
        expected_inspection_fingerprint=inspection.fingerprint,
    )

    assert inspection.output_loss_table == "PLT"
    rps = result.request_body["settings"]["regionPerilSimulationSet"]
    assert {entry["simulationSetId"] for entry in rps} == {50, 1001}
    assert {entry["eventRateSchemeId"] for entry in rps} == {0, 101}
    assert client.calls[-1]["json"] == result.request_body


def test_differing_pet_ids_in_one_partition_block():
    """Report the affected partition and PET IDs without choosing a replacement."""
    details = {
        1: analysis(1, framework="PLT", engine_type="HD", scheme_id=None),
        2: analysis(2, framework="PLT", engine_type="HD", scheme_id=None),
    }
    regions = {
        1: [region(1, framework="PLT", scheme_id=None, pet_id=50, periods=100000)],
        2: [region(2, framework="PLT", scheme_id=None, pet_id=51, periods=100000)],
    }
    manager, _, _ = make_manager(details, regions)

    inspection = manager.inspect(analysis_ids=[1, 2])

    problem = next(p for p in inspection.blocking_problems if p.code == "differing_pet_ids_unsupported")
    assert problem.pet_ids == (50, 51)
    assert problem.partition == GroupingPartitionKey("WF", "NA", "2.0")


def test_different_pet_ids_across_different_partitions_submit():
    """Permit exact PET mappings that belong to different documented partitions."""
    details = {
        1: analysis(1, framework="PLT", engine_type="HD", scheme_id=None),
        2: analysis(2, framework="PLT", engine_type="HD", scheme_id=None),
    }
    second_region = region(
        2, framework="PLT", scheme_id=None, pet_id=60, periods=50000,
    )
    second_region.update({"peril": "EQ", "region": "US", "subRegion": "CA"})
    regions = {
        1: [region(1, framework="PLT", scheme_id=None, pet_id=50, periods=100000)],
        2: [second_region],
    }
    manager, _, _ = make_manager(details, regions, post=True)
    inspection = manager.inspect(analysis_ids=[1, 2])

    result = manager.submit(
        analysis_ids=[1, 2],
        settings=settings(),
        event_rate_selections=[],
        expected_inspection_fingerprint=inspection.fingerprint,
    )

    assert inspection.blocking_problems == ()
    assert {partition.observed_pet_ids for partition in inspection.partitions} == {(50,), (60,)}
    assert {entry["simulationSetId"] for entry in result.request_body["settings"]["regionPerilSimulationSet"]} == {50, 60}


def test_nested_group_requires_one_output_pet_per_partition():
    """Accept one nested output PET and identify an ambiguous nested partition."""
    details = {
        1: analysis(1, framework="PLT", engine_type="HD", scheme_id=None, is_group=True),
        2: analysis(2, framework="PLT", engine_type="HD", scheme_id=None),
    }
    regions = {
        1: [region(1, framework="PLT", scheme_id=None, pet_id=50, periods=100000)],
        2: [region(2, framework="PLT", scheme_id=None, pet_id=50, periods=100000)],
    }
    manager, _, _ = make_manager(details, regions)
    supported = manager.inspect(analysis_ids=[1, 2])
    assert supported.blocking_problems == ()

    regions[1].append(
        region(1, framework="PLT", scheme_id=None, pet_id=51, periods=100000)
    )
    ambiguous = manager.inspect(analysis_ids=[1, 2])

    problem = next(p for p in ambiguous.blocking_problems if p.code == "nested_group_pet_ambiguous")
    assert problem.analysis_ids == (1,)
    assert problem.pet_ids == (50, 51)


def test_apply_contract_flag_blocks_plt_member():
    """Block an HD/PLT member that applies contract dates."""
    details, regions = mixed_fixtures()
    regions[2][0]["applyContractFlag"] = True
    manager, _, _ = make_manager(details, regions)

    inspection = manager.inspect(analysis_ids=[1, 2])

    assert "apply_contract_flag_unsupported" in {p.code for p in inspection.blocking_problems}


def test_missing_plt_region_periods_block_even_when_pet_has_default_periods():
    """Do not replace a missing source PLT length with PET reference periods."""
    details, regions = mixed_fixtures()
    regions[2][0]["periods"] = None
    manager, _, _ = make_manager(details, regions)

    inspection = manager.inspect(analysis_ids=[1, 2])

    assert "pet_periods_missing" in {p.code for p in inspection.blocking_problems}


def test_simulation_reference_lookup_is_reused_across_subregions():
    """Read one exact broad-region mapping for all matching source subregions."""
    details, regions = mixed_fixtures()
    regions[1].append(region(1, scheme_id=101, sub_region="TX"))
    manager, _, reference_data = make_manager(details, regions)

    inspection = manager.inspect(analysis_ids=[1, 2])

    assert inspection.blocking_problems == ()
    assert len(inspection.simulation_mappings) == 2
    assert len(reference_data.simulation_calls) == 1


def test_missing_member_and_region_data_have_stable_codes():
    """Do not drop requested members or silently accept empty region data."""
    details = {1: analysis(1), 2: {}}
    regions = {1: [], 2: []}
    manager, _, _ = make_manager(details, regions)

    inspection = manager.inspect(analysis_ids=[1, 2])

    assert inspection.analysis_ids == (1, 2)
    assert {p.code for p in inspection.blocking_problems} == {
        "member_not_found", "member_region_data_missing"
    }


def test_contradictory_elt_detail_and_region_metadata_blocks():
    """Do not silently choose between conflicting analysis and region facts."""
    details, regions = pure_elt_fixtures()
    regions[2][0]["region"] = "EU"
    manager, _, _ = make_manager(details, regions)

    inspection = manager.inspect(analysis_ids=[1, 2])

    assert "member_metadata_conflict" in {p.code for p in inspection.blocking_problems}


def test_missing_exact_simulation_mapping_blocks_mixed_group():
    """Treat zero exact simulation-set matches as a blocking inspection problem."""
    manager, _, reference_data = make_manager(*mixed_fixtures())
    reference_data.simulation_error = IRPAPIError("No simulation set found")

    inspection = manager.inspect(analysis_ids=[1, 2])

    assert "simulation_set_mapping_missing" in {p.code for p in inspection.blocking_problems}


def test_ambiguous_exact_simulation_mapping_blocks_mixed_group():
    """Treat multiple exact simulation-set rows as a distinct blocking problem."""
    manager, _, reference_data = make_manager(*mixed_fixtures())
    reference_data.simulation_error = IRPAPIError("Multiple simulation sets found")

    inspection = manager.inspect(analysis_ids=[1, 2])

    assert "simulation_set_mapping_ambiguous" in {p.code for p in inspection.blocking_problems}


@pytest.mark.parametrize(
    ("message", "code"),
    [
        ("No model version mapping found", "model_version_mapping_missing"),
        ("Multiple model version mappings found", "model_version_mapping_ambiguous"),
    ],
)
def test_model_version_cardinality_problem_is_structured(message, code):
    """Distinguish missing and ambiguous model-version mappings."""
    manager, _, reference_data = make_manager(*pure_elt_fixtures())
    reference_data.model_version_error = IRPAPIError(message)

    inspection = manager.inspect(analysis_ids=[1, 2])

    assert code in {p.code for p in inspection.blocking_problems}


def test_fingerprint_ignores_irrelevant_region_response_order():
    """Normalize region rows before computing the concurrency fingerprint."""
    details, regions = pure_elt_fixtures()
    regions[1].append(region(1, scheme_id=101, sub_region="TX"))
    regions[2].append(region(2, scheme_id=101, sub_region="TX"))
    manager, _, _ = make_manager(details, regions)
    first = manager.inspect(analysis_ids=[1, 2])
    regions[1].reverse()
    regions[2].reverse()

    second = manager.inspect(analysis_ids=[1, 2])

    assert first.fingerprint == second.fingerprint


@pytest.mark.parametrize(
    "invalid_settings",
    [
        replace(settings(), analysis_name=""),
        replace(settings(), num_of_simulations=0),
        replace(settings(), propagate_detailed_losses=1),
        replace(settings(), currency=replace(settings().currency, code="")),
    ],
)
def test_every_former_default_is_required(invalid_settings):
    """Reject absent-equivalent settings before reinspection or submission."""
    manager, client, _ = make_manager(*pure_elt_fixtures())

    with pytest.raises(IRPValidationError):
        manager.submit(
            analysis_ids=[1, 2],
            settings=invalid_settings,
            event_rate_selections=[],
            expected_inspection_fingerprint="v1:example",
        )

    assert client.calls == []


def test_empty_expected_fingerprint_is_direct_validation_error():
    """Require the concurrency token with no omission behavior."""
    manager, client, _ = make_manager(*pure_elt_fixtures())

    with pytest.raises(IRPValidationError):
        manager.submit(
            analysis_ids=[1, 2],
            settings=settings(),
            event_rate_selections=[],
            expected_inspection_fingerprint="",
        )

    assert client.calls == []


@pytest.mark.parametrize("analysis_ids", [[], [1], [1, 1], [1, 0], [1, True]])
def test_malformed_analysis_ids_raise_direct_validation_error(analysis_ids):
    """Reject malformed analysis IDs before reading Platform data."""
    manager, _, _ = make_manager({}, {})

    with pytest.raises(IRPValidationError):
        manager.inspect(analysis_ids=analysis_ids)


def test_get_job_uses_grouping_job_endpoint():
    """Expose grouping job status through GroupingManager."""
    manager, client, _ = make_manager({}, {})
    client.responses.append(FakeResponse(200, json_body={"id": 77, "status": "RUNNING"}))

    result = manager.get_job(job_id=77)

    assert result == {"id": 77, "status": "RUNNING"}
    assert client.calls[0]["path"] == "/platform/grouping/v1/jobs/77"
