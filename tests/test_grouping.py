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
    GroupingTreaty,
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
    """Return mutable analysis, region, and treaty fixtures by exact ID."""

    def __init__(
        self,
        details: Dict[int, Dict[str, Any]],
        regions: Dict[int, List[Dict[str, Any]]],
        treaties: Optional[Dict[int, List[Dict[str, Any]]]] = None,
    ):
        self.details = details
        self.regions = regions
        self.treaties = treaties or {}

    def get_analysis_by_id(self, analysis_id: int) -> Dict[str, Any]:
        """Return an exact-ID detail response."""
        return self.details.get(analysis_id, {})

    def get_regions(self, analysis_id: int) -> List[Dict[str, Any]]:
        """Return exact-ID region rows."""
        return self.regions.get(analysis_id, [])

    def search_analysis_treaties_paginated(
        self, analysis_id: int
    ) -> List[Dict[str, Any]]:
        """Return every treaty associated with one analysis ID."""
        return self.treaties.get(analysis_id, [])


class FakeReferenceDataManager:
    """Resolve fixture model versions, PETs, event-rate schemes, and exact
    simulation mappings."""

    def __init__(self) -> None:
        self.event_rate_schemes = [
            {"eventRateSchemeId": 101, "eventRateSchemeName": "Historical"},
            {"eventRateSchemeId": 102, "eventRateSchemeName": "Stochastic"},
        ]
        self.pet_metadata = [
            {
                "id": 50,
                "modelRegionCode": "NAWF",
                "modelVersionCode": "2.0",
                "numberOfPeriods": 100000,
            },
            {
                "id": 51,
                "modelRegionCode": "NAWF",
                "modelVersionCode": "2.0",
                "numberOfPeriods": 100000,
            },
            {
                "id": 60,
                "modelRegionCode": "USEQ",
                "modelVersionCode": "23.0",
                "numberOfPeriods": 50000,
            },
            {"id": 15, "modelRegionCode": "JPWS", "modelVersionCode": "2.0"},
            {"id": 16, "modelRegionCode": "JPWS", "modelVersionCode": "2.0"},
            {"id": 15, "modelRegionCode": "JPWS", "modelVersionCode": "2.1"},
            {"id": 16, "modelRegionCode": "JPWS", "modelVersionCode": "2.1"},
        ]
        self.pet_metadata_error: Optional[IRPAPIError] = None
        self.pet_metadata_calls: List[Dict[str, Any]] = []
        self.simulation_error: Optional[IRPAPIError] = None
        self.simulation_calls: List[Dict[str, Any]] = []
        self.model_version_error: Optional[IRPAPIError] = None

    def get_event_rate_schemes(self) -> Dict[str, Any]:
        """Return the fixture scheme list in the Platform's envelope."""
        return {"items": list(self.event_rate_schemes)}

    def get_model_version_by_engine_region_peril(
        self, engine_version: str, region_code: str, peril_code: str
    ) -> str:
        """Return the exact fixture model version."""
        if self.model_version_error:
            raise self.model_version_error
        if engine_version == "RL23":
            return "11.0"
        if region_code == "US" and peril_code == "EQ":
            return "23.0"
        return "2.1" if engine_version == "HDv2.1" else "2.0"

    def get_pet_metadata_by_id(self, pet_id: int) -> Dict[str, Any]:
        """Preserve strict lookup behavior in the test double."""
        matches = [pet for pet in self.pet_metadata if pet["id"] == pet_id]
        if not matches:
            raise IRPAPIError(f"No PET metadata found for PET ID {pet_id}")
        if len(matches) > 1:
            raise IRPAPIError(f"Multiple PET metadata rows found for PET ID {pet_id}")
        return matches[0]

    def get_pet_metadata_exact(self, **kwargs: Any) -> Dict[str, Any]:
        """Return one PET fixture matching every supplied qualifier."""
        self.pet_metadata_calls.append(kwargs)
        if self.pet_metadata_error:
            raise self.pet_metadata_error
        matches = [
            pet for pet in self.pet_metadata
            if pet["id"] == kwargs["pet_id"]
            and pet["modelVersionCode"] == kwargs["model_version"]
            and (
                kwargs["model_region_code"] is None
                or pet["modelRegionCode"] == kwargs["model_region_code"]
            )
        ]
        if not matches:
            raise IRPAPIError("No PET metadata found")
        if len(matches) > 1:
            raise IRPAPIError("Multiple PET metadata rows found")
        return matches[0]

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
    treaties: Optional[Dict[int, List[Dict[str, Any]]]] = None,
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
        analysis=FakeAnalysisManager(details, regions, treaties),
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


def treaty(
    analysis_id: int,
    treaty_id: int,
    *,
    treaty_number: str = "CATA-1",
    occurrence_limit: float = 1_000_000,
) -> Dict[str, Any]:
    """Build one Platform analysis-treaty fixture."""
    return {
        "analysisId": analysis_id,
        "treatyId": treaty_id,
        "treatyNumber": treaty_number,
        "treatyName": f"Display name {treaty_id}",
        "cedant": {"cedantId": "CED-1", "cedantName": "Example Cedant"},
        "producer": {"producerId": str(treaty_id), "producerName": "Producer"},
        "treatyType": "CATA",
        "currency": {"id": 1, "code": "USD", "name": "US Dollar"},
        "attachmentBasis": "L",
        "attachmentLevel": "PORT",
        "premium": treaty_id * 1000,
        "occurrenceLimit": occurrence_limit,
        "attachmentPoint": 100_000,
        "riskLimit": None,
        "retentionAmount": None,
        "percentagePlaced": 100,
        "effectiveDate": "2026-01-01T00:00:00.000Z",
        "expirationDate": "2026-12-31T00:00:00.000Z",
        "percentageRetention": 100,
        "percentageRiShare": 100,
        "percentageCovered": 100,
        "priority": 1,
        "numberOfReinstatements": 1,
        "reinstatementCharge": 0,
        "maolAmount": None,
        "isValid": True,
        "userId1": f"user-{treaty_id}",
        "userId2": None,
        "aggregateDeductible": 0,
        "aggregateLimit": 0,
        "uri": f"/analyses/{analysis_id}/treaties/{treaty_id}",
        "lobs": [{"lobId": 9, "lobName": "FACTORY", "uri": f"/lobs/{treaty_id}"}],
        "lossOccurrences": [{
            "id": treaty_id,
            "treatyId": treaty_id,
            "uri": f"/loss-occurrences/{treaty_id}",
            "regionPeril": {"id": 4, "code": "NAWF", "name": "Wildfire"},
            "lossOccurrenceTime": 168,
            "lossOccurrenceRadius": 0,
            "radiusUnit": {"id": 1, "code": "MI", "name": "Miles"},
            "multiLossOccurrence": {"id": 2, "code": "NO", "name": "No"},
        }],
        "tagIds": [treaty_id],
    }


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


def test_matching_treaty_terms_ignore_non_loss_properties():
    """Do not warn for IDs, names, premiums, producers, tags, or URIs."""
    details, regions = pure_elt_fixtures()
    manager, _, _ = make_manager(
        details,
        regions,
        treaties={1: [treaty(1, 11)], 2: [treaty(2, 22)]},
    )

    inspection = manager.inspect(analysis_ids=[1, 2])

    assert inspection.warnings == ()
    assert inspection.blocking_problems == ()


def test_inconsistent_treaty_terms_return_structured_warning():
    """Return each compared analysis treaty and its differing term value."""
    details, regions = pure_elt_fixtures()
    manager, _, _ = make_manager(
        details,
        regions,
        treaties={
            1: [treaty(1, 11)],
            2: [treaty(2, 22, occurrence_limit=2_000_000)],
        },
    )

    inspection = manager.inspect(analysis_ids=[1, 2])

    assert inspection.blocking_problems == ()
    assert len(inspection.warnings) == 1
    warning = inspection.warnings[0]
    assert warning.code == "inconsistent_treaty_terms"
    assert warning.analysis_ids == (1, 2)
    assert warning.treaty_numbers == ("CATA-1",)
    assert warning.treaty_ids == (11, 22)
    assert warning.differing_fields == ("occurrenceLimit",)
    assert [
        (row.analysis_id, row.treaty_id, row.treaty_number)
        for row in warning.treaties
    ] == [(1, 11, "CATA-1"), (2, 22, "CATA-1")]
    assert [row.terms["occurrenceLimit"] for row in warning.treaties] == [
        1_000_000,
        2_000_000,
    ]
    assert all(isinstance(row, GroupingTreaty) for row in warning.treaties)


def test_inconsistent_treaty_currency_returns_normalized_codes():
    """Return normalized currency codes that explain a currency mismatch."""
    details, regions = pure_elt_fixtures()
    first = treaty(1, 11)
    second = treaty(2, 22)
    second["currency"] = {"id": 2, "code": "CAD", "name": "Canadian Dollar"}
    manager, _, _ = make_manager(
        details,
        regions,
        treaties={1: [first], 2: [second]},
    )

    warning = manager.inspect(analysis_ids=[1, 2]).warnings[0]

    assert warning.differing_fields == ("currency",)
    assert [row.terms["currency"] for row in warning.treaties] == ["USD", "CAD"]


def test_inconsistent_treaty_rows_are_sorted_by_analysis_and_treaty_id():
    """Sort three compared analysis treaties by analysis ID and treaty ID."""
    details, regions = pure_elt_fixtures()
    details[3] = analysis(3, scheme_id=101)
    regions[3] = [region(3, scheme_id=101)]
    manager, _, _ = make_manager(
        details,
        regions,
        treaties={
            1: [treaty(1, 31)],
            2: [treaty(2, 22, occurrence_limit=2_000_000)],
            3: [treaty(3, 13)],
        },
    )

    warning = manager.inspect(analysis_ids=[3, 1, 2]).warnings[0]

    assert [
        (row.analysis_id, row.treaty_id) for row in warning.treaties
    ] == [(1, 31), (2, 22), (3, 13)]


def test_inconsistent_treaty_row_keeps_missing_treaty_id():
    """Return a compared analysis treaty whose treatyId is absent."""
    details, regions = pure_elt_fixtures()
    first = treaty(1, 11)
    first.pop("treatyId")
    manager, _, _ = make_manager(
        details,
        regions,
        treaties={
            1: [first],
            2: [treaty(2, 22, occurrence_limit=2_000_000)],
        },
    )

    warning = manager.inspect(analysis_ids=[1, 2]).warnings[0]

    assert warning.treaty_ids == (22,)
    assert [(row.analysis_id, row.treaty_id) for row in warning.treaties] == [
        (1, None),
        (2, 22),
    ]


def test_different_treaty_numbers_are_not_compared():
    """Compare terms only when Treaty Number identifies the same treaty."""
    details, regions = pure_elt_fixtures()
    manager, _, _ = make_manager(
        details,
        regions,
        treaties={
            1: [treaty(1, 11, treaty_number="CATA-1")],
            2: [
                treaty(
                    2,
                    22,
                    treaty_number="CATA-2",
                    occurrence_limit=2_000_000,
                )
            ],
        },
    )

    inspection = manager.inspect(analysis_ids=[1, 2])

    assert inspection.warnings == ()


def test_loss_occurrence_and_lob_differences_are_treaty_warnings():
    """Compare nested loss-occurrence terms and LOB assignments."""
    details, regions = pure_elt_fixtures()
    first = treaty(1, 11)
    second = treaty(2, 22)
    second["lobs"] = [{"lobId": 10, "lobName": "OFFICE"}]
    second["lossOccurrences"][0]["lossOccurrenceTime"] = 72
    manager, _, _ = make_manager(
        details,
        regions,
        treaties={1: [first], 2: [second]},
    )

    inspection = manager.inspect(analysis_ids=[1, 2])

    assert inspection.warnings[0].differing_fields == ("lobs", "lossOccurrences")


def test_inconsistent_treaty_warning_does_not_block_submission():
    """Submit when treaty terms remain inconsistent after reinspection."""
    details, regions = pure_elt_fixtures()
    manager, client, _ = make_manager(
        details,
        regions,
        treaties={
            1: [treaty(1, 11)],
            2: [treaty(2, 22, occurrence_limit=2_000_000)],
        },
        post=True,
    )
    inspection = manager.inspect(analysis_ids=[1, 2])

    submission = manager.submit(
        analysis_ids=[1, 2],
        settings=settings(),
        event_rate_selections=[],
        expected_inspection_fingerprint=inspection.fingerprint,
    )

    assert submission.job_id == 7001
    assert client.calls[-1]["method"] == "POST"


def test_treaty_term_change_after_inspection_changes_fingerprint():
    """Require another review when treaty terms change before submission."""
    details, regions = pure_elt_fixtures()
    manager, client, _ = make_manager(
        details,
        regions,
        treaties={1: [treaty(1, 11)], 2: [treaty(2, 22)]},
    )
    inspection = manager.inspect(analysis_ids=[1, 2])
    manager._irp.analysis.treaties[2][0]["occurrenceLimit"] = 2_000_000

    with pytest.raises(IRPGroupingValidationError) as exc_info:
        manager.submit(
            analysis_ids=[1, 2],
            settings=settings(),
            event_rate_selections=[],
            expected_inspection_fingerprint=inspection.fingerprint,
        )

    assert exc_info.value.problems[0].code == "inspection_changed"
    assert client.calls == []


def test_conflicting_pure_elt_requires_one_of_the_observed_schemes():
    """Return both observed choices without selecting one."""
    manager, _, _ = make_manager(*pure_elt_fixtures(conflicting=True))

    result = manager.inspect(analysis_ids=[1, 2])

    partition = result.partitions[0]
    assert partition.event_rate_selection_required is True
    assert [option.event_rate_scheme_id for option in partition.event_rate_scheme_options] == [101, 102]


def test_display_name_region_rows_resolve_to_the_detail_codes():
    """Platform region rows carry ``peril`` as a display name; both members
    must land in one coded partition with labelled choices and no problems."""
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
    ] == [(101, "Historical"), (102, "Stochastic")]


def test_each_offered_scheme_is_named_from_reference_data():
    """A group's detail names only one of the schemes its members ran under, so
    naming every offered scheme from the detail showed one name on both. Each
    ID takes its own name from the reference list."""
    details = {
        1: analysis(1, is_group=True, scheme_id=None, engine_type="Group"),
        2: analysis(2, scheme_id=101, scheme_name="Historical"),
    }
    details[1]["additionalProperties"] = [{
        "key": "eventRateSchemes",
        "properties": [{"id": 0, "name": "", "value": {
            "regionCode": "NA", "perilCode": "WS", "framework": "ELT",
            "eventRateSchemeId": 101,
            "eventRateSchemeName": "Historical"}}],
    }]
    regions = {
        1: [region(1, scheme_id=101), region(1, scheme_id=102, sub_region="TX")],
        2: [region(2, scheme_id=101)],
    }
    manager, _, _ = make_manager(details, regions)

    result = manager.inspect(analysis_ids=[1, 2])

    partition = result.partitions[0]
    assert partition.event_rate_selection_required is True
    assert [
        (option.event_rate_scheme_id, option.label)
        for option in partition.event_rate_scheme_options
    ] == [(101, "Historical"), (102, "Stochastic")]


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


def jp_plt_fixtures(*, nested: bool = False):
    """Return JP Typhoon and Non-Typhoon PLT members for model 2.1."""
    details = {
        1: analysis(
            1,
            framework="PLT",
            engine_type="HD",
            scheme_id=None,
            is_group=nested,
        ),
        2: analysis(2, framework="PLT", engine_type="HD", scheme_id=None),
    }
    for detail in details.values():
        detail.update({
            "engineVersion": "HDv2.1",
            "perilCode": "WS",
            "regionCode": "JP",
        })
    regions = {
        1: [region(1, framework="PLT", scheme_id=None, pet_id=15, periods=100000)],
        2: [region(2, framework="PLT", scheme_id=None, pet_id=16, periods=100000)],
    }
    for rows in regions.values():
        for row in rows:
            row.update({
                "engineVersion": "HDv2.1",
                "peril": "WS",
                "region": "JP",
                "subRegion": "JP",
            })
    return details, regions


def test_jp_typhoon_and_non_typhoon_pets_submit_for_model_2_1():
    """Resolve duplicated PET IDs by version and submit both JPWS rows."""
    manager, _, reference_data = make_manager(*jp_plt_fixtures(), post=True)

    inspection = manager.inspect(analysis_ids=[1, 2])
    result = manager.submit(
        analysis_ids=[1, 2],
        settings=settings(),
        event_rate_selections=[],
        expected_inspection_fingerprint=inspection.fingerprint,
    )

    assert inspection.blocking_problems == ()
    assert inspection.partitions[0].observed_pet_ids == (15, 16)
    rows = result.request_body["settings"]["regionPerilSimulationSet"]
    assert [(row["modelRegionCode"], row["modelVersion"], row["simulationSetId"])
            for row in rows] == [("JPWS", "2.1", 15), ("JPWS", "2.1", 16)]
    assert {
        (call["pet_id"], call["model_version"], call["model_region_code"])
        for call in reference_data.pet_metadata_calls
    } == {(15, "2.1", "JPWS"), (16, "2.1", "JPWS")}


def test_reversing_analysis_ids_preserves_request_row_order():
    """Sort grouping request rows independently of caller analysis order."""
    first_manager, _, _ = make_manager(*jp_plt_fixtures(), post=True)
    first_inspection = first_manager.inspect(analysis_ids=[1, 2])
    first = first_manager.submit(
        analysis_ids=[1, 2],
        settings=settings(),
        event_rate_selections=[],
        expected_inspection_fingerprint=first_inspection.fingerprint,
    )
    second_manager, _, _ = make_manager(*jp_plt_fixtures(), post=True)
    second_inspection = second_manager.inspect(analysis_ids=[2, 1])
    second = second_manager.submit(
        analysis_ids=[2, 1],
        settings=settings(),
        event_rate_selections=[],
        expected_inspection_fingerprint=second_inspection.fingerprint,
    )

    assert (
        first.request_body["settings"]["regionPerilSimulationSet"]
        == second.request_body["settings"]["regionPerilSimulationSet"]
    )


@pytest.mark.parametrize(
    "pet_error",
    [IRPAPIError("No PET metadata found"), IRPAPIError("Multiple PET metadata rows found")],
)
def test_unavailable_exact_pet_metadata_does_not_warn_or_block(pet_error):
    """Use analysis and software-version data when qualified PET lookup fails."""
    manager, _, reference_data = make_manager(*jp_plt_fixtures())
    reference_data.pet_metadata_error = pet_error

    inspection = manager.inspect(analysis_ids=[1, 2])

    assert inspection.blocking_problems == ()
    assert inspection.warnings == ()


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


def test_differing_pet_ids_in_one_partition_submit_every_pet():
    """Submit each distinct source PET and leave compatibility to Platform."""
    details = {
        1: analysis(1, framework="PLT", engine_type="HD", scheme_id=None),
        2: analysis(2, framework="PLT", engine_type="HD", scheme_id=None),
    }
    regions = {
        1: [region(1, framework="PLT", scheme_id=None, pet_id=50, periods=100000)],
        2: [region(2, framework="PLT", scheme_id=None, pet_id=51, periods=100000)],
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
    assert inspection.partitions[0].observed_pet_ids == (50, 51)
    rows = result.request_body["settings"]["regionPerilSimulationSet"]
    assert [row["simulationSetId"] for row in rows] == [50, 51]


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


def test_nested_group_submits_every_distinct_pet_row():
    """Submit multiple PET IDs reported by a nested PLT group."""
    details = {
        1: analysis(1, framework="PLT", engine_type="HD", scheme_id=None, is_group=True),
        2: analysis(2, framework="PLT", engine_type="HD", scheme_id=None),
    }
    regions = {
        1: [region(1, framework="PLT", scheme_id=None, pet_id=50, periods=100000)],
        2: [region(2, framework="PLT", scheme_id=None, pet_id=50, periods=100000)],
    }
    manager, _, _ = make_manager(details, regions, post=True)
    supported = manager.inspect(analysis_ids=[1, 2])
    assert supported.blocking_problems == ()

    regions[1].append(
        region(1, framework="PLT", scheme_id=None, pet_id=51, periods=100000)
    )
    inspection = manager.inspect(analysis_ids=[1, 2])
    result = manager.submit(
        analysis_ids=[1, 2],
        settings=settings(),
        event_rate_selections=[],
        expected_inspection_fingerprint=inspection.fingerprint,
    )

    assert inspection.blocking_problems == ()
    rows = result.request_body["settings"]["regionPerilSimulationSet"]
    assert [row["simulationSetId"] for row in rows] == [50, 51]


def test_apply_contract_flag_blocks_plt_member():
    """Block an HD/PLT member that applies contract dates."""
    details, regions = mixed_fixtures()
    regions[2][0]["applyContractFlag"] = True
    manager, _, _ = make_manager(details, regions)

    inspection = manager.inspect(analysis_ids=[1, 2])

    assert "apply_contract_flag_unsupported" in {p.code for p in inspection.blocking_problems}


def test_missing_plt_pet_id_blocks():
    """Require a positive PET ID on every PLT region."""
    details, regions = mixed_fixtures()
    regions[2][0]["petId"] = None
    manager, _, _ = make_manager(details, regions)

    inspection = manager.inspect(analysis_ids=[1, 2])

    assert "pet_id_missing" in {p.code for p in inspection.blocking_problems}


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


def test_nested_elt_group_uses_region_metadata_and_offers_scheme_choice():
    """Do not compare each nested group region with its summary metadata."""
    details, regions = pure_elt_fixtures(conflicting=True)
    for detail in details.values():
        detail.update({
            "engineType": "Group",
            "isGroup": True,
            "engineVersion": "RL24",
            "perilCode": "WF",
            "regionCode": "US",
        })
    manager, _, _ = make_manager(details, regions)

    inspection = manager.inspect(analysis_ids=[1, 2])

    assert inspection.blocking_problems == ()
    assert inspection.partitions[0].key == GroupingPartitionKey("WS", "NA", "11.0")
    assert inspection.partitions[0].event_rate_selection_required is True
    assert [
        option.event_rate_scheme_id
        for option in inspection.partitions[0].event_rate_scheme_options
    ] == [101, 102]


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
