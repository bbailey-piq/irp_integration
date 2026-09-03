"""
Rules-based analysis grouping operations.

Grouping uses an inspect-then-submit contract. Inspection reads analyses,
regions, treaties, and reference mappings without creating a Platform job.
Submission repeats the inspection, compares its deterministic fingerprint,
validates the caller's explicit choices, and posts the resulting request
immediately. Treaties with the same Treaty Number and different loss-affecting
terms produce warnings but do not block submission.

Treaty comparison includes cedant, treaty type, currency, attachment and limit
terms, dates, percentages, priority, reinstatement and aggregate terms, LOBs,
and loss occurrences. Each warning carries the compared analysis treaty rows.
Treaty comparison excludes treaty IDs, display names, producers, premiums,
user-defined fields, tags, and URIs.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple, TYPE_CHECKING, TypeGuard

from .constants import CREATE_ANALYSIS_GROUP, GET_ANALYSIS_GROUPING_JOB
from .exceptions import IRPAPIError, IRPGroupingValidationError, IRPValidationError
from .utils import extract_id_from_location_header

if TYPE_CHECKING:
    from . import IRPClient


class GroupingProblemCode(str, Enum):
    """Stable codes returned for rule-based grouping problems."""

    INSPECTION_CHANGED = "inspection_changed"
    MEMBER_NOT_FOUND = "member_not_found"
    MEMBER_METADATA_MISSING = "member_metadata_missing"
    MEMBER_REGION_DATA_MISSING = "member_region_data_missing"
    MEMBER_CLASSIFICATION_CONFLICT = "member_classification_conflict"
    MODEL_VERSION_MAPPING_MISSING = "model_version_mapping_missing"
    MODEL_VERSION_MAPPING_AMBIGUOUS = "model_version_mapping_ambiguous"
    EVENT_RATE_SCHEME_MISSING = "event_rate_scheme_missing"
    EVENT_RATE_SELECTION_MISSING = "event_rate_selection_missing"
    EVENT_RATE_SELECTION_DUPLICATE = "event_rate_selection_duplicate"
    EVENT_RATE_SELECTION_UNKNOWN_PARTITION = "event_rate_selection_unknown_partition"
    EVENT_RATE_SELECTION_NOT_REQUIRED = "event_rate_selection_not_required"
    EVENT_RATE_SELECTION_NOT_OFFERED = "event_rate_selection_not_offered"
    PET_ID_MISSING = "pet_id_missing"
    PET_PERIODS_MISSING = "pet_periods_missing"
    APPLY_CONTRACT_FLAG_UNSUPPORTED = "apply_contract_flag_unsupported"
    SIMULATION_SET_MAPPING_MISSING = "simulation_set_mapping_missing"
    SIMULATION_SET_MAPPING_AMBIGUOUS = "simulation_set_mapping_ambiguous"
    INCONSISTENT_TREATY_TERMS = "inconsistent_treaty_terms"


@dataclass(frozen=True)
class GroupingPartitionKey:
    """Peril, region, and model-version key used by grouping rules."""

    peril_code: str
    region_code: str
    model_version: str


@dataclass(frozen=True)
class EventRateSchemeOption:
    """Event-rate scheme observed on at least one selected analysis."""

    event_rate_scheme_id: int
    label: Optional[str] = None


@dataclass(frozen=True)
class GroupingRegionFact:
    """Normalized Platform region fact used by grouping inspection."""

    analysis_id: int
    framework: str
    peril_code: str
    region_code: str
    model_version: str
    engine_version: str
    sub_region: str
    model_region_code: str
    event_rate_scheme_id: Optional[int]
    pet_id: Optional[int]
    periods: Optional[int]
    apply_contract_flag: bool


@dataclass(frozen=True)
class GroupingMember:
    """Normalized facts for one requested Platform analysis ID."""

    analysis_id: int
    exists: bool
    is_group: bool
    analysis_framework: Optional[str]
    engine_type: Optional[str]
    engine_version: Optional[str]
    peril_code: Optional[str]
    region_code: Optional[str]
    model_version: Optional[str]
    regions: Tuple[GroupingRegionFact, ...]


@dataclass(frozen=True)
class GroupingPartition:
    """Grouping choices and PET facts for one documented partition."""

    key: GroupingPartitionKey
    analysis_ids: Tuple[int, ...]
    event_rate_scheme_options: Tuple[EventRateSchemeOption, ...]
    observed_pet_ids: Tuple[int, ...]
    event_rate_selection_required: bool


@dataclass(frozen=True)
class GroupingSimulationMapping:
    """Exact reference-data mapping used for one simulated ELT region."""

    partition: GroupingPartitionKey
    analysis_ids: Tuple[int, ...]
    engine_version: str
    model_region_code: str
    event_rate_scheme_id: int
    simulation_set_id: int
    simulation_periods: int


@dataclass(frozen=True)
class GroupingTreaty:
    """One treaty as applied to one analysis, with the loss-affecting terms the grouping comparison read.

    Terms are the analysis-level values from
    ``AnalysisManager.search_analysis_treaties_paginated``, not the EDM
    definition: an analysis run in CAD against a treaty defined in USD
    reports CAD. Keys are the ``LOSS_AFFECTING_TREATY_FIELDS`` names plus
    ``lobs`` and ``lossOccurrences``, normalized the same way the comparison
    normalizes them, so a value shown for a field in ``differing_fields``
    always explains why that field differs.
    """

    analysis_id: int
    treaty_id: Optional[int]
    treaty_number: str
    terms: Dict[str, Any]


@dataclass(frozen=True)
class GroupingProblem:
    """Structured grouping problem suitable for caller rendering."""

    code: str
    message: str
    analysis_ids: Tuple[int, ...] = ()
    partition: Optional[GroupingPartitionKey] = None
    pet_ids: Tuple[int, ...] = ()
    treaty_numbers: Tuple[str, ...] = ()
    treaty_ids: Tuple[int, ...] = ()
    differing_fields: Tuple[str, ...] = ()
    treaties: Tuple[GroupingTreaty, ...] = ()


@dataclass(frozen=True)
class GroupingInspection:
    """Fresh facts, choices, warnings, and blocks for selected analyses."""

    analysis_ids: Tuple[int, ...]
    resource_uris: Tuple[str, ...]
    inspected_at: str
    fingerprint: str
    members: Tuple[GroupingMember, ...]
    output_loss_table: str
    simulate_to_plt: bool
    partitions: Tuple[GroupingPartition, ...]
    simulation_mappings: Tuple[GroupingSimulationMapping, ...]
    required_caller_inputs: Tuple[str, ...]
    warnings: Tuple[GroupingProblem, ...]
    blocking_problems: Tuple[GroupingProblem, ...]


@dataclass(frozen=True)
class GroupingCurrency:
    """Explicit currency settings for a grouping request."""

    code: str
    scheme: str
    vintage: str
    as_of_date: str


@dataclass(frozen=True)
class GroupingSettings:
    """Explicit caller settings for a grouping request."""

    analysis_name: str
    currency: GroupingCurrency
    propagate_detailed_losses: bool
    num_of_simulations: int
    description: Optional[str] = None
    reporting_window_start: Optional[str] = None
    simulation_window_start: Optional[str] = None
    simulation_window_end: Optional[str] = None


@dataclass(frozen=True)
class EventRateSelection:
    """Caller-selected event-rate scheme for a conflicting partition."""

    partition: GroupingPartitionKey
    event_rate_scheme_id: int


@dataclass(frozen=True)
class GroupingSubmission:
    """Created grouping job ID and the exact submitted request body."""

    job_id: int
    request_body: Dict[str, Any]


def _positive_int(value: Any) -> TypeGuard[int]:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def _text(value: Any) -> Optional[str]:
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _field(data: Mapping[str, Any], *names: str) -> Any:
    for name in names:
        if name in data and data[name] is not None:
            return data[name]
    return None


def _resolve_code(value: Any, code: Optional[str], name: Optional[str]) -> Optional[str]:
    """Return ``value`` as a code.

    Region rows carry display names such as ``"Windstorm"`` in ``peril`` while
    the analysis detail carries both ``perilCode`` and ``peril``. A value equal
    to the detail's display name resolves to the detail's code; anything else is
    returned unchanged.
    """
    text = _text(value)
    if text is None:
        return None
    if name is not None and text.casefold() == name.casefold():
        return code
    return text


def _event_rate_from_analysis(analysis: Mapping[str, Any]) -> Tuple[Optional[int], Optional[str]]:
    direct = _field(analysis, "eventRateSchemeId", "rateSchemeId")
    label = _text(_field(analysis, "eventRateSchemeName", "rateSchemeName"))
    if label is None:
        # The detail lists scheme names under ``eventRateSchemeNames`` with
        # ``id`` 0, so a name is attributed only when exactly one is listed.
        names = analysis.get("eventRateSchemeNames")
        if isinstance(names, list) and len(names) == 1 and isinstance(names[0], Mapping):
            label = _text(names[0].get("name"))
    if _positive_int(direct):
        return int(direct), label

    properties = analysis.get("additionalProperties") or []
    if not isinstance(properties, list):
        return None, label
    for entry in properties:
        if not isinstance(entry, Mapping):
            continue
        key = entry.get("key")
        nested = entry.get("properties") or []
        if not isinstance(nested, list):
            continue
        for prop in nested:
            if not isinstance(prop, Mapping):
                continue
            candidate = prop.get("id")
            value = prop.get("value")
            if isinstance(value, Mapping):
                candidate = _field(value, "eventRateSchemeId", "rateSchemeId")
                label = label or _text(_field(value, "eventRateSchemeName", "name"))
            if key in {"eventRateSchemeId", "eventRateSchemes"} and _positive_int(candidate):
                return int(candidate), label
    return None, label


class GroupingManager:
    """Inspect analysis members and submit resolved grouping requests."""

    FINGERPRINT_VERSION = 3

    LOSS_AFFECTING_TREATY_FIELDS = (
        "cedant",
        "treatyType",
        "currency",
        "attachmentBasis",
        "attachmentLevel",
        "occurrenceLimit",
        "attachmentPoint",
        "riskLimit",
        "retentionAmount",
        "percentagePlaced",
        "effectiveDate",
        "expirationDate",
        "percentageRetention",
        "percentageRiShare",
        "percentageCovered",
        "priority",
        "numberOfReinstatements",
        "reinstatementCharge",
        "maolAmount",
        "aggregateDeductible",
        "aggregateLimit",
    )

    def __init__(self, irp: "IRPClient") -> None:
        """Initialize the grouping manager.

        Args:
            irp: Owning IRP client instance
        """
        self._irp = irp
        self.client = irp.client

    def inspect(self, *, analysis_ids: Sequence[int]) -> GroupingInspection:
        """Inspect selected analyses without creating a Platform grouping job.

        Args:
            analysis_ids: At least two distinct positive Platform analysis IDs

        Returns:
            Normalized member facts, choices, warnings, blocks, and fingerprint

        Raises:
            IRPValidationError: If analysis_ids is malformed
            IRPAPIError: If a Platform or reference-data read fails
        """
        normalized_ids = self._validate_analysis_ids(analysis_ids)
        return self._inspect(normalized_ids)

    def submit(
        self,
        *,
        analysis_ids: Sequence[int],
        settings: GroupingSettings,
        event_rate_selections: Sequence[EventRateSelection],
        expected_inspection_fingerprint: str,
    ) -> GroupingSubmission:
        """Reinspect, validate explicit choices, and create a grouping job.

        Args:
            analysis_ids: At least two distinct positive Platform analysis IDs
            settings: Explicit grouping request settings
            event_rate_selections: One offered scheme for each conflicting partition
            expected_inspection_fingerprint: Fingerprint returned by the caller's inspection

        Returns:
            Created grouping job ID and exact submitted request body

        Raises:
            IRPValidationError: If a direct method argument is malformed
            IRPGroupingValidationError: If inspected facts or caller selections block submission
            IRPAPIError: If a Platform read or grouping POST fails
        """
        normalized_ids = self._validate_analysis_ids(analysis_ids)
        self._validate_settings(settings)
        selections = self._validate_selection_arguments(event_rate_selections)
        if not _text(expected_inspection_fingerprint):
            raise IRPValidationError("expected_inspection_fingerprint must be a non-empty string")

        inspection = self._inspect(normalized_ids)
        if inspection.fingerprint != expected_inspection_fingerprint:
            raise IRPGroupingValidationError((GroupingProblem(
                code=GroupingProblemCode.INSPECTION_CHANGED.value,
                message="Grouping facts changed after inspection; inspect the analyses again.",
                analysis_ids=normalized_ids,
            ),))
        if inspection.blocking_problems:
            raise IRPGroupingValidationError(inspection.blocking_problems)

        selected = self._resolve_event_rate_selections(inspection, selections)
        request_body = self._build_request(inspection, settings, selected)
        try:
            response = self.client.request("POST", CREATE_ANALYSIS_GROUP, json=request_body)
            job_id = extract_id_from_location_header(response, "analysis group creation")
        except IRPAPIError:
            raise
        except Exception as exc:
            raise IRPAPIError(
                f"Failed to submit analysis group '{settings.analysis_name}': {exc}"
            ) from exc
        return GroupingSubmission(job_id=int(job_id), request_body=request_body)

    def get_job(self, *, job_id: int) -> Dict[str, Any]:
        """Retrieve grouping job status by job ID.

        Args:
            job_id: Positive Platform grouping job ID

        Returns:
            Grouping job status response

        Raises:
            IRPValidationError: If job_id is invalid
            IRPAPIError: If the Platform read fails
        """
        if not _positive_int(job_id):
            raise IRPValidationError("job_id must be a positive integer")
        try:
            response = self.client.request(
                "GET", GET_ANALYSIS_GROUPING_JOB.format(jobId=job_id)
            )
            return response.json()
        except IRPAPIError:
            raise
        except Exception as exc:
            raise IRPAPIError(
                f"Failed to get analysis grouping job status for job ID {job_id}: {exc}"
            ) from exc

    @staticmethod
    def _validate_analysis_ids(analysis_ids: Sequence[int]) -> Tuple[int, ...]:
        if isinstance(analysis_ids, (str, bytes)) or not isinstance(analysis_ids, Sequence):
            raise IRPValidationError("analysis_ids must be a sequence of positive integers")
        normalized = tuple(analysis_ids)
        if len(normalized) < 2:
            raise IRPValidationError("analysis_ids must contain at least two analysis IDs")
        if any(not _positive_int(value) for value in normalized):
            raise IRPValidationError("analysis_ids must contain only positive integers")
        if len(set(normalized)) != len(normalized):
            raise IRPValidationError("analysis_ids must contain distinct analysis IDs")
        return normalized

    @staticmethod
    def _validate_settings(settings: GroupingSettings) -> None:
        if not isinstance(settings, GroupingSettings):
            raise IRPValidationError("settings must be a GroupingSettings instance")
        if not _text(settings.analysis_name):
            raise IRPValidationError("settings.analysis_name must be a non-empty string")
        if not isinstance(settings.currency, GroupingCurrency):
            raise IRPValidationError("settings.currency must be a GroupingCurrency instance")
        for name in ("code", "scheme", "vintage", "as_of_date"):
            if not _text(getattr(settings.currency, name)):
                raise IRPValidationError(f"settings.currency.{name} must be a non-empty string")
        if not isinstance(settings.propagate_detailed_losses, bool):
            raise IRPValidationError("settings.propagate_detailed_losses must be a boolean")
        if not _positive_int(settings.num_of_simulations):
            raise IRPValidationError("settings.num_of_simulations must be a positive integer")
        for name in (
            "description",
            "reporting_window_start",
            "simulation_window_start",
            "simulation_window_end",
        ):
            value = getattr(settings, name)
            if value is not None and not isinstance(value, str):
                raise IRPValidationError(f"settings.{name} must be a string or None")

    @staticmethod
    def _validate_selection_arguments(
        selections: Sequence[EventRateSelection],
    ) -> Tuple[EventRateSelection, ...]:
        if isinstance(selections, (str, bytes)) or not isinstance(selections, Sequence):
            raise IRPValidationError("event_rate_selections must be a sequence")
        normalized = tuple(selections)
        for selection in normalized:
            if not isinstance(selection, EventRateSelection):
                raise IRPValidationError(
                    "event_rate_selections must contain EventRateSelection values"
                )
            if not isinstance(selection.partition, GroupingPartitionKey):
                raise IRPValidationError("selection.partition must be a GroupingPartitionKey")
            if not all((
                _text(selection.partition.peril_code),
                _text(selection.partition.region_code),
                _text(selection.partition.model_version),
            )):
                raise IRPValidationError("selection.partition fields must be non-empty strings")
            if not _positive_int(selection.event_rate_scheme_id):
                raise IRPValidationError(
                    "selection.event_rate_scheme_id must be a positive integer"
                )
        return normalized

    def _inspect(self, analysis_ids: Tuple[int, ...]) -> GroupingInspection:
        problems: List[GroupingProblem] = []
        members: List[GroupingMember] = []
        treaties: List[Dict[str, Any]] = []
        labels: Dict[int, Optional[str]] = {}
        scheme_names: Optional[Dict[int, Optional[str]]] = None
        version_cache: Dict[Tuple[str, str, str], Tuple[Optional[str], Optional[Exception]]] = {}
        pet_cache: Dict[
            Tuple[int, str, Optional[str]],
            Tuple[Optional[Dict[str, Any]], Optional[Exception]],
        ] = {}

        def model_version(engine: str, region: str, peril: str) -> Tuple[Optional[str], Optional[Exception]]:
            key = (engine, region, peril)
            if key not in version_cache:
                try:
                    value = self._irp.reference_data.get_model_version_by_engine_region_peril(
                        engine, region, peril
                    )
                    version_cache[key] = (str(value), None)
                except IRPAPIError as exc:
                    version_cache[key] = (None, exc)
            return version_cache[key]

        def pet_metadata(
            pet_id: int,
            model_version: str,
            model_region_code: Optional[str],
        ) -> Tuple[Optional[Dict[str, Any]], Optional[Exception]]:
            key = (pet_id, model_version, model_region_code)
            if key not in pet_cache:
                try:
                    value = self._irp.reference_data.get_pet_metadata_exact(
                        pet_id=pet_id,
                        model_version=model_version,
                        model_region_code=model_region_code,
                    )
                    pet_cache[key] = (value, None)
                except IRPAPIError as exc:
                    pet_cache[key] = (None, exc)
            return pet_cache[key]

        def scheme_name(scheme_id: int) -> Optional[str]:
            """The Platform's own name for an event-rate scheme ID. A member's
            region rows carry the ID alone, and its detail names at most one
            scheme, so the reference list is the only source that names every
            offered ID."""
            nonlocal scheme_names
            if scheme_names is None:
                payload = self._irp.reference_data.get_event_rate_schemes()
                rows = payload.get("items") if isinstance(payload, Mapping) else payload
                scheme_names = {
                    int(row["eventRateSchemeId"]): _text(row.get("eventRateSchemeName"))
                    for row in rows or ()
                    if isinstance(row, Mapping)
                    and _positive_int(row.get("eventRateSchemeId"))
                }
            return scheme_names.get(scheme_id)

        for analysis_id in analysis_ids:
            try:
                analysis = self._irp.analysis.get_analysis_by_id(analysis_id)
            except IRPAPIError as exc:
                message = str(exc).lower()
                if "404" not in message and "not found" not in message:
                    raise
                problems.append(GroupingProblem(
                    code=GroupingProblemCode.MEMBER_NOT_FOUND.value,
                    message=f"Analysis {analysis_id} was not found.",
                    analysis_ids=(analysis_id,),
                ))
                members.append(GroupingMember(
                    analysis_id, False, False, None, None, None, None, None, None, ()
                ))
                continue

            if not isinstance(analysis, Mapping) or not analysis:
                problems.append(GroupingProblem(
                    code=GroupingProblemCode.MEMBER_NOT_FOUND.value,
                    message=f"Analysis {analysis_id} returned no analysis details.",
                    analysis_ids=(analysis_id,),
                ))
                members.append(GroupingMember(
                    analysis_id, False, False, None, None, None, None, None, None, ()
                ))
                continue

            raw_treaties = self._irp.analysis.search_analysis_treaties_paginated(analysis_id)
            if not isinstance(raw_treaties, list):
                raise IRPAPIError(
                    f"Treaty search for analysis ID {analysis_id} returned a non-list response"
                )
            for treaty in raw_treaties:
                if not isinstance(treaty, Mapping):
                    raise IRPAPIError(
                        f"Treaty search for analysis ID {analysis_id} returned a malformed treaty"
                    )
                treaty_number = _text(treaty.get("treatyNumber"))
                if treaty_number is None:
                    raise IRPAPIError(
                        f"Treaty for analysis ID {analysis_id} has no Treaty Number"
                    )
                treaty_id = treaty.get("treatyId")
                treaties.append({
                    "analysis_id": analysis_id,
                    "treaty_id": int(treaty_id) if _positive_int(treaty_id) else None,
                    "treaty_number": treaty_number,
                    "terms": self._loss_affecting_treaty_terms(treaty),
                })

            raw_regions = self._irp.analysis.get_regions(analysis_id)
            if not isinstance(raw_regions, list) or not raw_regions:
                problems.append(GroupingProblem(
                    code=GroupingProblemCode.MEMBER_REGION_DATA_MISSING.value,
                    message=f"Analysis {analysis_id} returned no region data.",
                    analysis_ids=(analysis_id,),
                ))
                raw_regions = []

            analysis_framework = _text(_field(analysis, "analysisFramework", "framework"))
            analysis_framework = analysis_framework.upper() if analysis_framework else None
            engine_type = _text(_field(analysis, "engineType", "type"))
            is_group = bool(_field(analysis, "isGroup")) or (engine_type or "").upper() == "GROUP"
            detail_engine = _text(_field(analysis, "engineVersion", "softwareVersionCode"))
            detail_peril = _text(_field(analysis, "perilCode", "peril"))
            detail_region = _text(_field(analysis, "regionCode", "region"))
            detail_peril_name = _text(analysis.get("peril"))
            detail_region_name = _text(analysis.get("region"))
            analysis_scheme, analysis_label = _event_rate_from_analysis(analysis)
            if analysis_scheme is not None:
                labels[analysis_scheme] = analysis_label

            region_facts: List[GroupingRegionFact] = []
            observed_frameworks = set()
            for raw_region in raw_regions:
                if not isinstance(raw_region, Mapping):
                    problems.append(GroupingProblem(
                        code=GroupingProblemCode.MEMBER_METADATA_MISSING.value,
                        message=f"Analysis {analysis_id} returned a malformed region row.",
                        analysis_ids=(analysis_id,),
                    ))
                    continue
                framework = _text(_field(raw_region, "framework", "analysisFramework"))
                framework = (framework or analysis_framework or "").upper()
                if framework not in {"ELT", "PLT"}:
                    problems.append(GroupingProblem(
                        code=GroupingProblemCode.MEMBER_METADATA_MISSING.value,
                        message=f"Analysis {analysis_id} has a region with no ELT/PLT classification.",
                        analysis_ids=(analysis_id,),
                    ))
                    continue
                observed_frameworks.add(framework)
                row_engine = _text(_field(raw_region, "engineVersion", "softwareVersionCode"))
                row_peril = _resolve_code(
                    _field(raw_region, "perilCode", "peril"), detail_peril, detail_peril_name
                )
                row_region = _resolve_code(
                    _field(raw_region, "regionCode", "region"), detail_region, detail_region_name
                )
                engine = row_engine or detail_engine
                peril = row_peril or detail_peril
                region = row_region or detail_region
                sub_region = _text(_field(raw_region, "subRegion", "subRegionCode")) or ""
                apply_contract = bool(_field(raw_region, "applyContractFlag"))
                scheme = _field(raw_region, "eventRateSchemeId", "rateSchemeId")
                scheme_id = int(scheme) if _positive_int(scheme) else analysis_scheme
                pet_value = _field(raw_region, "petId", "simulationSetId")
                pet_id = int(pet_value) if _positive_int(pet_value) else None
                period_value = _field(raw_region, "periods", "simulationPeriods")
                periods = int(period_value) if _positive_int(period_value) else None

                resolved_version: Optional[str] = None
                if framework == "PLT":
                    if pet_id is None:
                        problems.append(GroupingProblem(
                            code=GroupingProblemCode.PET_ID_MISSING.value,
                            message=f"PLT analysis {analysis_id} has a region with no positive PET ID.",
                            analysis_ids=(analysis_id,),
                        ))
                    if periods is None:
                        problems.append(GroupingProblem(
                            code=GroupingProblemCode.PET_PERIODS_MISSING.value,
                            message=f"PLT analysis {analysis_id} has no positive period count.",
                            analysis_ids=(analysis_id,),
                            pet_ids=(pet_id,) if pet_id else (),
                        ))
                    if apply_contract:
                        problems.append(GroupingProblem(
                            code=GroupingProblemCode.APPLY_CONTRACT_FLAG_UNSUPPORTED.value,
                            message=f"PLT analysis {analysis_id} applies contract dates and cannot be grouped.",
                            analysis_ids=(analysis_id,),
                        ))

                if not engine or not peril or not region:
                    problems.append(GroupingProblem(
                        code=GroupingProblemCode.MEMBER_METADATA_MISSING.value,
                        message=(f"Analysis {analysis_id} has a region missing engine, peril, "
                                 "or region metadata."),
                        analysis_ids=(analysis_id,),
                    ))
                    continue

                if resolved_version is None:
                    resolved_version, version_error = model_version(engine, region, peril)
                    if version_error is not None:
                        code = (GroupingProblemCode.MODEL_VERSION_MAPPING_AMBIGUOUS.value
                                if "multiple" in str(version_error).lower()
                                else GroupingProblemCode.MODEL_VERSION_MAPPING_MISSING.value)
                        problems.append(GroupingProblem(
                            code=code,
                            message=(f"Model version for analysis {analysis_id}, engine {engine}, "
                                     f"region {region}, and peril {peril} was not resolved exactly."),
                            analysis_ids=(analysis_id,),
                        ))
                        continue

                if resolved_version is None:
                    problems.append(GroupingProblem(
                        code=GroupingProblemCode.MODEL_VERSION_MAPPING_MISSING.value,
                        message=f"Analysis {analysis_id} has no resolved model version.",
                        analysis_ids=(analysis_id,),
                    ))
                    continue

                if framework == "PLT" and pet_id is not None:
                    broad_model_region = f"{region}{peril}"
                    pet, _ = pet_metadata(
                        pet_id,
                        resolved_version,
                        broad_model_region,
                    )
                    if pet is not None:
                        pet_model_region = _text(pet.get("modelRegionCode"))
                        if pet_model_region and len(pet_model_region) >= 3:
                            peril = pet_model_region[-2:]
                            region = pet_model_region[:-2]

                if framework == "ELT" and scheme_id is None:
                    problems.append(GroupingProblem(
                        code=GroupingProblemCode.EVENT_RATE_SCHEME_MISSING.value,
                        message=f"ELT analysis {analysis_id} has no positive event-rate scheme ID.",
                        analysis_ids=(analysis_id,),
                        partition=GroupingPartitionKey(peril, region, resolved_version),
                    ))

                model_region = f"{sub_region}{peril}" if sub_region else f"{region}{peril}"
                region_facts.append(GroupingRegionFact(
                    analysis_id=analysis_id,
                    framework=framework,
                    peril_code=peril,
                    region_code=region,
                    model_version=resolved_version,
                    engine_version=engine,
                    sub_region=sub_region,
                    model_region_code=model_region,
                    event_rate_scheme_id=scheme_id if framework == "ELT" else None,
                    pet_id=pet_id if framework == "PLT" else None,
                    periods=periods if framework == "PLT" else None,
                    apply_contract_flag=apply_contract,
                ))

            if analysis_framework and observed_frameworks and observed_frameworks != {analysis_framework}:
                problems.append(GroupingProblem(
                    code=GroupingProblemCode.MEMBER_CLASSIFICATION_CONFLICT.value,
                    message=(f"Analysis {analysis_id} framework {analysis_framework} conflicts "
                             f"with its region frameworks {sorted(observed_frameworks)}."),
                    analysis_ids=(analysis_id,),
                ))
            if analysis_framework is None and len(observed_frameworks) == 1:
                analysis_framework = next(iter(observed_frameworks))
            versions = sorted({fact.model_version for fact in region_facts})
            members.append(GroupingMember(
                analysis_id=analysis_id,
                exists=True,
                is_group=is_group,
                analysis_framework=analysis_framework,
                engine_type=engine_type,
                engine_version=detail_engine,
                peril_code=detail_peril,
                region_code=detail_region,
                model_version=versions[0] if len(versions) == 1 else None,
                regions=tuple(sorted(region_facts, key=self._region_sort_key)),
            ))

        all_facts = [fact for member in members for fact in member.regions]
        simulate_to_plt = any(fact.framework == "PLT" for fact in all_facts)
        output_loss_table = "PLT" if simulate_to_plt else "ELT"

        partition_facts: Dict[GroupingPartitionKey, List[GroupingRegionFact]] = {}
        for fact in all_facts:
            key = GroupingPartitionKey(fact.peril_code, fact.region_code, fact.model_version)
            partition_facts.setdefault(key, []).append(fact)

        partitions: List[GroupingPartition] = []
        for key in sorted(partition_facts, key=self._partition_sort_key):
            facts = partition_facts[key]
            analysis_id_set = tuple(sorted({fact.analysis_id for fact in facts}))
            scheme_ids = sorted({
                fact.event_rate_scheme_id for fact in facts
                if fact.framework == "ELT" and fact.event_rate_scheme_id is not None
            })
            pet_ids = tuple(sorted({
                fact.pet_id for fact in facts
                if fact.framework == "PLT" and fact.pet_id is not None
            }))
            partitions.append(GroupingPartition(
                key=key,
                analysis_ids=analysis_id_set,
                event_rate_scheme_options=tuple(
                    EventRateSchemeOption(
                        scheme_id, scheme_name(scheme_id) or labels.get(scheme_id)
                    )
                    for scheme_id in scheme_ids
                ),
                observed_pet_ids=pet_ids,
                event_rate_selection_required=len(scheme_ids) > 1,
            ))

        mappings: List[GroupingSimulationMapping] = []
        if simulate_to_plt:
            simulation_cache: Dict[
                Tuple[int, str, str], Tuple[Optional[Dict[str, Any]], Optional[Exception]]
            ] = {}
            mapping_groups: Dict[
                Tuple[GroupingPartitionKey, str, str, int], List[GroupingRegionFact]
            ] = {}
            schemes_by_key = {
                partition.key: tuple(
                    option.event_rate_scheme_id for option in partition.event_rate_scheme_options
                )
                for partition in partitions
            }
            for fact in all_facts:
                if fact.framework != "ELT":
                    continue
                key = GroupingPartitionKey(fact.peril_code, fact.region_code, fact.model_version)
                for scheme_id in schemes_by_key.get(key, ()):
                    mapping_groups.setdefault(
                        (key, fact.engine_version, fact.model_region_code, scheme_id), []
                    ).append(fact)

            for mapping_key in sorted(
                mapping_groups,
                key=lambda value: (
                    self._partition_sort_key(value[0]), value[1], value[2], value[3]
                ),
            ):
                key, engine, model_region, scheme_id = mapping_key
                facts = mapping_groups[mapping_key]
                broad_model_region = f"{key.region_code}{key.peril_code}"
                reference_key = (scheme_id, broad_model_region, key.model_version)
                if reference_key not in simulation_cache:
                    try:
                        resolved_row = self._irp.reference_data.get_simulation_set_exact(
                            event_rate_scheme_id=scheme_id,
                            model_region_code=broad_model_region,
                            model_version=key.model_version,
                        )
                        simulation_cache[reference_key] = (resolved_row, None)
                    except IRPAPIError as exc:
                        simulation_cache[reference_key] = (None, exc)
                cached_row, mapping_error = simulation_cache[reference_key]
                if mapping_error is not None:
                    code = (GroupingProblemCode.SIMULATION_SET_MAPPING_AMBIGUOUS.value
                            if "multiple" in str(mapping_error).lower()
                            else GroupingProblemCode.SIMULATION_SET_MAPPING_MISSING.value)
                    problems.append(GroupingProblem(
                        code=code,
                        message=(f"Simulation set for scheme {scheme_id}, model region "
                                 f"{broad_model_region}, and model version {key.model_version} "
                                 "was not resolved exactly."),
                        analysis_ids=tuple(sorted({fact.analysis_id for fact in facts})),
                        partition=key,
                    ))
                    continue
                if cached_row is None:
                    raise IRPAPIError("Exact simulation-set lookup returned no row")
                simulation_id = _field(cached_row, "id", "simulationSetId")
                simulation_periods = _field(cached_row, "defaultPeriods", "numberOfPeriods")
                if not _positive_int(simulation_id) or not _positive_int(simulation_periods):
                    problems.append(GroupingProblem(
                        code=GroupingProblemCode.SIMULATION_SET_MAPPING_MISSING.value,
                        message=(f"Simulation set for scheme {scheme_id} has no positive ID "
                                 "or period count."),
                        analysis_ids=tuple(sorted({fact.analysis_id for fact in facts})),
                        partition=key,
                    ))
                    continue
                mappings.append(GroupingSimulationMapping(
                    partition=key,
                    analysis_ids=tuple(sorted({fact.analysis_id for fact in facts})),
                    engine_version=engine,
                    model_region_code=model_region,
                    event_rate_scheme_id=scheme_id,
                    simulation_set_id=int(simulation_id),
                    simulation_periods=int(simulation_periods),
                ))

        problems = self._deduplicate_problems(problems)
        warnings = self._treaty_warnings(treaties)
        required = [
            "analysis_name",
            "currency",
            "propagate_detailed_losses",
            "num_of_simulations",
        ]
        if any(partition.event_rate_selection_required for partition in partitions):
            required.append("event_rate_selections")

        inspected_at = datetime.now(timezone.utc).isoformat()
        resource_uris = tuple(
            f"/platform/riskdata/v1/analyses/{analysis_id}" for analysis_id in analysis_ids
        )
        fingerprint = self._fingerprint(
            analysis_ids=analysis_ids,
            resource_uris=resource_uris,
            members=tuple(members),
            output_loss_table=output_loss_table,
            simulate_to_plt=simulate_to_plt,
            partitions=tuple(partitions),
            mappings=tuple(mappings),
            treaties=tuple(treaties),
            problems=tuple(problems),
        )
        return GroupingInspection(
            analysis_ids=analysis_ids,
            resource_uris=resource_uris,
            inspected_at=inspected_at,
            fingerprint=fingerprint,
            members=tuple(members),
            output_loss_table=output_loss_table,
            simulate_to_plt=simulate_to_plt,
            partitions=tuple(partitions),
            simulation_mappings=tuple(mappings),
            required_caller_inputs=tuple(required),
            warnings=tuple(warnings),
            blocking_problems=tuple(problems),
        )

    @staticmethod
    def _partition_sort_key(key: GroupingPartitionKey) -> Tuple[str, str, str]:
        return key.peril_code, key.region_code, key.model_version

    @staticmethod
    def _region_sort_key(fact: GroupingRegionFact) -> Tuple[Any, ...]:
        return (
            fact.peril_code,
            fact.region_code,
            fact.model_version,
            fact.framework,
            fact.model_region_code,
            fact.engine_version,
            fact.event_rate_scheme_id or 0,
            fact.pet_id or 0,
            fact.periods or 0,
        )

    @staticmethod
    def _deduplicate_problems(problems: List[GroupingProblem]) -> List[GroupingProblem]:
        unique: Dict[str, GroupingProblem] = {}
        for problem in problems:
            payload = {
                "code": problem.code,
                "analysis_ids": problem.analysis_ids,
                "partition": asdict(problem.partition) if problem.partition else None,
                "pet_ids": problem.pet_ids,
                "treaty_numbers": problem.treaty_numbers,
                "treaty_ids": problem.treaty_ids,
                "differing_fields": problem.differing_fields,
            }
            unique[json.dumps(payload, sort_keys=True)] = problem
        return [unique[key] for key in sorted(unique)]

    @staticmethod
    def _reference_value(value: Any, *keys: str) -> Any:
        if not isinstance(value, Mapping):
            return value
        return _field(value, *keys)

    @classmethod
    def _loss_affecting_treaty_terms(cls, treaty: Mapping[str, Any]) -> Dict[str, Any]:
        terms = {field: treaty.get(field) for field in cls.LOSS_AFFECTING_TREATY_FIELDS}
        terms["cedant"] = cls._reference_value(
            treaty.get("cedant"), "cedantId", "cedantName"
        )
        terms["currency"] = cls._reference_value(treaty.get("currency"), "code", "id")

        lobs = []
        for lob in treaty.get("lobs") or []:
            lobs.append(cls._reference_value(lob, "lobId", "lobName"))
        terms["lobs"] = sorted(
            lobs, key=lambda value: json.dumps(value, sort_keys=True, default=str)
        )

        loss_occurrences = []
        for occurrence in treaty.get("lossOccurrences") or []:
            if not isinstance(occurrence, Mapping):
                loss_occurrences.append(occurrence)
                continue
            loss_occurrences.append({
                "regionPeril": cls._reference_value(
                    occurrence.get("regionPeril"), "code", "id"
                ),
                "lossOccurrenceTime": occurrence.get("lossOccurrenceTime"),
                "lossOccurrenceRadius": occurrence.get("lossOccurrenceRadius"),
                "radiusUnit": cls._reference_value(
                    occurrence.get("radiusUnit"), "code", "id"
                ),
                "multiLossOccurrence": cls._reference_value(
                    occurrence.get("multiLossOccurrence"), "code", "id"
                ),
            })
        terms["lossOccurrences"] = sorted(
            loss_occurrences,
            key=lambda value: json.dumps(value, sort_keys=True, default=str),
        )
        return terms

    @classmethod
    def _treaty_warnings(cls, treaties: List[Dict[str, Any]]) -> List[GroupingProblem]:
        by_number: Dict[str, List[Dict[str, Any]]] = {}
        for treaty in treaties:
            by_number.setdefault(treaty["treaty_number"], []).append(treaty)

        warnings = []
        for treaty_number in sorted(by_number):
            matches = by_number[treaty_number]
            if len(matches) < 2:
                continue
            term_payloads = {
                json.dumps(treaty["terms"], sort_keys=True, separators=(",", ":"))
                for treaty in matches
            }
            if len(term_payloads) == 1:
                continue
            differing_fields = tuple(sorted(
                field
                for field in matches[0]["terms"]
                if len({
                    json.dumps(
                        treaty["terms"].get(field),
                        sort_keys=True,
                        separators=(",", ":"),
                    )
                    for treaty in matches
                }) > 1
            ))
            affected_analysis_ids = tuple(sorted({
                int(treaty["analysis_id"]) for treaty in matches
            }))
            affected_treaty_ids = tuple(sorted({
                int(treaty["treaty_id"])
                for treaty in matches
                if treaty["treaty_id"] is not None
            }))
            warnings.append(GroupingProblem(
                code=GroupingProblemCode.INCONSISTENT_TREATY_TERMS.value,
                message=(
                    f"Treaty Number {json.dumps(treaty_number)} has inconsistent "
                    f"loss-affecting terms {list(differing_fields)} across analyses "
                    f"{list(affected_analysis_ids)}."
                ),
                analysis_ids=affected_analysis_ids,
                treaty_numbers=(treaty_number,),
                treaty_ids=affected_treaty_ids,
                differing_fields=differing_fields,
                treaties=tuple(
                    GroupingTreaty(
                        analysis_id=int(treaty["analysis_id"]),
                        treaty_id=treaty["treaty_id"],
                        treaty_number=treaty["treaty_number"],
                        terms=treaty["terms"],
                    )
                    for treaty in sorted(
                        matches,
                        key=lambda treaty: (
                            treaty["analysis_id"],
                            treaty["treaty_id"] or 0,
                        ),
                    )
                ),
            ))
        return warnings

    def _fingerprint(
        self,
        *,
        analysis_ids: Tuple[int, ...],
        resource_uris: Tuple[str, ...],
        members: Tuple[GroupingMember, ...],
        output_loss_table: str,
        simulate_to_plt: bool,
        partitions: Tuple[GroupingPartition, ...],
        mappings: Tuple[GroupingSimulationMapping, ...],
        treaties: Tuple[Dict[str, Any], ...],
        problems: Tuple[GroupingProblem, ...],
    ) -> str:
        partition_payload = []
        for partition in partitions:
            partition_payload.append({
                "key": asdict(partition.key),
                "analysis_ids": partition.analysis_ids,
                "event_rate_scheme_ids": tuple(
                    option.event_rate_scheme_id
                    for option in partition.event_rate_scheme_options
                ),
                "observed_pet_ids": partition.observed_pet_ids,
                "event_rate_selection_required": partition.event_rate_selection_required,
            })
        problem_payload = [{
            "code": problem.code,
            "analysis_ids": problem.analysis_ids,
            "partition": asdict(problem.partition) if problem.partition else None,
            "pet_ids": problem.pet_ids,
            "treaty_numbers": problem.treaty_numbers,
            "treaty_ids": problem.treaty_ids,
            "differing_fields": problem.differing_fields,
        } for problem in problems]
        payload = {
            "version": self.FINGERPRINT_VERSION,
            "analysis_ids": analysis_ids,
            "resource_uris": resource_uris,
            "members": [asdict(member) for member in sorted(members, key=lambda value: value.analysis_id)],
            "output_loss_table": output_loss_table,
            "simulate_to_plt": simulate_to_plt,
            "partitions": partition_payload,
            "simulation_mappings": [asdict(mapping) for mapping in mappings],
            "treaties": sorted(
                treaties,
                key=lambda treaty: (
                    treaty["analysis_id"],
                    treaty["treaty_number"],
                    treaty["treaty_id"] or 0,
                ),
            ),
            "problems": problem_payload,
        }
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return f"v{self.FINGERPRINT_VERSION}:{hashlib.sha256(canonical.encode('utf-8')).hexdigest()}"

    def _resolve_event_rate_selections(
        self,
        inspection: GroupingInspection,
        selections: Tuple[EventRateSelection, ...],
    ) -> Dict[GroupingPartitionKey, int]:
        partitions = {partition.key: partition for partition in inspection.partitions}
        required = {
            key: partition for key, partition in partitions.items()
            if partition.event_rate_selection_required
        }
        supplied: Dict[GroupingPartitionKey, int] = {}
        problems: List[GroupingProblem] = []
        for selection in selections:
            key = selection.partition
            if key in supplied:
                problems.append(GroupingProblem(
                    code=GroupingProblemCode.EVENT_RATE_SELECTION_DUPLICATE.value,
                    message="A conflicting partition has more than one event-rate selection.",
                    partition=key,
                ))
                continue
            supplied[key] = selection.event_rate_scheme_id
            partition = partitions.get(key)
            if partition is None:
                problems.append(GroupingProblem(
                    code=GroupingProblemCode.EVENT_RATE_SELECTION_UNKNOWN_PARTITION.value,
                    message="An event-rate selection names a partition not returned by inspection.",
                    partition=key,
                ))
                continue
            if key not in required:
                problems.append(GroupingProblem(
                    code=GroupingProblemCode.EVENT_RATE_SELECTION_NOT_REQUIRED.value,
                    message="An event-rate selection was supplied for a non-conflicting partition.",
                    analysis_ids=partition.analysis_ids,
                    partition=key,
                ))
                continue
            offered = {
                option.event_rate_scheme_id for option in partition.event_rate_scheme_options
            }
            if selection.event_rate_scheme_id not in offered:
                problems.append(GroupingProblem(
                    code=GroupingProblemCode.EVENT_RATE_SELECTION_NOT_OFFERED.value,
                    message=(f"Event-rate scheme {selection.event_rate_scheme_id} was not "
                             "observed on the selected members for this partition."),
                    analysis_ids=partition.analysis_ids,
                    partition=key,
                ))
        for key, partition in required.items():
            if key not in supplied:
                problems.append(GroupingProblem(
                    code=GroupingProblemCode.EVENT_RATE_SELECTION_MISSING.value,
                    message="A conflicting partition requires an explicit event-rate selection.",
                    analysis_ids=partition.analysis_ids,
                    partition=key,
                ))
        if problems:
            raise IRPGroupingValidationError(tuple(self._deduplicate_problems(problems)))

        resolved: Dict[GroupingPartitionKey, int] = {}
        for key, partition in partitions.items():
            options = partition.event_rate_scheme_options
            if len(options) == 1:
                resolved[key] = options[0].event_rate_scheme_id
            elif len(options) > 1:
                resolved[key] = supplied[key]
        return resolved

    def _build_request(
        self,
        inspection: GroupingInspection,
        settings: GroupingSettings,
        selected_schemes: Dict[GroupingPartitionKey, int],
    ) -> Dict[str, Any]:
        region_peril = self._build_region_peril_simulation_set(
            inspection, selected_schemes
        )
        request_settings: Dict[str, Any] = {
            "analysisName": settings.analysis_name,
            "currency": {
                "code": settings.currency.code,
                "scheme": settings.currency.scheme,
                "vintage": settings.currency.vintage,
                "asOfDate": settings.currency.as_of_date,
            },
            "simulateToPLT": inspection.simulate_to_plt,
            "propagateDetailedLosses": settings.propagate_detailed_losses,
            "numOfSimulations": settings.num_of_simulations,
            "regionPerilSimulationSet": region_peril,
        }
        optional_fields = (
            ("description", settings.description),
            ("reportingWindowStart", settings.reporting_window_start),
            ("simulationWindowStart", settings.simulation_window_start),
            ("simulationWindowEnd", settings.simulation_window_end),
        )
        for field_name, value in optional_fields:
            if value is not None:
                request_settings[field_name] = value
        return {
            "resourceType": "analyses",
            "resourceUris": list(inspection.resource_uris),
            "settings": request_settings,
        }

    def _build_region_peril_simulation_set(
        self,
        inspection: GroupingInspection,
        selected_schemes: Dict[GroupingPartitionKey, int],
    ) -> List[Dict[str, Any]]:
        conflicting = any(
            partition.event_rate_selection_required for partition in inspection.partitions
        )
        if not inspection.simulate_to_plt and not conflicting:
            return []

        mappings = {
            (
                mapping.partition,
                mapping.engine_version,
                mapping.model_region_code,
                mapping.event_rate_scheme_id,
            ): mapping
            for mapping in inspection.simulation_mappings
        }
        entries: Dict[str, Dict[str, Any]] = {}
        for member in inspection.members:
            for fact in member.regions:
                key = GroupingPartitionKey(
                    fact.peril_code, fact.region_code, fact.model_version
                )
                if fact.framework == "PLT":
                    if fact.pet_id is None or fact.periods is None:
                        continue
                    entry = {
                        "engineVersion": fact.engine_version,
                        "eventRateSchemeId": 0,
                        "modelRegionCode": fact.model_region_code,
                        "modelVersion": fact.model_version,
                        "perilCode": fact.peril_code,
                        "regionCode": fact.region_code,
                        "simulationPeriods": fact.periods,
                        "simulationSetId": fact.pet_id,
                    }
                else:
                    scheme_id = selected_schemes.get(key)
                    if scheme_id is None:
                        continue
                    if inspection.simulate_to_plt:
                        mapping = mappings[(
                            key, fact.engine_version, fact.model_region_code, scheme_id
                        )]
                        simulation_set_id = mapping.simulation_set_id
                        simulation_periods = mapping.simulation_periods
                    else:
                        simulation_set_id = 0
                        simulation_periods = 0
                    entry = {
                        "engineVersion": fact.engine_version,
                        "eventRateSchemeId": scheme_id,
                        "modelRegionCode": fact.model_region_code,
                        "modelVersion": fact.model_version,
                        "perilCode": fact.peril_code,
                        "regionCode": fact.region_code,
                        "simulationPeriods": simulation_periods,
                        "simulationSetId": simulation_set_id,
                    }
                canonical = json.dumps(entry, sort_keys=True, separators=(",", ":"))
                entries[canonical] = entry
        return [entries[key] for key in sorted(entries)]


__all__ = [
    "EventRateSchemeOption",
    "EventRateSelection",
    "GroupingCurrency",
    "GroupingInspection",
    "GroupingManager",
    "GroupingMember",
    "GroupingPartition",
    "GroupingPartitionKey",
    "GroupingProblem",
    "GroupingProblemCode",
    "GroupingRegionFact",
    "GroupingSettings",
    "GroupingSimulationMapping",
    "GroupingSubmission",
    "GroupingTreaty",
]
