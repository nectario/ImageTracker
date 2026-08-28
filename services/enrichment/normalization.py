from __future__ import annotations

from dataclasses import dataclass, replace
import hashlib
import json
from pathlib import Path
import re
from typing import Any, Mapping, Sequence

from services.enrichment.models import GeocodeResolution, ReverseGeocodeResult


class NormalizationConfigurationError(ValueError):
    pass


@dataclass(frozen=True)
class LocationNormalizationRule:
    name: str
    city_equals: str | None
    state_in: tuple[str, ...]
    country_in: tuple[str, ...]
    street_contains_any: tuple[str, ...]
    original_street_number_in: tuple[str, ...]
    normalized_street_address: str


@dataclass(frozen=True)
class LocationNormalizationRuleset:
    rules: tuple[LocationNormalizationRule, ...]
    version: str


def _text(value: Any) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None


def _texts(value: Any) -> tuple[str, ...]:
    if isinstance(value, str):
        text = _text(value)
        return (text,) if text else ()
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return tuple(
            text
            for item in value
            if (text := _text(item)) is not None
        )
    return ()


def _ruleset_version(payload: Mapping[str, Any]) -> str:
    explicit = _text(payload.get("Version")) or _text(payload.get("version"))
    if explicit:
        return explicit[:64]
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return f"sha256:{hashlib.sha256(canonical.encode('utf-8')).hexdigest()[:16]}"


def load_location_normalization_rules(
    path: str | Path,
) -> LocationNormalizationRuleset:
    selected_path = Path(path)
    if not selected_path.is_file():
        return LocationNormalizationRuleset(rules=(), version="none")
    try:
        payload = json.loads(selected_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise NormalizationConfigurationError(
            "Location normalization rules could not be loaded"
        ) from exc
    if isinstance(payload, list):
        root: Mapping[str, Any] = {"Rules": payload}
        entries = payload
    elif isinstance(payload, Mapping):
        root = payload
        entries = payload.get("Rules", [])
    else:
        raise NormalizationConfigurationError(
            "Location normalization rules must be a JSON object or array"
        )
    if not isinstance(entries, list):
        raise NormalizationConfigurationError("Rules must be a JSON array")

    rules: list[LocationNormalizationRule] = []
    for index, item in enumerate(entries, start=1):
        if not isinstance(item, Mapping):
            continue
        normalized = _text(item.get("NormalizedStreetAddress"))
        if normalized is None:
            continue
        rules.append(
            LocationNormalizationRule(
                name=_text(item.get("Name")) or f"Rule{index}",
                city_equals=_text(item.get("CityEquals")),
                state_in=_texts(item.get("StateIn")),
                country_in=_texts(item.get("CountryIn")),
                street_contains_any=_texts(item.get("StreetContainsAny")),
                original_street_number_in=_texts(item.get("OriginalStreetNumberIn")),
                normalized_street_address=normalized,
            )
        )
    return LocationNormalizationRuleset(
        rules=tuple(rules),
        version=_ruleset_version(root),
    )


class LocationNormalizer:
    def __init__(self, ruleset: LocationNormalizationRuleset) -> None:
        self._ruleset = ruleset

    def normalize_result(self, result: ReverseGeocodeResult) -> ReverseGeocodeResult:
        if result.resolution is None:
            return result
        return result.with_resolution(self.normalize(result.resolution))

    def normalize(self, location: GeocodeResolution) -> GeocodeResolution:
        original = location.original_street_number or self._extract_street_number(
            location.street_address
        )
        matched = next(
            (
                rule
                for rule in self._ruleset.rules
                if self._matches(rule, location, original)
            ),
            None,
        )
        if matched is None:
            return location.with_normalization(
                street_address=location.street_address,
                original_street_number=original,
                rule_version=location.normalization_rule_version,
            )
        version = self._applied_rule_version(matched)
        normalized = location.with_normalization(
            street_address=matched.normalized_street_address,
            original_street_number=original,
            rule_version=version,
        )
        region = " ".join(
            value for value in (location.state, location.postal_code) if value
        )
        display_name = ", ".join(
            value
            for value in (
                matched.normalized_street_address,
                location.city,
                region or None,
                location.country,
            )
            if value
        )
        return replace(
            normalized,
            location_display_name=display_name or normalized.location_display_name,
        )

    def can_reuse(self, location: GeocodeResolution) -> bool:
        """Reject legacy normalized rows when their current rule is no longer valid."""

        raw = location.raw_provider_json
        if isinstance(raw.get("OriginalAddress"), Mapping):
            return True
        if location.normalization_rule_version is None:
            return True
        original = location.original_street_number or self._extract_street_number(
            location.street_address
        )
        matched = next(
            (
                rule
                for rule in self._ruleset.rules
                if self._matches(rule, location, original)
            ),
            None,
        )
        return (
            matched is not None
            and self._applied_rule_version(matched)
            == location.normalization_rule_version
        )

    def _applied_rule_version(self, rule: LocationNormalizationRule) -> str:
        suffix = self._ruleset.version
        available = max(1, 64 - len(suffix) - 1)
        return f"{rule.name[:available]}@{suffix}"

    @staticmethod
    def _matches(
        rule: LocationNormalizationRule,
        location: GeocodeResolution,
        original_street_number: str | None,
    ) -> bool:
        city = (location.city or "").strip().casefold()
        state = (location.state or "").strip().casefold()
        country = (location.country or "").strip().casefold()
        street = (location.street_address or "").strip().casefold()
        original = (original_street_number or "").strip().casefold()
        if rule.city_equals and city != rule.city_equals.strip().casefold():
            return False
        if rule.state_in and state not in {value.casefold() for value in rule.state_in}:
            return False
        if rule.country_in and country not in {
            value.casefold() for value in rule.country_in
        }:
            return False
        if rule.street_contains_any and not any(
            fragment.casefold() in street for fragment in rule.street_contains_any
        ):
            return False
        if rule.original_street_number_in and original not in {
            value.casefold() for value in rule.original_street_number_in
        }:
            return False
        return True

    @staticmethod
    def _extract_street_number(street_address: str | None) -> str | None:
        if not street_address:
            return None
        match = re.match(r"^\s*(\d+[A-Za-z\-]?)\b", street_address)
        return match.group(1) if match else None
