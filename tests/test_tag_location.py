from __future__ import annotations

import pytest

from tag_location import _build_street_token_groups, _filter_unclassified_rows, _parse_gps_values


def test_parse_gps_values_from_comma_input():
    point = _parse_gps_values(["40.6631583333,-74.1143888889"])
    assert point.latitude == pytest.approx(40.6631583333)
    assert point.longitude == pytest.approx(-74.1143888889)


def test_parse_gps_values_from_two_parts():
    point = _parse_gps_values(["40.6631583333", "-74.1143888889"])
    assert point.latitude == pytest.approx(40.6631583333)
    assert point.longitude == pytest.approx(-74.1143888889)


def test_build_street_token_groups_stops_after_suffix():
    groups = _build_street_token_groups("99 Prospect Ave Bayonne NJ 07002")
    assert groups == [["99"], ["prospect"], ["ave", "avenue"]]


def test_parse_gps_values_rejects_out_of_range():
    with pytest.raises(ValueError):
        _parse_gps_values(["101", "200"])


def test_filter_unclassified_rows_keeps_null_and_blank():
    rows = [
        {"Id": 1, "Category": None},
        {"Id": 2, "Category": ""},
        {"Id": 3, "Category": "   "},
        {"Id": 4, "Category": "Home"},
    ]
    filtered = _filter_unclassified_rows(rows)
    assert [row["Id"] for row in filtered] == [1, 2, 3]
