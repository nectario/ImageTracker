from __future__ import annotations

import pytest

from imagetracker.gps_extractor import _altitude_to_float, _dms_to_decimal, _rational_to_float


def test_rational_to_float_supports_fraction_tuple():
    assert _rational_to_float((3, 2)) == pytest.approx(1.5)


def test_dms_to_decimal_handles_western_hemisphere():
    value = _dms_to_decimal([(40, 1), (42, 1), (30, 1)], "W")
    assert value == pytest.approx(-40.7083333, rel=1e-6)


def test_altitude_to_float_handles_below_sea_level_ref():
    assert _altitude_to_float((10, 1), 1) == pytest.approx(-10.0)
