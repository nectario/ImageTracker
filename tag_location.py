from __future__ import annotations

import argparse
import math
import re
import sys
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

import requests

from ImageTracker import Database, load_settings, parse_mysql_config


@dataclass(frozen=True)
class GeoPoint:
    latitude: float
    longitude: float


_STREET_SUFFIX_SYNONYMS: Dict[str, List[str]] = {
    "ave": ["ave", "avenue"],
    "avenue": ["avenue", "ave"],
    "st": ["st", "street"],
    "street": ["street", "st"],
    "rd": ["rd", "road"],
    "road": ["road", "rd"],
    "dr": ["dr", "drive"],
    "drive": ["drive", "dr"],
    "blvd": ["blvd", "boulevard"],
    "boulevard": ["boulevard", "blvd"],
    "ln": ["ln", "lane"],
    "lane": ["lane", "ln"],
    "ct": ["ct", "court"],
    "court": ["court", "ct"],
    "pl": ["pl", "place"],
    "place": ["place", "pl"],
    "pkwy": ["pkwy", "parkway"],
    "parkway": ["parkway", "pkwy"],
    "ter": ["ter", "terrace"],
    "terrace": ["terrace", "ter"],
    "cir": ["cir", "circle"],
    "circle": ["circle", "cir"],
}

_STREET_SUFFIX_TERMINATORS = set(_STREET_SUFFIX_SYNONYMS.keys())


def _normalize_category(raw: str) -> str:
    category = raw.strip()
    if not category:
        raise ValueError("Category cannot be empty.")
    return category


def _parse_gps_values(raw_values: Sequence[str]) -> GeoPoint:
    joined = " ".join(part.strip() for part in raw_values if part.strip())
    if not joined:
        raise ValueError("GPS value is empty.")

    normalized = joined.replace(",", " ")
    pieces = [part for part in normalized.split() if part]
    if len(pieces) != 2:
        raise ValueError("GPS must be two numbers: lat,lon or lat lon.")

    try:
        latitude = float(pieces[0])
        longitude = float(pieces[1])
    except ValueError as exc:
        raise ValueError("GPS values must be numeric.") from exc

    if not (-90.0 <= latitude <= 90.0):
        raise ValueError("Latitude must be between -90 and 90.")
    if not (-180.0 <= longitude <= 180.0):
        raise ValueError("Longitude must be between -180 and 180.")

    return GeoPoint(latitude=latitude, longitude=longitude)


def _tokenize_address(raw_address: str) -> List[str]:
    return [token for token in re.findall(r"[A-Za-z0-9]+", raw_address.lower()) if token]


def _build_street_token_groups(raw_address: str) -> List[List[str]]:
    tokens = _tokenize_address(raw_address)
    if not tokens:
        return []

    groups: List[List[str]] = []
    first_numeric_index = next((idx for idx, token in enumerate(tokens) if any(ch.isdigit() for ch in token)), None)
    if first_numeric_index is not None:
        groups.append([tokens[first_numeric_index]])
        tokens = tokens[first_numeric_index + 1 :]

    if not tokens:
        return groups

    street_tokens: List[str] = []
    for token in tokens:
        street_tokens.append(token)
        if token in _STREET_SUFFIX_TERMINATORS:
            break
        if len(street_tokens) >= 3:
            break

    for token in street_tokens:
        synonyms = _STREET_SUFFIX_SYNONYMS.get(token, [token])
        deduped = list(dict.fromkeys(synonyms))
        groups.append(deduped)

    return groups


def _haversine_meters(point_a: GeoPoint, point_b: GeoPoint) -> float:
    earth_radius_m = 6_371_000.0
    phi1 = math.radians(point_a.latitude)
    phi2 = math.radians(point_b.latitude)
    d_phi = math.radians(point_b.latitude - point_a.latitude)
    d_lambda = math.radians(point_b.longitude - point_a.longitude)
    a = math.sin(d_phi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2.0) ** 2
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
    return earth_radius_m * c


def _geocode_address(api_key: str, address: str) -> Optional[GeoPoint]:
    response = requests.get(
        "https://maps.googleapis.com/maps/api/geocode/json",
        params={"address": address, "key": api_key, "language": "en"},
        timeout=30,
    )
    if response.status_code >= 400:
        raise RuntimeError(f"Google geocoding failed ({response.status_code}): {response.text}")

    payload = response.json()
    status = str(payload.get("status") or "").upper()
    if status == "ZERO_RESULTS":
        return None
    if status != "OK":
        error_message = payload.get("error_message")
        if isinstance(error_message, str) and error_message.strip():
            raise RuntimeError(f"Google geocoding status={status}: {error_message.strip()}")
        raise RuntimeError(f"Google geocoding status={status}")

    results = payload.get("results") or []
    if not results:
        return None

    location = ((results[0] or {}).get("geometry") or {}).get("location") or {}
    latitude = location.get("lat")
    longitude = location.get("lng")
    if latitude is None or longitude is None:
        return None

    try:
        return GeoPoint(latitude=float(latitude), longitude=float(longitude))
    except (TypeError, ValueError):
        return None


def _select_rows_by_address_tokens(conn, token_groups: List[List[str]]) -> List[Dict[str, Any]]:
    if not token_groups:
        return []

    where_fragments = [
        "`IsDeleted` = 0",
        "`StreetAddress` IS NOT NULL",
        "TRIM(`StreetAddress`) <> ''",
    ]
    params: List[Any] = []
    for group in token_groups:
        if not group:
            continue
        group_fragments = []
        for token in group:
            group_fragments.append("LOWER(`StreetAddress`) LIKE %s")
            params.append(f"%{token.lower()}%")
        where_fragments.append(f"({' OR '.join(group_fragments)})")

    sql = f"""
        SELECT
            `Id`,
            `Source`,
            `DriveItemId`,
            `FileName`,
            `StreetAddress`,
            `Latitude`,
            `Longitude`,
            `Category`,
            `CategorySource`
        FROM `ImageAsset`
        WHERE {' AND '.join(where_fragments)}
    """

    with conn.cursor() as cursor:
        cursor.execute(sql, tuple(params))
        return cursor.fetchall()


def _select_rows_by_radius(conn, center: GeoPoint, radius_meters: float) -> List[Dict[str, Any]]:
    lat_delta = radius_meters / 111_320.0
    cos_lat = math.cos(math.radians(center.latitude))
    lon_delta = 180.0 if abs(cos_lat) < 1e-6 else radius_meters / (111_320.0 * abs(cos_lat))

    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                `Id`,
                `Source`,
                `DriveItemId`,
                `FileName`,
                `StreetAddress`,
                `Latitude`,
                `Longitude`,
                `Category`,
                `CategorySource`
            FROM `ImageAsset`
            WHERE `IsDeleted` = 0
              AND `Latitude` IS NOT NULL
              AND `Longitude` IS NOT NULL
              AND `Latitude` BETWEEN %s AND %s
              AND `Longitude` BETWEEN %s AND %s
            """,
            (
                center.latitude - lat_delta,
                center.latitude + lat_delta,
                center.longitude - lon_delta,
                center.longitude + lon_delta,
            ),
        )
        rows = cursor.fetchall()

    filtered: List[Dict[str, Any]] = []
    for row in rows:
        try:
            row_point = GeoPoint(latitude=float(row["Latitude"]), longitude=float(row["Longitude"]))
        except (TypeError, ValueError):
            continue
        distance = _haversine_meters(center, row_point)
        if distance <= radius_meters:
            filtered.append(row)
    return filtered


def _merge_rows(*row_sets: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: Dict[int, Dict[str, Any]] = {}
    for rows in row_sets:
        for row in rows:
            row_id = row.get("Id")
            if isinstance(row_id, int):
                merged[row_id] = row
    return list(merged.values())


def _apply_manual_category(conn, row_ids: List[int], category: str) -> int:
    if not row_ids:
        return 0
    placeholders = ", ".join(["%s"] * len(row_ids))
    sql = f"""
        UPDATE `ImageAsset`
        SET
            `Category` = %s,
            `CategorySource` = 'Manual',
            `UpdatedAtUtc` = UTC_TIMESTAMP()
        WHERE `Id` IN ({placeholders})
    """
    with conn.cursor() as cursor:
        cursor.execute(sql, tuple([category, *row_ids]))
        return cursor.rowcount


def _filter_unclassified_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    filtered: List[Dict[str, Any]] = []
    for row in rows:
        category = row.get("Category")
        if category is None:
            filtered.append(row)
            continue
        if isinstance(category, str) and not category.strip():
            filtered.append(row)
    return filtered


def _print_preview(rows: List[Dict[str, Any]]) -> None:
    preview = sorted(rows, key=lambda row: row.get("Id", 0))[:10]
    for row in preview:
        print(
            f"Id={row.get('Id')} FileName={row.get('FileName')} "
            f"StreetAddress={row.get('StreetAddress')} "
            f"Category={row.get('Category')} Source={row.get('CategorySource')}"
        )


def run_tagging(
    *,
    category: str,
    address: Optional[str],
    gps: Optional[GeoPoint],
    radius_meters: float,
    dry_run: bool,
    where_category_is_null: bool,
) -> int:
    settings = load_settings()
    mysql_config = parse_mysql_config(settings)
    database = Database(mysql_config)

    with database.connection() as conn:
        rows_from_address: List[Dict[str, Any]] = []
        rows_from_radius: List[Dict[str, Any]] = []

        if address:
            token_groups = _build_street_token_groups(address)
            rows_from_address = _select_rows_by_address_tokens(conn, token_groups)
            print(f"AddressMatchRows={len(rows_from_address)}")

            if settings.google_maps_api_key:
                geocoded = _geocode_address(settings.google_maps_api_key, address)
                if geocoded:
                    rows_from_radius = _select_rows_by_radius(conn, geocoded, radius_meters)
                    print(
                        f"GeocodedAddress={geocoded.latitude:.7f},{geocoded.longitude:.7f} "
                        f"RadiusMatchRows={len(rows_from_radius)}"
                    )
                else:
                    print("GeocodedAddress=NoResult")
            else:
                print("GeocodedAddress=SkippedNoGoogleKey")

        if gps:
            rows_from_radius = _select_rows_by_radius(conn, gps, radius_meters)
            print(f"GpsInput={gps.latitude:.7f},{gps.longitude:.7f} RadiusMatchRows={len(rows_from_radius)}")

        matched_rows = _merge_rows(rows_from_address, rows_from_radius)
        if where_category_is_null:
            before_count = len(matched_rows)
            matched_rows = _filter_unclassified_rows(matched_rows)
            print(f"CategoryNullFilter=1 Before={before_count} After={len(matched_rows)}")
        else:
            print("CategoryNullFilter=0")
        row_ids = [int(row["Id"]) for row in matched_rows]

        print(f"DB={mysql_config.database}")
        print(f"TotalMatchedRows={len(row_ids)}")
        if not row_ids:
            return 0

        _print_preview(matched_rows)
        if dry_run:
            print("DryRun=1 RowsTagged=0")
            return 0

        tagged_count = _apply_manual_category(conn, row_ids, category)
        print(f"DryRun=0 RowsTagged={tagged_count} Category={category} CategorySource=Manual")
        return 0


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="tag_location.py",
        description="Manual location tagging for ImageTracker rows.",
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--address",
        nargs="+",
        help="Address text. You can pass quoted or unquoted values.",
    )
    mode.add_argument(
        "--gps",
        nargs="+",
        help='GPS values as "lat,lon" or "lat lon".',
    )
    parser.add_argument("--category", required=True, help="Category label to apply.")
    parser.add_argument("--radius-meters", type=float, default=10.0, help="Radius for GPS/address geocode matching.")
    parser.add_argument("--dry-run", action="store_true", help="Preview matches without updating.")
    parser.add_argument(
        "--where-category-is-null",
        action="store_true",
        help="Only tag rows where Category is NULL or empty.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)

    try:
        category = _normalize_category(args.category)
        if args.radius_meters <= 0:
            raise ValueError("radius-meters must be > 0.")

        address_value = " ".join(args.address).strip() if args.address else None
        gps_value = _parse_gps_values(args.gps) if args.gps else None

        return run_tagging(
            category=category,
            address=address_value,
            gps=gps_value,
            radius_meters=float(args.radius_meters),
            dry_run=bool(args.dry_run),
            where_category_is_null=bool(args.where_category_is_null),
        )
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
