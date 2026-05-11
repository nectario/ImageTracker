from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
import json
import math
from pathlib import Path
from typing import Any, Dict, Optional

import pytest

from ImageTracker import (
    CaptionResult,
    CaptureDateTimeInfo,
    ExifGpsExtractor,
    GpsCoordinates,
    LocationNormalizationRule,
    LocationResolution,
    LocalPhotoSyncService,
    Settings,
    TimeZoneResolution,
    _drive_item_id_for_path,
    load_location_normalization_rules,
    parse_cutoff_date,
)


@dataclass
class DummyConnection:
    closed: bool = False

    def close(self):
        self.closed = True


class FakeDatabase:
    @contextmanager
    def connection(self):
        yield DummyConnection()

    def connect(self, *, autocommit: bool = False):
        return DummyConnection()


class FakeMigrationRunner:
    def __init__(self):
        self.called = 0

    def apply_all(self, conn):
        self.called += 1


class FakeImageRepository:
    def __init__(self):
        self.rows: Dict[str, Dict[str, Any]] = {}
        self.upserts = []
        self.caption_updates = []
        self.location_updates = []
        self.category_updates = []

    def get_by_source_and_drive_item(self, conn, source: str, drive_item_id: str):
        return self.rows.get(drive_item_id)

    def upsert(self, conn, payload):
        row = self.rows.get(payload.drive_item_id, {}).copy()
        row.update(
            {
                "Source": payload.source,
                "DriveItemId": payload.drive_item_id,
                "FileName": payload.file_name,
                "Latitude": payload.latitude,
                "Longitude": payload.longitude,
                "Altitude": payload.altitude,
                "Description": row.get("Description"),
                "DescriptionModel": row.get("DescriptionModel"),
                "DescriptionUpdatedAtUtc": row.get("DescriptionUpdatedAtUtc"),
                "LocationDisplayName": row.get("LocationDisplayName"),
                "StreetAddress": row.get("StreetAddress"),
                "OriginalStreetNumber": row.get("OriginalStreetNumber"),
                "Neighborhood": row.get("Neighborhood"),
                "City": row.get("City"),
                "County": row.get("County"),
                "State": row.get("State"),
                "PostalCode": row.get("PostalCode"),
                "Country": row.get("Country"),
                "CountryCode": row.get("CountryCode"),
                "Category": row.get("Category"),
                "CategorySource": row.get("CategorySource"),
                "LocationProvider": row.get("LocationProvider"),
                "LocationUpdatedAtUtc": row.get("LocationUpdatedAtUtc"),
            }
        )
        self.rows[payload.drive_item_id] = row
        self.upserts.append(payload)

    def update_caption(
        self,
        conn,
        source: str,
        drive_item_id: str,
        description: str,
        description_model: str,
        updated_at_utc: datetime,
    ):
        row = self.rows.setdefault(drive_item_id, {})
        row["Description"] = description
        row["DescriptionModel"] = description_model
        row["DescriptionUpdatedAtUtc"] = updated_at_utc
        self.caption_updates.append((source, drive_item_id, description, description_model))

    def update_location(
        self,
        conn,
        source: str,
        drive_item_id: str,
        location: LocationResolution,
        location_provider: str,
        updated_at_utc: datetime,
    ):
        row = self.rows.setdefault(drive_item_id, {})
        row["LocationDisplayName"] = location.location_display_name
        row["StreetAddress"] = location.street_address
        row["OriginalStreetNumber"] = location.original_street_number
        row["Neighborhood"] = location.neighborhood
        row["City"] = location.city
        row["County"] = location.county
        row["State"] = location.state
        row["PostalCode"] = location.postal_code
        row["Country"] = location.country
        row["CountryCode"] = location.country_code
        row["LocationProvider"] = location_provider
        row["LocationUpdatedAtUtc"] = updated_at_utc
        self.location_updates.append((source, drive_item_id, location.location_display_name, location_provider))

    def update_category(
        self,
        conn,
        source: str,
        drive_item_id: str,
        category: str,
        category_source: str,
        updated_at_utc: datetime,
    ):
        row = self.rows.setdefault(drive_item_id, {})
        row["Category"] = category
        row["CategorySource"] = category_source
        self.category_updates.append((source, drive_item_id, category, category_source, updated_at_utc))

    def find_category_by_street_address(
        self,
        conn,
        *,
        street_address: str,
        exclude_source: str,
        exclude_drive_item_id: str,
    ) -> Optional[str]:
        matches = []
        for drive_item_id, row in self.rows.items():
            if row.get("Source") == exclude_source and drive_item_id == exclude_drive_item_id:
                continue
            if row.get("StreetAddress") != street_address:
                continue
            category = row.get("Category")
            if not category:
                continue
            source = str(row.get("CategorySource") or "")
            is_manual = source.strip().lower() == "manual"
            matches.append((0 if is_manual else 1, -int(bool(row.get("ModifiedAt"))), str(category).strip()))
        if not matches:
            return None
        matches.sort()
        category = matches[0][2]
        return category or None

    def find_category_by_radius(
        self,
        conn,
        *,
        latitude: float,
        longitude: float,
        radius_meters: float,
        exclude_source: str,
        exclude_drive_item_id: str,
    ) -> Optional[str]:
        best_category = None
        best_distance = None
        best_manual = False
        for drive_item_id, row in self.rows.items():
            if row.get("Source") == exclude_source and drive_item_id == exclude_drive_item_id:
                continue
            category = row.get("Category")
            row_lat = row.get("Latitude")
            row_lon = row.get("Longitude")
            if not category or row_lat is None or row_lon is None:
                continue

            distance = self._haversine_meters(latitude, longitude, float(row_lat), float(row_lon))
            if distance > radius_meters:
                continue

            is_manual = str(row.get("CategorySource") or "").strip().lower() == "manual"
            if best_distance is None or distance < best_distance:
                best_category = str(category).strip()
                best_distance = distance
                best_manual = is_manual
                continue
            if distance == best_distance and is_manual and not best_manual:
                best_category = str(category).strip()
                best_manual = True

        return best_category

    @staticmethod
    def _haversine_meters(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        earth_radius_m = 6_371_000.0
        phi1 = math.radians(lat1)
        phi2 = math.radians(lat2)
        d_phi = math.radians(lat2 - lat1)
        d_lambda = math.radians(lon2 - lon1)
        a = math.sin(d_phi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2.0) ** 2
        c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
        return earth_radius_m * c


class FakeCaptioner:
    def __init__(self, model: str = "gpt-5.2"):
        self.model = model
        self.calls = 0

    def generate_caption(self, image_bytes: bytes):
        self.calls += 1
        return CaptionResult(short_description="A local photo description.", model=self.model)


class FakeGpsExtractor:
    def __init__(self, result=None):
        self.result = result
        self.calls = []

    def extract(self, content_bytes: bytes, *, mime_type: Optional[str], file_name: str):
        self.calls.append((content_bytes, mime_type, file_name))
        return self.result


class FakeCaptureDateTimeExtractor:
    def __init__(self, result: Optional[CaptureDateTimeInfo] = None):
        self.result = result
        self.calls = []

    def extract(self, content_bytes: bytes, *, mime_type: Optional[str], file_name: str):
        self.calls.append((content_bytes, mime_type, file_name))
        return self.result


class FakeTimeZoneResolver:
    provider = "GoogleMapsTimeZone"

    def __init__(self, result: Optional[TimeZoneResolution] = None):
        self.result = result or TimeZoneResolution(
            time_zone_id="America/New_York",
            utc_offset_minutes=-300,
        )
        self.calls = []

    def resolve_timezone(self, *, latitude: float, longitude: float, timestamp_utc: datetime):
        self.calls.append((latitude, longitude, timestamp_utc))
        return self.result


class FakeLocationResolver:
    provider = "GoogleMapsGeocoding"

    def __init__(self, result: Optional[LocationResolution] = None):
        self.result = result or LocationResolution(
            location_display_name="Staten Island, NY, USA",
            street_address="123 Main St",
            original_street_number="123",
            neighborhood="Arden Heights",
            city="Staten Island",
            county="Richmond County",
            state="New York",
            postal_code="10312",
            country="United States",
            country_code="US",
        )
        self.calls = []

    def reverse_geocode(self, latitude: float, longitude: float) -> Optional[LocationResolution]:
        self.calls.append((latitude, longitude))
        return self.result


class UnavailableGpsExtractor:
    is_available = False

    def extract(self, content_bytes: bytes, *, mime_type: Optional[str], file_name: str):
        raise AssertionError("Should not be called when extractor is unavailable")


def make_settings() -> Settings:
    return Settings(
        onedrive_tenant="consumers",
        onedrive_client_id="client-id",
        onedrive_scopes=["User.Read", "Files.Read.All"],
        onedrive_camera_upload_path="/Pictures/Camera Roll",
        onedrive_camera_upload_fallback_paths=[],
        mysql_dsn="mysql://user:password@localhost:3306/db",
        openai_api_key=None,
        google_maps_api_key=None,
        location_normalization_rules_path="location_normalization_rules.json",
        openai_vision_model="gpt-5.2",
        photo_sync_initial_cutoff_days=14,
        photo_caption_max_words=18,
    )


def build_service(
    *,
    image_repo: Optional[FakeImageRepository] = None,
    captioner: Optional[FakeCaptioner] = None,
    gps_extractor=None,
    capture_datetime_extractor=None,
    location_resolver=None,
    time_zone_resolver=None,
    location_normalization_rules=None,
    progress_now_fn=None,
):
    now = datetime(2026, 2, 23, 12, 0, 0)
    image_repo = image_repo or FakeImageRepository()

    service = LocalPhotoSyncService(
        settings=make_settings(),
        database=FakeDatabase(),
        migration_runner=FakeMigrationRunner(),
        image_repo=image_repo,
        captioner=captioner,
        gps_extractor=gps_extractor,
        capture_datetime_extractor=capture_datetime_extractor,
        location_resolver=location_resolver,
        time_zone_resolver=time_zone_resolver,
        location_normalization_rules=location_normalization_rules,
        now_fn=lambda: now,
        progress_now_fn=progress_now_fn or (lambda: 0.0),
    )

    return service, image_repo


def set_file_mtime(path: Path, dt_utc: datetime) -> None:
    ts = dt_utc.replace(tzinfo=timezone.utc).timestamp()
    path.touch(exist_ok=True)
    path.write_bytes(b"fake-image-bytes")
    import os

    os.utime(path, (ts, ts))


def test_parse_cutoff_date_accepts_date_only():
    cutoff = parse_cutoff_date("2026-02-01")
    assert isinstance(cutoff, datetime)


def test_load_location_normalization_rules_from_json(tmp_path: Path):
    rules_path = tmp_path / "rules.json"
    rules_path.write_text(
        json.dumps(
            {
                "Rules": [
                    {
                        "Name": "TestRule",
                        "CityEquals": "Bayonne",
                        "StateIn": ["New Jersey", "NJ"],
                        "StreetContainsAny": ["Prospect Avenue"],
                        "OriginalStreetNumberIn": ["99"],
                        "NormalizedStreetAddress": "99 Prospect Avenue",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    rules = load_location_normalization_rules(rules_path)

    assert len(rules) == 1
    assert rules[0].name == "TestRule"
    assert rules[0].city_equals == "Bayonne"
    assert rules[0].normalized_street_address == "99 Prospect Avenue"


def test_skip_processed_files_without_force(tmp_path: Path):
    old_file = tmp_path / "IMG_0001.JPG"
    new_file = tmp_path / "IMG_0002.JPG"

    cutoff_utc = datetime(2026, 2, 20, 0, 0, 0)
    set_file_mtime(old_file, cutoff_utc - timedelta(days=2))
    set_file_mtime(new_file, cutoff_utc + timedelta(hours=1))

    image_repo = FakeImageRepository()
    existing_id = _drive_item_id_for_path(new_file)
    image_repo.rows[existing_id] = {
        "Source": "LocalFile",
        "DriveItemId": existing_id,
        "Description": "Already processed.",
    }

    service, _ = build_service(image_repo=image_repo, gps_extractor=UnavailableGpsExtractor())

    result = service.run_sync(directory=tmp_path, cutoff_utc=cutoff_utc, force=False)

    assert result.scanned_count == 2
    assert result.eligible_count == 1
    assert result.skipped_count == 1
    assert result.upserted_count == 0
    assert result.geocoded_count == 0
    assert result.timezone_enriched_count == 0


def test_run_sync_prints_periodic_progress(tmp_path: Path, capsys):
    first_photo = tmp_path / "IMG_0001.JPG"
    second_photo = tmp_path / "IMG_0002.JPG"
    cutoff_utc = datetime(2026, 2, 20, 0, 0, 0)
    set_file_mtime(first_photo, cutoff_utc + timedelta(hours=1))
    set_file_mtime(second_photo, cutoff_utc + timedelta(hours=2))

    progress_times = iter([0.0, 0.0, 6.0])

    service, _ = build_service(
        gps_extractor=UnavailableGpsExtractor(),
        progress_now_fn=lambda: next(progress_times),
    )

    result = service.run_sync(directory=tmp_path, cutoff_utc=cutoff_utc, force=False)

    captured = capsys.readouterr()
    assert result.upserted_count == 2
    assert "Local sync progress: started" in captured.out
    assert "Local sync progress: elapsed=6s scanned=2" in captured.out
    assert "current=IMG_0002.JPG" in captured.out


def test_force_reprocesses_existing_and_preserves_filename(tmp_path: Path):
    photo = tmp_path / "IMG_8677.JPG"
    cutoff_utc = datetime(2026, 2, 20, 0, 0, 0)
    set_file_mtime(photo, cutoff_utc + timedelta(hours=1))

    image_repo = FakeImageRepository()
    drive_item_id = _drive_item_id_for_path(photo)
    image_repo.rows[drive_item_id] = {
        "Source": "LocalFile",
        "DriveItemId": drive_item_id,
        "Description": "Old caption.",
        "DescriptionModel": "old-model",
        "DescriptionUpdatedAtUtc": datetime(2026, 2, 20, 0, 0, 0),
        "Latitude": None,
        "Longitude": None,
        "Altitude": None,
    }

    gps_extractor = FakeGpsExtractor(
        GpsCoordinates(latitude=37.7749, longitude=-122.4194, altitude=10.0)
    )
    captioner = FakeCaptioner()

    service, image_repo = build_service(
        image_repo=image_repo,
        captioner=captioner,
        gps_extractor=gps_extractor,
    )

    result = service.run_sync(directory=tmp_path, cutoff_utc=cutoff_utc, force=True)

    assert result.scanned_count == 1
    assert result.eligible_count == 1
    assert result.skipped_count == 0
    assert result.upserted_count == 1
    assert result.captioned_count == 1
    assert result.geocoded_count == 0
    assert result.timezone_enriched_count == 0
    assert len(image_repo.upserts) == 1

    payload = image_repo.upserts[0]
    assert payload.file_name == "IMG_8677.JPG"
    assert payload.latitude == pytest.approx(37.7749)
    assert payload.longitude == pytest.approx(-122.4194)
    assert payload.altitude == pytest.approx(10.0)

    assert captioner.calls == 1
    assert len(gps_extractor.calls) == 1


def test_geocode_runs_when_gps_present_and_location_missing(tmp_path: Path):
    photo = tmp_path / "IMG_9000.JPG"
    cutoff_utc = datetime(2026, 2, 20, 0, 0, 0)
    set_file_mtime(photo, cutoff_utc + timedelta(hours=1))

    gps_extractor = FakeGpsExtractor(
        GpsCoordinates(latitude=40.6631583333, longitude=-74.1143888889, altitude=6.4)
    )
    location_resolver = FakeLocationResolver()

    service, image_repo = build_service(
        gps_extractor=gps_extractor,
        location_resolver=location_resolver,
    )

    result = service.run_sync(directory=tmp_path, cutoff_utc=cutoff_utc, force=False)

    assert result.upserted_count == 1
    assert result.geocoded_count == 1
    assert result.timezone_enriched_count == 0
    assert len(location_resolver.calls) == 1
    assert len(image_repo.location_updates) == 1

    drive_item_id = _drive_item_id_for_path(photo)
    row = image_repo.rows[drive_item_id]
    assert row["LocationDisplayName"] == "Staten Island, NY, USA"
    assert row["State"] == "New York"
    assert row["CountryCode"] == "US"
    assert row["OriginalStreetNumber"] == "123"


def test_normalizes_bayonne_prospect_to_99_and_preserves_original_number(tmp_path: Path):
    photo = tmp_path / "IMG_9001.JPG"
    cutoff_utc = datetime(2026, 2, 20, 0, 0, 0)
    set_file_mtime(photo, cutoff_utc + timedelta(hours=1))

    gps_extractor = FakeGpsExtractor(
        GpsCoordinates(latitude=40.6631583333, longitude=-74.1143888889, altitude=6.4)
    )
    location_resolver = FakeLocationResolver(
        LocationResolution(
            location_display_name="Bayonne, New Jersey, United States",
            street_address="101 Prospect Avenue",
            original_street_number="101",
            neighborhood=None,
            city="Bayonne",
            county="Hudson County",
            state="New Jersey",
            postal_code="07002",
            country="United States",
            country_code="US",
        )
    )
    normalization_rules = [
        LocationNormalizationRule(
            name="BayonneProspectCanonical99",
            city_equals="Bayonne",
            state_in=["New Jersey", "NJ"],
            country_in=["United States", "USA"],
            street_contains_any=["Prospect Avenue", "Prospect Ave"],
            original_street_number_in=["97", "99", "101", "103"],
            normalized_street_address="99 Prospect Avenue",
        )
    ]

    service, image_repo = build_service(
        gps_extractor=gps_extractor,
        location_resolver=location_resolver,
        location_normalization_rules=normalization_rules,
    )

    result = service.run_sync(directory=tmp_path, cutoff_utc=cutoff_utc, force=False)
    assert result.geocoded_count == 1
    assert result.timezone_enriched_count == 0

    drive_item_id = _drive_item_id_for_path(photo)
    row = image_repo.rows[drive_item_id]
    assert row["StreetAddress"] == "99 Prospect Avenue"
    assert row["OriginalStreetNumber"] == "101"


def test_category_propagates_from_matching_street_address(tmp_path: Path):
    photo = tmp_path / "IMG_9100.JPG"
    cutoff_utc = datetime(2026, 2, 20, 0, 0, 0)
    set_file_mtime(photo, cutoff_utc + timedelta(hours=1))

    gps_extractor = FakeGpsExtractor(
        GpsCoordinates(latitude=40.6631583333, longitude=-74.1143888889, altitude=6.4)
    )
    location_resolver = FakeLocationResolver(
        LocationResolution(
            location_display_name="Bayonne, New Jersey, United States",
            street_address="99 Prospect Avenue",
            original_street_number="99",
            neighborhood=None,
            city="Bayonne",
            county="Hudson County",
            state="New Jersey",
            postal_code="07002",
            country="United States",
            country_code="US",
        )
    )

    image_repo = FakeImageRepository()
    image_repo.rows["seed-home"] = {
        "Source": "LocalFile",
        "DriveItemId": "seed-home",
        "StreetAddress": "99 Prospect Avenue",
        "Category": "Home",
        "CategorySource": "Manual",
        "IsDeleted": 0,
    }

    service, image_repo = build_service(
        image_repo=image_repo,
        gps_extractor=gps_extractor,
        location_resolver=location_resolver,
    )

    result = service.run_sync(directory=tmp_path, cutoff_utc=cutoff_utc, force=False)
    assert result.upserted_count == 1
    assert result.geocoded_count == 1

    drive_item_id = _drive_item_id_for_path(photo)
    row = image_repo.rows[drive_item_id]
    assert row["Category"] == "Home"
    assert row["CategorySource"] == "AddressPropagation"
    assert len(image_repo.category_updates) == 1


def test_category_uses_radius_fallback_when_address_missing(tmp_path: Path):
    photo = tmp_path / "IMG_9101.JPG"
    cutoff_utc = datetime(2026, 2, 20, 0, 0, 0)
    set_file_mtime(photo, cutoff_utc + timedelta(hours=1))

    gps_extractor = FakeGpsExtractor(
        GpsCoordinates(latitude=40.6631583333, longitude=-74.1143888889, altitude=6.4)
    )
    location_resolver = FakeLocationResolver(
        LocationResolution(
            location_display_name="Bayonne, New Jersey, United States",
            street_address=None,
            original_street_number=None,
            neighborhood=None,
            city="Bayonne",
            county="Hudson County",
            state="New Jersey",
            postal_code="07002",
            country="United States",
            country_code="US",
        )
    )

    image_repo = FakeImageRepository()
    image_repo.rows["seed-radius"] = {
        "Source": "LocalFile",
        "DriveItemId": "seed-radius",
        "Latitude": 40.6631583333,
        "Longitude": -74.1143888889,
        "Category": "Home",
        "CategorySource": "Manual",
        "IsDeleted": 0,
    }

    service, image_repo = build_service(
        image_repo=image_repo,
        gps_extractor=gps_extractor,
        location_resolver=location_resolver,
    )

    result = service.run_sync(directory=tmp_path, cutoff_utc=cutoff_utc, force=False)
    assert result.upserted_count == 1
    assert result.geocoded_count == 1

    drive_item_id = _drive_item_id_for_path(photo)
    row = image_repo.rows[drive_item_id]
    assert row["Category"] == "Home"
    assert row["CategorySource"] == "RadiusPropagation10m"
    assert len(image_repo.category_updates) == 1


def test_timezone_enrichment_runs_for_gps_rows_when_capture_offset_missing(tmp_path: Path):
    photo = tmp_path / "IMG_9010.JPG"
    cutoff_utc = datetime(2026, 2, 20, 0, 0, 0)
    set_file_mtime(photo, cutoff_utc + timedelta(hours=1))

    gps_extractor = FakeGpsExtractor(
        GpsCoordinates(latitude=40.6631583333, longitude=-74.1143888889, altitude=6.4)
    )
    capture_datetime_extractor = FakeCaptureDateTimeExtractor(
        CaptureDateTimeInfo(
            local_datetime=datetime(2026, 2, 23, 17, 45, 42),
            time_zone=None,
            utc_offset_minutes=None,
            utc_datetime=datetime(2026, 2, 23, 22, 45, 42),
        )
    )
    time_zone_resolver = FakeTimeZoneResolver(
        TimeZoneResolution(time_zone_id="America/New_York", utc_offset_minutes=-300)
    )

    service, image_repo = build_service(
        gps_extractor=gps_extractor,
        capture_datetime_extractor=capture_datetime_extractor,
        time_zone_resolver=time_zone_resolver,
    )

    result = service.run_sync(directory=tmp_path, cutoff_utc=cutoff_utc, force=False)

    assert result.upserted_count == 1
    assert result.timezone_enriched_count == 1
    assert len(time_zone_resolver.calls) == 1

    payload = image_repo.upserts[0]
    assert payload.date_time == datetime(2026, 2, 23, 17, 45, 42)
    assert payload.date_value == date(2026, 2, 23)
    assert payload.time_value == time(17, 45, 42)
    assert payload.time_zone == "America/New_York"
    assert payload.utc_offset_minutes == -300
    assert payload.date_time_utc == datetime(2026, 2, 23, 22, 45, 42)


def test_exif_gps_extractor_reads_gps_ifd_when_gpsinfo_is_offset():
    gps_tag_id = 34853
    gps_tags = {
        1: "GPSLatitudeRef",
        2: "GPSLatitude",
        3: "GPSLongitudeRef",
        4: "GPSLongitude",
        5: "GPSAltitudeRef",
        6: "GPSAltitude",
    }

    class DummyExif(dict):
        def __init__(self):
            super().__init__({gps_tag_id: 2732})

        def get_ifd(self, tag_id: int):
            if tag_id != gps_tag_id:
                return {}
            return {
                1: "N",
                2: (40.0, 39.0, 47.37),
                3: "W",
                4: (74.0, 6.0, 51.8),
                5: b"\x00",
                6: 6.429310507007807,
            }

    class DummyImage:
        def getexif(self):
            return DummyExif()

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class DummyImageModule:
        @staticmethod
        def open(_):
            return DummyImage()

    extractor = ExifGpsExtractor()
    extractor._pil_available = True
    extractor._image_module = DummyImageModule()
    extractor._gps_tag_id = gps_tag_id
    extractor._gps_tags = gps_tags

    coords = extractor.extract(
        b"fake-bytes",
        mime_type="image/jpeg",
        file_name="IMG_0001.JPG",
    )

    assert coords is not None
    assert coords.latitude == pytest.approx(40.6631583333, rel=1e-6)
    assert coords.longitude == pytest.approx(-74.1143888889, rel=1e-6)
    assert coords.altitude == pytest.approx(6.4293105070, rel=1e-6)
