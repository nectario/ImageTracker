# ImageTracker

ImageTracker is a single Python script (`ImageTracker.py`) that syncs local photos into MySQL using PascalCase schema names.

## Script Location

- `ImageTracker.py` at repository root.
- No `imagetracker/` module package is required.

## Run

Use `python` (not `python3`):

```bash
python ImageTracker.py --directory "/mnt/d/Pictures/Camera Uploads" --cutoff-date "2026-01-01"
```

Force reprocessing of previously processed files:

```bash
python ImageTracker.py --directory "/mnt/d/Pictures/Camera Uploads" --cutoff-date "2026-01-01" --force
```

Manual location tagging (separate script):

```bash
python tag_location.py --address 99 Prospect Ave Bayonne NJ 07002 --category "Home"
python tag_location.py --gps 40.6631583333,-74.1143888889 --category "Kids at the Park"
```

Useful options:

- `--radius-meters` (default `10`)
- `--dry-run` (preview matching rows before update)
- `--where-category-is-null` (only fill unclassified rows)
- Tags are stored as `CategorySource='Manual'`.

## Local Sync Behavior

1. Scans supported image files recursively in `--directory`.
2. Processes files with modified timestamp >= `--cutoff-date`.
3. Skips already processed files (`Source='LocalFile'`) unless `--force` is supplied.
4. Stores file names exactly as-is (for example: `IMG_8677.JPG`).
5. Extracts GPS primarily from EXIF metadata in the local file bytes.
6. Optionally generates `Description` when `OPENAI_API_KEY` is set.
7. Optionally reverse-geocodes GPS to location fields when `GOOGLE_MAPS_API_KEY` is set.
8. Applies configurable location normalization rules from `location_normalization_rules.json` (or `LOCATION_NORMALIZATION_RULES_PATH`).
9. Persists capture-time analytics fields: `DateTime`, `Date`, `Time`, `TimeZone`, `UtcOffsetMinutes`, and `DateTimeUtc`.
10. Enriches missing `TimeZone`/`UtcOffsetMinutes` for GPS rows via Google Time Zone API.
11. Auto-assigns `Category` from existing tagged photos using `StreetAddress` first, then a 10-meter GPS radius fallback.
12. Prints periodic progress lines during long scans so the importer does not appear stalled.

## Database

Migrations are applied automatically from `migrations/`.

Expected tables:

- `ImageAsset`
- `OneDriveSyncState`
- `OneDriveTokenCache`
- `SchemaMigration`

## Environment

Copy `.env.example` to `.env` and set values.

MySQL resolution order when `MYSQL_DSN` is not set:

- Host: `IMAGETRACKER_MYSQL_HOST` -> `MYSQL_HOST`
- Port: `IMAGETRACKER_MYSQL_PORT` -> `MYSQL_PORT`
- User: `IMAGETRACKER_MYSQL_USER` -> `MYSQL_USERID` -> `MYSQL_USER`
- Password: `IMAGETRACKER_MYSQL_PASSWORD` -> `MYSQL_PASSWORD`
- Database: `IMAGETRACKER_MYSQL_DATABASE` -> `MYSQL_DATABASE_IMAGETRACKER` -> `MYSQL_DATABASE`

Optional captioning:

- `OPENAI_API_KEY`
- `OPENAI_VISION_MODEL` (default: `gpt-5.2`)
- `PHOTO_CAPTION_MAX_WORDS` (default: `18`)

Optional location enrichment:

- `GOOGLE_MAPS_API_KEY`
- `LOCATION_NORMALIZATION_RULES_PATH` (default: `location_normalization_rules.json`)
- Populates `LocationDisplayName`, `StreetAddress`, `Neighborhood`, `City`, `County`,
  `State`, `PostalCode`, `Country`, `CountryCode`, `OriginalStreetNumber`,
  `LocationProvider`, `LocationUpdatedAtUtc`.
- Enable both Google APIs in the same project: Geocoding API and Time Zone API.

Category fields:

- `Category` (`VARCHAR(255)`): optional location bucket such as `Home`.
- `CategorySource` (`VARCHAR(64)`): provenance (`Manual`, `AddressPropagation`, `RadiusPropagation10m`, etc.).
- Rows without GPS/address (for example screenshots) remain unclassified unless manually tagged.

Location normalization rules:

- File format is JSON with a top-level `Rules` array.
- Current default rule canonicalizes Bayonne `Prospect Ave/Avenue` edge numbers
  (`97/99/101/103`) to `99 Prospect Avenue`, preserving `OriginalStreetNumber`.

Capture-time fields:

- `DateTime`: local capture date/time (EXIF-first, fallback to file modified time converted to local timezone).
- `Date`: derived from `DateTime`.
- `Time`: derived from `DateTime`.
- `TimeZone`: timezone identifier or UTC offset label.
- `UtcOffsetMinutes`: offset minutes at capture time.
- `DateTimeUtc`: UTC companion timestamp for stable cross-timezone ordering.
- `CreatedAt` / `ModifiedAt`: MySQL `TIMESTAMP` columns for row lifecycle.
