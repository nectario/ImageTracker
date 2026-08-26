# ImageTracker Handoff

Last updated: 2026-08-26

This document is for another Codex/model taking over the ImageTracker work. It captures current implementation state, user preferences, operational history, database state, and known pitfalls.

## Current Objective

ImageTracker is now a local Python photo importer that scans a large local photo folder, extracts reliable metadata, enriches location details, and stores analytics-friendly rows in MySQL.

The original Microsoft OneDrive/Graph delta design was superseded by the user's later decision to run locally against the long-term photo archive folder. Do not assume the active pipeline is OneDrive unless the user explicitly asks to revive it.

Active source folder:

```bash
/mnt/d/Pictures/Camera Uploads
```

Typical import command:

```bash
python ImageTracker.py --directory "/mnt/d/Pictures/Camera Uploads" --cutoff-date "2026-01-01"
```

Important: use `python`, not `python3`, because the user explicitly requested that.

## User Preferences And Constraints

- Work only on `main`. The user explicitly said: never create branches when implementing a solution.
- The remote default branch is `main`.
- Keep the codebase simple. The user rejected an `imagetracker/` package and internal `scripts/` directory.
- All active Python code should live at repository root unless the user changes this preference.
- The main importer must be a single root script named `ImageTracker.py`.
- Manual location tagging is allowed as a separate root script, currently `tag_location.py`.
- Preserve file names exactly as they are on disk. Do not normalize, rename, or infer alternate file names. Example: `IMG_8677.JPG` must remain exactly `IMG_8677.JPG`.
- MySQL table and column names must remain PascalCase.
- Avoid using `MYSQL_DATABASE` for this project because that env var was used by another project, TransactionAutoCategorizer. Use `MYSQL_DATABASE_IMAGETRACKER` or `IMAGETRACKER_MYSQL_DATABASE`.
- If using OpenAI, the model should be `gpt-5.2`.
- Screenshots/no-GPS/no-address rows should remain unclassified (`Category=NULL`) unless manually tagged.
- GPS is the primary reason for the project. Missing GPS is expected for screenshots and some received images, but real camera photos should usually have GPS.
- The user cares about local Date/Time analytics, not only UTC timestamps.

## Repository State

Workspace:

```bash
/mnt/c/development/projects/imagetracker
```

Observed real path casing:

```bash
/mnt/c/Development/Projects/ImageTracker
```

Git:

```text
branch: main
remote: origin git@github.com:nectario/ImageTracker.git
origin HEAD: refs/heads/main
latest commit: 79e765e Enhance local photo import and tagging
status before creating this handoff: clean, main tracking origin/main
status after creating this handoff: HANDOFF.md is untracked unless committed later
```

Recent commits:

```text
79e765e Enhance local photo import and tagging
94094a6 Restructure to single root ImageTracker.py script
f6632bc Add local directory photo sync command with cutoff and force options
a33db45 Document and test verbatim FileName persistence
1c9fe73 Prefer EXIF GPS extraction from image content with Graph fallback
```

Tracked files:

```text
ImageTracker.py
README.md
location_normalization_rules.json
migrations/001_CreatePhotoSyncTables.sql
migrations/002_RenameShortDescriptionColumns.sql
migrations/003_AddImageAssetLocationColumns.sql
migrations/004_AddImageAssetOriginalStreetNumber.sql
migrations/005_AddImageAssetTemporalColumns.sql
migrations/006_AddImageAssetCategoryColumns.sql
pyproject.toml
tag_location.py
tests/conftest.py
tests/test_local_photo_sync.py
tests/test_tag_location.py
```

## Tests

Latest test run at handoff:

```bash
pytest -q
```

Result:

```text
16 passed, 1 warning in 0.45s
```

Warning:

```text
requests dependency warning: urllib3/chardet version mismatch
```

The warning did not fail tests.

## Environment

`.env.example` documents the expected variables. `.env` is ignored by git.

At handoff, `.env` on disk only exposed the key name `GOOGLE_MAPS_API_KEY` when inspected without values. The shell/process environment resolved the MySQL settings and OpenAI settings.

Do not print or commit secret values.

Resolved non-secret DB identity at handoff:

```text
host: deeptrading-prod.ccx5bogmroze.us-east-2.rds.amazonaws.com
port: 3306
user: admin
database: ImageTracker
charset: utf8mb4
password: present in environment, value intentionally not recorded
```

OpenAI/Google flags at handoff:

```text
OPENAI_API_KEY: present in process environment, value intentionally not recorded
OPENAI_VISION_MODEL: gpt-5.2
GOOGLE_MAPS_API_KEY: present, value intentionally not recorded
```

MySQL env resolution order in `ImageTracker.py`:

```text
MYSQL_DSN, if set, wins.

Otherwise:
Host: IMAGETRACKER_MYSQL_HOST -> MYSQL_HOST -> 127.0.0.1
Port: IMAGETRACKER_MYSQL_PORT -> MYSQL_PORT -> 3306
User: IMAGETRACKER_MYSQL_USER -> MYSQL_USERID -> MYSQL_USER
Password: IMAGETRACKER_MYSQL_PASSWORD -> MYSQL_PASSWORD
Database: IMAGETRACKER_MYSQL_DATABASE -> MYSQL_DATABASE_IMAGETRACKER -> MYSQL_DATABASE
```

Use project-scoped vars where possible:

```bash
IMAGETRACKER_MYSQL_HOST=...
IMAGETRACKER_MYSQL_PORT=3306
IMAGETRACKER_MYSQL_USER=...
IMAGETRACKER_MYSQL_PASSWORD=...
IMAGETRACKER_MYSQL_DATABASE=ImageTracker
```

Compatibility vars the user specifically supplied earlier:

```bash
MYSQL_DATABASE_IMAGETRACKER
MYSQL_PORT
MYSQL_USERID
MYSQL_HOST
MYSQL_PASSWORD
```

Do not rely on `MYSQL_DATABASE` unless there is no alternative. It previously pointed to TransactionAutoCategorizer.

## Database

Database name:

```text
ImageTracker
```

MySQL time zones at handoff:

```text
session time_zone: UTC
global time_zone: UTC
system_time_zone: UTC
```

Tables:

```text
ImageAsset
OneDriveSyncState
OneDriveTokenCache
SchemaMigration
```

The OneDrive tables still exist because they were part of the original design, and each had one row at handoff. They are not used by the current active local importer.

Applied migrations:

```text
001_CreatePhotoSyncTables.sql          applied 2026-02-23 02:48:10 UTC
002_RenameShortDescriptionColumns.sql  applied 2026-02-23 03:34:55 UTC
003_AddImageAssetLocationColumns.sql   applied 2026-02-24 15:46:03 UTC
004_AddImageAssetOriginalStreetNumber.sql applied 2026-02-24 16:14:46 UTC
005_AddImageAssetTemporalColumns.sql   applied 2026-02-24 16:44:43 UTC
006_AddImageAssetCategoryColumns.sql   applied 2026-02-24 20:05:50 UTC
```

Exact `ImageAsset` counts at handoff:

```text
TotalRows: 2099
LocalFileRows: 2099
OneDriveRows: 0
DeletedRows: 0
RowsWithGps: 1098
RowsMissingGps: 1001
RowsWithDescription: 1543
RowsMissingDescription: 556
RowsWithLocation: 1098
RowsWithTimeZone: 1538
MinDateTime: 2025-12-09 12:06:13
MaxDateTime: 2026-05-10 17:54:29
MaxDateTimeUtc: 2026-05-10 21:54:29 from latest row query
MaxModifiedAt: 2026-05-11 01:03:45 UTC
```

Important implication: as of 2026-08-26, the database did not reflect photo imports after 2026-05-10. The robocopy/move job has logs through 2026-08-26, but ImageTracker import itself has not been reflected in DB after May 2026.

Category counts at handoff:

```text
Category NULL: 1469
Category Home: 630
```

Category source counts at handoff:

```text
CategorySource NULL: 1469
CategorySource AddressPropagation: 354
CategorySource Manual: 276
```

Prospect Avenue normalized/tagged state at handoff:

```text
99 Prospect Avenue, OriginalStreetNumber 99, Home, AddressPropagation: 325
99 Prospect Avenue, OriginalStreetNumber 99, Home, Manual: 256
99 Prospect Avenue, OriginalStreetNumber 101, Home, Manual: 17
99 Prospect Avenue, OriginalStreetNumber 101, Home, AddressPropagation: 16
99 Prospect Avenue, OriginalStreetNumber 97, Home, AddressPropagation: 9
99 Prospect Avenue, OriginalStreetNumber 103, Home, AddressPropagation: 4
99 Prospect Avenue, OriginalStreetNumber 97, Home, Manual: 2
99 Prospect Avenue, OriginalStreetNumber 103, Home, Manual: 1
```

There are also a small number of unclassified Prospect Avenue rows at other street numbers such as 73, 122, 57, 55, 61, 69, 74, 81, 87. Do not automatically tag those as Home unless the user confirms or a normalization/tagging rule should include them.

Latest rows at handoff:

```text
Id 4830  2026-05-10 17.54.29.png  DateTime 2026-05-10 17:54:29  no GPS  no category  no description
Id 4829  2026-05-10 17.35.46.jpg  DateTime 2026-05-10 17:35:46  GPS present  99 Prospect Avenue  Home  no description
Id 4828  2026-05-10 17.33.47.jpg  DateTime 2026-05-10 17:33:47  GPS present  99 Prospect Avenue  Home  no description
Id 4827  2026-05-10 17.33.35.jpg  DateTime 2026-05-10 17:33:35  GPS present  99 Prospect Avenue  Home  no description
Id 4826  2026-05-10 17.33.22.jpg  DateTime 2026-05-10 17:33:22  GPS present  99 Prospect Avenue  Home  no description
Id 4825  2026-05-10 14.38.15.jpg  DateTime 2026-05-10 14:38:14  GPS present  91 East 28th Street  no category  no description
```

## ImageAsset Columns

Current columns:

```text
Id BIGINT AUTO_INCREMENT PRIMARY KEY
Source VARCHAR(32) NOT NULL DEFAULT 'OneDrive'
DriveItemId VARCHAR(128) NOT NULL
FileName VARCHAR(512) NOT NULL
DateTime DATETIME NULL
Date DATE NULL
Time TIME NULL
TimeZone VARCHAR(64) NULL
UtcOffsetMinutes SMALLINT NULL
DateTimeUtc DATETIME NULL
TakenDateTimeUtc DATETIME NULL
Latitude DOUBLE NULL
Longitude DOUBLE NULL
Altitude DOUBLE NULL
Description TEXT NULL
DescriptionModel VARCHAR(128) NULL
DescriptionUpdatedAtUtc DATETIME NULL
LocationDisplayName VARCHAR(512) NULL
StreetAddress VARCHAR(512) NULL
OriginalStreetNumber VARCHAR(32) NULL
Neighborhood VARCHAR(255) NULL
City VARCHAR(255) NULL
County VARCHAR(255) NULL
State VARCHAR(255) NULL
PostalCode VARCHAR(32) NULL
Country VARCHAR(255) NULL
CountryCode VARCHAR(8) NULL
LocationProvider VARCHAR(64) NULL
LocationUpdatedAtUtc DATETIME NULL
Category VARCHAR(255) NULL
CategorySource VARCHAR(64) NULL
IsDeleted TINYINT(1) NOT NULL DEFAULT 0
DeletedAtUtc DATETIME NULL
RawGraphJson JSON NULL
InsertedAtUtc DATETIME NOT NULL
UpdatedAtUtc DATETIME NOT NULL
CreatedAt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
ModifiedAt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
```

Notable column history:

- Original `ShortDescription` was renamed to `Description`.
- Original `ShortDescriptionModel` was renamed to `DescriptionModel`.
- Original `ShortDescriptionUpdatedAtUtc` was renamed to `DescriptionUpdatedAtUtc`.
- `DateTime`, `Date`, and `Time` were added because the user wants local image time analytics.
- `CreatedAt` and `ModifiedAt` are MySQL `TIMESTAMP` row lifecycle fields.
- `OriginalStreetNumber` preserves the raw reverse-geocoded street number before normalization.

Indexes:

```text
PRIMARY (Id)
UNIQUE Ux_ImageAsset_Source_DriveItemId (Source, DriveItemId)
Ix_ImageAsset_TakenDateTimeUtc (TakenDateTimeUtc)
Ix_ImageAsset_LatLon (Latitude, Longitude)
Ix_ImageAsset_Category (Category prefix 191)
Ix_ImageAsset_CategorySource (CategorySource)
Ix_ImageAsset_Category_Date (Category prefix 191, Date)
Ix_ImageAsset_StreetAddress (StreetAddress prefix 191)
Ix_ImageAsset_StreetAddress_Category (StreetAddress prefix 191, Category prefix 191)
Ix_ImageAsset_Date (Date)
Ix_ImageAsset_TimeZone_Date (TimeZone, Date)
Ix_ImageAsset_LatLon_Date (Latitude, Longitude, Date)
```

## Active Importer: ImageTracker.py

Entry point:

```bash
python ImageTracker.py --directory "<photo-dir>" --cutoff-date "YYYY-MM-DD" [--force]
```

Arguments:

```text
--directory: required photo directory to scan recursively
--cutoff-date: required date or ISO datetime; files with modified timestamp >= cutoff are eligible
--force: reprocess files even when already present
```

Supported extensions:

```text
.jpg .jpeg .heic .heif .png .webp .tif .tiff
```

Scanning behavior:

- Uses `os.scandir()` iteratively with a stack instead of `Path.rglob()`, which was a speed improvement for the massive folder.
- Recurses into subdirectories.
- Ignores symlinked dirs/files by using `follow_symlinks=False`.
- Skips unreadable files and continues.
- Filters by extension before calling `stat()`.
- Filters eligible files by file modified timestamp converted to UTC.
- Prints progress every 5 seconds or every 2,000 scanned supported photo files.

Progress output shape:

```text
Local sync progress: started directory=... cutoff_utc=... force=0
Local sync progress: elapsed=...s scanned=... eligible=... skipped=... upserted=... captioned=... geocoded=... timezone_enriched=... current=...
Local sync complete: scanned=..., eligible=..., skipped=..., upserted=..., captioned=..., geocoded=..., timezone_enriched=...
```

Identity/upsert behavior:

- `Source` is always `LocalFile`.
- `DriveItemId` is `sha1(str(path.resolve()).lower())`.
- Unique key is `(Source, DriveItemId)`.
- Existing rows are skipped when `--force` is not supplied.
- `--force` causes upsert/reprocessing, but manual categories are protected from overwrite.
- `RawGraphJson` is a JSON object despite the name, currently containing local file metadata:

```json
{
  "LocalPath": "...",
  "FileModifiedUtc": "...Z",
  "FileSizeBytes": 123
}
```

The name `RawGraphJson` is legacy from the OneDrive implementation and was kept for schema continuity.

## Metadata Extraction

GPS:

- Class: `ExifGpsExtractor`
- Uses Pillow.
- Attempts to register HEIF/HEIC support via `pillow_heif.register_heif_opener()`.
- Reads EXIF `GPSInfo`.
- Handles the case where `GPSInfo` is an offset by calling `exif.get_ifd()`.
- Converts DMS rationals to decimal latitude/longitude.
- Handles west/south negative signs.
- Reads altitude when present, including below-sea-level ref.
- If GPS is unavailable, leaves `Latitude`, `Longitude`, `Altitude` null.

Capture date/time:

- Class: `ExifCaptureDateTimeExtractor`
- Reads EXIF `DateTimeOriginal`, then `DateTimeDigitized`, then `DateTime`.
- Reads EXIF offset tags when available: `OffsetTimeOriginal`, `OffsetTimeDigitized`, `OffsetTime`.
- If EXIF date is present but no offset is present, it preserves the local-looking EXIF `DateTime` and uses existing UTC or file modified UTC as the UTC companion.
- If no EXIF date is available, it derives local DateTime from file modified UTC converted to the machine local timezone.

Temporal semantics:

- `DateTime`: local capture date/time, intended for personal analytics.
- `Date`: derived from `DateTime`.
- `Time`: derived from `DateTime`.
- `TimeZone`: timezone identifier or UTC offset label.
- `UtcOffsetMinutes`: offset minutes at capture time.
- `DateTimeUtc`: UTC companion timestamp.
- `TakenDateTimeUtc`: retained for compatibility with original schema and populated from the same UTC companion.
- `InsertedAtUtc`/`UpdatedAtUtc`: importer-managed UTC datetimes.
- `CreatedAt`/`ModifiedAt`: MySQL row lifecycle timestamps.

## Google Enrichment

Enabled when `GOOGLE_MAPS_API_KEY` is set.

Required Google APIs:

```text
Geocoding API
Time Zone API
```

Reverse geocoding:

- Class: `GoogleMapsLocationResolver`.
- Calls `https://maps.googleapis.com/maps/api/geocode/json`.
- Caches by rounded `latitude,longitude` to 5 decimals.
- Extracts:
  - `LocationDisplayName`
  - `StreetAddress`
  - `OriginalStreetNumber`
  - `Neighborhood`
  - `City`
  - `County`
  - `State`
  - `PostalCode`
  - `Country`
  - `CountryCode`
  - `LocationProvider`
  - `LocationUpdatedAtUtc`

Timezone enrichment:

- Class: `GoogleTimeZoneResolver`.
- Calls `https://maps.googleapis.com/maps/api/timezone/json`.
- Caches by rounded `latitude,longitude` to 4 decimals plus capture hour.
- Fills missing `TimeZone` and `UtcOffsetMinutes` only when GPS and a UTC timestamp exist.
- Does not call Google if capture already has both timezone and offset.

Location normalization:

- Rules are loaded from `location_normalization_rules.json` unless `LOCATION_NORMALIZATION_RULES_PATH` points elsewhere.
- Current default rule is `BayonneProspectCanonical99`.
- It canonicalizes Bayonne Prospect Avenue edge geocoding from `97`, `99`, `101`, and `103` Prospect Avenue to `99 Prospect Avenue`.
- It preserves the raw `OriginalStreetNumber`, because the user said this can reveal where in the house the photo was taken, such as near a window/balcony/edge.

Current normalization file:

```json
{
  "Rules": [
    {
      "Name": "BayonneProspectCanonical99",
      "CityEquals": "Bayonne",
      "StateIn": ["New Jersey", "NJ"],
      "CountryIn": ["United States", "USA"],
      "StreetContainsAny": ["Prospect Avenue", "Prospect Ave"],
      "OriginalStreetNumberIn": ["97", "99", "101", "103"],
      "NormalizedStreetAddress": "99 Prospect Avenue"
    }
  ]
}
```

## Captioning

Enabled only when `OPENAI_API_KEY` is set.

Default model:

```text
gpt-5.2
```

Class:

```text
OpenAIVisionCaptioner
```

API:

```text
POST https://api.openai.com/v1/responses
```

Prompt rules:

- Exactly one sentence.
- Under `PHOTO_CAPTION_MAX_WORDS`, default 18.
- Do not identify people.
- Do not infer addresses.
- Do not infer sensitive attributes.
- If screenshot/document, describe at a high level.

Storage:

```text
Description
DescriptionModel
DescriptionUpdatedAtUtc
```

Important operational history:

- The user previously had OpenAI quota issues.
- Bulk import generated repeated `429 insufficient_quota` errors, and one `520` error.
- To finish that import, the command was rerun with captioning disabled:

```bash
OPENAI_API_KEY='' python ImageTracker.py --directory "/mnt/d/Pictures/Camera Uploads" --cutoff-date "2026-04-08"
```

- That created/imported rows without descriptions.
- At handoff there were 556 rows missing descriptions.
- If asked to backfill descriptions, do a single-image preflight first. Do not spam hundreds of caption requests if quota still returns 429.

## Category Tagging

Two mechanisms exist:

1. Manual tagging via `tag_location.py`.
2. Automatic inference during import from already-tagged rows.

Manual examples:

```bash
python tag_location.py --address 99 Prospect Ave Bayonne NJ 07002 --category "Home"
python tag_location.py --gps 40.6631583333,-74.1143888889 --category "Kids at the Park"
```

Useful options:

```bash
--radius-meters 10
--dry-run
--where-category-is-null
```

`--where-category-is-null` means only rows with `Category IS NULL` or blank are updated. This is useful when bulk tagging a location without overwriting existing manual or inferred categories.

Tagging behavior:

- Manual updates set `CategorySource='Manual'`.
- Address mode tokenizes the address and matches `StreetAddress` by number/street/suffix tokens.
- Address mode also geocodes the address with Google if a key is available, then radius-matches too.
- GPS mode radius-matches directly.
- Radius default is 10 meters.
- `--dry-run` prints a preview and does not update.
- Preview prints up to 10 matching rows.

Automatic inference during import:

- If current row has no category, first search existing non-deleted rows with the same `StreetAddress`.
- Prefer manual categories in address lookup ordering.
- If no address match, use a 10-meter GPS radius fallback.
- Source values:
  - `AddressPropagation`
  - `RadiusPropagation10m`
- Existing manual categories are not overwritten, even when `--force` is used.
- Screenshots/no-GPS/no-address remain unclassified.

Current known manual tag:

```bash
python tag_location.py --address 99 Prospect Ave Bayonne NJ 07002 --category "Home"
```

This was applied earlier and caused the 99 Prospect Avenue cluster to be Home.

## External Windows/Robocopy Scripts

These are outside the repo but relevant operationally:

```text
C:\Development\scripts\move_camera_uploads_pics.cmd
C:\Development\scripts\start_move_camera_uploads_pics.cmd
C:\Development\scripts\send_imagetracker_failure_email.ps1
C:\Development\logs\ImageTracker\
```

Current `move_camera_uploads_pics.cmd` behavior:

- Source: `C:\Users\nektarios\Dropbox\Camera Uploads`
- Destination: `D:\Pictures\Camera Uploads`
- Runs robocopy:

```cmd
robocopy "C:\Users\nektarios\Dropbox\Camera Uploads" "D:\Pictures\Camera Uploads" /E /COPYALL /MOV /R:5 /W:5 /XX
```

- Logs to `C:\Development\logs\ImageTracker\move_camera_uploads_YYYYMMDD.log`.
- Treats robocopy exit codes 0 through 7 as success.
- Creates `LAST_SUCCESS.txt` or `LAST_FAILURE.txt`.
- Creates a desktop alert file on failure:

```text
%USERPROFILE%\Desktop\CameraUploads_Backup_FAILED.txt
```

- Sends failure email through `send_imagetracker_failure_email.ps1` if SMTP env vars are configured.

Current `start_move_camera_uploads_pics.cmd` launches the robocopy script detached/minimized so Windows logon is not blocked:

```cmd
start "" /min cmd.exe /c "\"%SCRIPT%\""
```

Important: ImageTracker import was removed from the robocopy script per user request. The robocopy job only moves Dropbox Camera Uploads into the durable `D:\Pictures\Camera Uploads` archive. The ImageTracker import must be scheduled separately if desired.

At handoff, robocopy logs existed through 2026-08-26, but the database max `DateTime` was still 2026-05-10.

## Original OneDrive Design Status

The first requested design was:

- OneDrive Camera Upload folder.
- Microsoft Graph delta.
- `imagetracker photos:auth`.
- `imagetracker photos:sync`.
- Device-code auth/MSAL token cache in MySQL.
- Delta link persisted for incremental sync.

That design is not the current active implementation. The current root `ImageTracker.py` does not expose `imagetracker photos:auth` or `imagetracker photos:sync`; it exposes only local directory import arguments.

The original OneDrive tables still exist:

```text
OneDriveSyncState
OneDriveTokenCache
```

They are not currently used by the local importer.

If the user asks to revive OneDrive:

- Confirm first because the user explicitly changed the plan away from OneDrive.
- Do not replace the local importer unless requested.
- Preserve PascalCase schema.
- Consider adding OneDrive support as a separate command or script only if the user accepts that extra complexity.

## Operational Runbook

Verify repo state:

```bash
cd /mnt/c/development/projects/imagetracker
git status --short --branch
git branch -vv
```

Run tests:

```bash
pytest -q
```

Verify DB config without exposing secrets:

```bash
python - <<'PY'
from ImageTracker import load_settings, parse_mysql_config
s = load_settings()
c = parse_mysql_config(s)
print(c.host, c.port, c.user, c.database, c.charset)
print("has_password", bool(c.password))
print("has_openai_api_key", bool(s.openai_api_key))
print("openai_vision_model", s.openai_vision_model)
print("has_google_maps_api_key", bool(s.google_maps_api_key))
PY
```

Run an incremental local import with a conservative overlap:

```bash
python ImageTracker.py --directory "/mnt/d/Pictures/Camera Uploads" --cutoff-date "2026-05-01"
```

If OpenAI quota errors appear and the user mainly wants metadata/GPS imported:

```bash
OPENAI_API_KEY='' python ImageTracker.py --directory "/mnt/d/Pictures/Camera Uploads" --cutoff-date "2026-05-01"
```

Force reprocess metadata for a cutoff period:

```bash
python ImageTracker.py --directory "/mnt/d/Pictures/Camera Uploads" --cutoff-date "2026-01-01" --force
```

Use `--force` carefully:

- It rereads file bytes and can redo Google/OpenAI work.
- It still protects manual categories.
- It can update timestamps and metadata.

Manual tag dry run:

```bash
python tag_location.py --address 99 Prospect Ave Bayonne NJ 07002 --category "Home" --dry-run
```

Manual tag only empty categories:

```bash
python tag_location.py --address 99 Prospect Ave Bayonne NJ 07002 --category "Home" --where-category-is-null
```

GPS manual tag:

```bash
python tag_location.py --gps 40.6631583333,-74.1143888889 --category "Kids at the Park" --radius-meters 10 --dry-run
```

## Useful SQL

Current DB snapshot:

```sql
SELECT
    COUNT(*) AS TotalRows,
    SUM(CASE WHEN `Latitude` IS NOT NULL AND `Longitude` IS NOT NULL THEN 1 ELSE 0 END) AS RowsWithGps,
    SUM(CASE WHEN `Latitude` IS NULL OR `Longitude` IS NULL THEN 1 ELSE 0 END) AS RowsMissingGps,
    SUM(CASE WHEN `Description` IS NOT NULL AND TRIM(`Description`) <> '' THEN 1 ELSE 0 END) AS RowsWithDescription,
    SUM(CASE WHEN `Description` IS NULL OR TRIM(`Description`) = '' THEN 1 ELSE 0 END) AS RowsMissingDescription,
    MIN(`DateTime`) AS MinDateTime,
    MAX(`DateTime`) AS MaxDateTime,
    MAX(`ModifiedAt`) AS MaxModifiedAt
FROM `ImageAsset`;
```

Latest imported photos:

```sql
SELECT
    `Id`,
    `FileName`,
    `DateTime`,
    `DateTimeUtc`,
    `Latitude`,
    `Longitude`,
    `StreetAddress`,
    `Category`,
    `Description` IS NOT NULL AS HasDescription
FROM `ImageAsset`
WHERE `IsDeleted` = 0
ORDER BY `DateTime` DESC, `Id` DESC
LIMIT 25;
```

Photos for a local day:

```sql
SELECT
    `Id`,
    `FileName`,
    `DateTime`,
    `Latitude`,
    `Longitude`,
    `StreetAddress`,
    `Category`,
    `Description`
FROM `ImageAsset`
WHERE `IsDeleted` = 0
  AND `Date` = '2026-05-10'
ORDER BY `DateTime`;
```

Rows missing GPS, excluding screenshots by extension:

```sql
SELECT
    `Id`,
    `FileName`,
    `DateTime`
FROM `ImageAsset`
WHERE `IsDeleted` = 0
  AND (`Latitude` IS NULL OR `Longitude` IS NULL)
  AND LOWER(`FileName`) NOT REGEXP '\\.(png|webp)$'
ORDER BY `DateTime` DESC
LIMIT 100;
```

Category summary:

```sql
SELECT
    COALESCE(NULLIF(TRIM(`Category`), ''), '(NULL)') AS Category,
    COUNT(*) AS RowCount
FROM `ImageAsset`
WHERE `IsDeleted` = 0
GROUP BY COALESCE(NULLIF(TRIM(`Category`), ''), '(NULL)')
ORDER BY RowCount DESC, Category;
```

Prospect Avenue audit:

```sql
SELECT
    `StreetAddress`,
    `OriginalStreetNumber`,
    `Category`,
    `CategorySource`,
    COUNT(*) AS RowCount
FROM `ImageAsset`
WHERE `IsDeleted` = 0
  AND `StreetAddress` LIKE '%Prospect%'
GROUP BY `StreetAddress`, `OriginalStreetNumber`, `Category`, `CategorySource`
ORDER BY RowCount DESC, `StreetAddress`, `OriginalStreetNumber`;
```

Backfill candidates missing description:

```sql
SELECT
    `Id`,
    `FileName`,
    `DateTime`,
    JSON_UNQUOTE(JSON_EXTRACT(`RawGraphJson`, '$.LocalPath')) AS LocalPath
FROM `ImageAsset`
WHERE `IsDeleted` = 0
  AND (`Description` IS NULL OR TRIM(`Description`) = '')
ORDER BY `DateTime` DESC
LIMIT 50;
```

## DataGrip Notes

If the user cannot see the database in DataGrip:

- Open the MySQL data source properties.
- Go to the Schemas/Databases tab.
- Refresh the schema list.
- Check/select `ImageTracker`.
- Apply/OK.
- If the data source uses a default schema field, set it to `ImageTracker`.
- Do not point the ImageTracker data source at `TransactionAutoCategorizer`.

## Known Issues And Pitfalls

- `RawGraphJson` is legacy-named. It currently stores local metadata, not Graph JSON.
- OneDrive tables exist but are not active.
- `ImageAsset.Source` default is still `'OneDrive'` from the original migration, but local imports explicitly set `Source='LocalFile'`.
- `DriveItemId` for local files depends on resolved full path lowercased. Moving the long-term archive path could create duplicate rows unless a migration/reconciliation strategy is added.
- Import cutoff is based on file modified time, not EXIF capture time. This was deliberate for speed and incremental behavior over the huge folder, but it means an old photo copied recently can become eligible.
- If a file is skipped because it already exists and `--force` is not used, missing descriptions/geocoding from that row will not be backfilled by the normal importer.
- Google location caching is in-memory per run only. It speeds nearby photos during one run but does not persist cache separately.
- The importer is single-threaded. It was optimized with `os.scandir`, but it does not yet exploit the user's 96-core Threadripper.
- OpenAI quota has been unreliable. Do a one-file caption preflight before any large caption backfill.
- Some images from Viber/other messaging apps may lack GPS even if they are real photos. The user identified this as normal.
- Screenshots often lack GPS and should not trigger false alarms.
- Google reverse geocoding may return adjacent street numbers for photos taken near edges of the house. The current normalization rule handles 97/99/101/103 Prospect Avenue in Bayonne.
- Existing manual categories should be treated as authoritative.

## Potential Next Improvements

These are not requested as active work, but they are likely future directions:

- Add a dedicated caption backfill command with rate limiting, resume support, and preflight quota check.
- Add a dedicated location/timezone backfill command for existing skipped rows so `--force` is not needed for everything.
- Persist a scan manifest or file mtime index to avoid scanning the entire folder every time.
- Add multiprocessing or threaded file metadata extraction for large cutoff imports. Be careful with DB writes and API rate limits.
- Add a table for location/category rules instead of JSON-only normalization.
- Add S3 ingestion if the user later wants serverless/startup-style deployment. The user liked S3 as a future option but explicitly said to hold off.
- Add DB-side duplicate detection if files are moved or paths change.
- Add an operational scheduler for the ImageTracker import separate from robocopy.

## Minimal Mental Model For Next Agent

The current source of truth is local files in `D:\Pictures\Camera Uploads`, viewed from WSL as `/mnt/d/Pictures/Camera Uploads`. `ImageTracker.py` scans that tree, skips already-seen paths unless forced, extracts EXIF GPS and capture date, enriches GPS rows with Google address/timezone, optionally captions with OpenAI `gpt-5.2`, and upserts into MySQL `ImageTracker.ImageAsset` using PascalCase columns. Manual category tagging is handled by `tag_location.py`, and `Home` has already been applied to the normalized `99 Prospect Avenue` cluster. Work on `main`; do not create a branch.
