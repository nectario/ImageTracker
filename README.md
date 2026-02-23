# ImageTracker

ImageTracker syncs iPhone photos from OneDrive Camera Upload into MySQL using Microsoft Graph delta queries, then optionally generates short AI descriptions from thumbnails.

## Features

- Incremental sync with Microsoft Graph `delta` (no repeated full scans after first run)
- Stores photo metadata in MySQL PascalCase tables/columns:
  - `ImageAsset.FileName`
  - `ImageAsset.Description`
  - `ImageAsset.Latitude` / `ImageAsset.Longitude`
- Device-code auth command for OneDrive token bootstrap
- Optional OpenAI vision captioning
- First-run ingestion cutoff (`PHOTO_SYNC_INITIAL_CUTOFF_DAYS`) to reduce initial cost while still walking all delta pages to store `DeltaLink`

## Commands

- `imagetracker photos:auth`
- `imagetracker photos:sync`

## Environment

Copy `.env.example` to `.env` and set values.

Required:

- `ONEDRIVE_CLIENT_ID`
- `MYSQL_DSN` or project-scoped MySQL vars (`IMAGETRACKER_MYSQL_*`)

MySQL env resolution order when `MYSQL_DSN` is not set:

- Host: `IMAGETRACKER_MYSQL_HOST` -> `MYSQL_HOST`
- Port: `IMAGETRACKER_MYSQL_PORT` -> `MYSQL_PORT`
- User: `IMAGETRACKER_MYSQL_USER` -> `MYSQL_USERID` -> `MYSQL_USER`
- Password: `IMAGETRACKER_MYSQL_PASSWORD` -> `MYSQL_PASSWORD`
- Database: `IMAGETRACKER_MYSQL_DATABASE` -> `MYSQL_DATABASE_IMAGETRACKER` -> `MYSQL_DATABASE`

Defaults included:

- `ONEDRIVE_TENANT=consumers`
- `ONEDRIVE_SCOPES="User.Read Files.Read.All offline_access"`
- `ONEDRIVE_CAMERA_UPLOAD_PATH="/Pictures/Camera Roll"`
- `ONEDRIVE_CAMERA_UPLOAD_FALLBACK_PATHS="/Pictures/CameraRoll,/Pictures/Camera Uploads,/Pictures/OneDrive Camera Roll"`
- `PHOTO_SYNC_INITIAL_CUTOFF_DAYS=14`
- `PHOTO_CAPTION_MAX_WORDS=18`

Optional captioning:

- `OPENAI_API_KEY`
- `OPENAI_VISION_MODEL` (default: `gpt-5.2`)

## Microsoft App Registration

1. Go to Azure Portal -> App registrations -> New registration.
2. Use personal Microsoft account support (for `consumers` tenant use-case).
3. Add delegated permissions:
   - `User.Read`
   - `Files.Read.All`
   - `offline_access`
4. Copy `Application (client) ID` to `ONEDRIVE_CLIENT_ID`.

## Install and Run

```bash
pip install -e .
```

Authenticate once (or whenever token cache needs refresh):

```bash
imagetracker photos:auth
```

Run incremental sync:

```bash
imagetracker photos:sync
```

## Sync Behavior

1. Loads cached MSAL token from `OneDriveTokenCache`.
2. Resolves camera upload folder using `ONEDRIVE_CAMERA_UPLOAD_PATH`, then fallback paths.
3. Uses stored `OneDriveSyncState.DeltaLink` if present, otherwise starts new folder delta.
4. Pages through `@odata.nextLink` until `@odata.deltaLink` is reached.
5. Upserts/marks rows in `ImageAsset`.
6. Writes `OneDriveSyncState.DeltaLink` only after a successful full page traversal.

## Schema Notes

Migrations are auto-applied by both commands. The migration creates:

- `ImageAsset`
- `OneDriveSyncState`
- `OneDriveTokenCache`

All table and column names use PascalCase.

## Example SQL: Today's Photos (America/New_York)

```sql
SET @LocalTz = 'America/New_York';
SET @StartUtc = CONVERT_TZ(
  DATE_FORMAT(CONVERT_TZ(UTC_TIMESTAMP(), 'UTC', @LocalTz), '%Y-%m-%d 00:00:00'),
  @LocalTz,
  'UTC'
);
SET @EndUtc = DATE_ADD(@StartUtc, INTERVAL 1 DAY);

SELECT
  `Id`,
  `FileName`,
  `TakenDateTimeUtc`,
  `Latitude`,
  `Longitude`,
  `Description`
FROM `ImageAsset`
WHERE `IsDeleted` = 0
  AND `TakenDateTimeUtc` >= @StartUtc
  AND `TakenDateTimeUtc` < @EndUtc
ORDER BY `TakenDateTimeUtc` DESC;
```

## Troubleshooting

- `Run imagetracker photos:auth` error:
  - Token cache is missing/expired or silent token refresh failed. Run `imagetracker photos:auth` again.
- Missing GPS data:
  - Graph item does not include `location.geoCoordinates`; many photos do not carry location metadata.
- Caption missing:
  - `OPENAI_API_KEY` is not set, thumbnail is unavailable, or caption generation failed.
- No scheduler wiring:
  - This repository currently has no existing scheduler framework, so sync runs via CLI invocation.
