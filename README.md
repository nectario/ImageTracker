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

## Local Sync Behavior

1. Scans supported image files recursively in `--directory`.
2. Processes files with modified timestamp >= `--cutoff-date`.
3. Skips already processed files (`Source='LocalFile'`) unless `--force` is supplied.
4. Stores file names exactly as-is (for example: `IMG_8677.JPG`).
5. Extracts GPS primarily from EXIF metadata in the local file bytes.
6. Optionally generates `Description` when `OPENAI_API_KEY` is set.

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
