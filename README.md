# ImageTracker

ImageTracker is a consumer media app for indexing, finding, and reliving
photos and videos. It is also the media source for the future NektronAI
Intelligence Layer.

Phase 1 currently implements the Local-mode data path: the CLI discovers a
folder, extracts available metadata, computes an exact SHA-256 content hash,
and sends metadata manifests to the authenticated API. MySQL stores one
`MediaAsset` per user and exact hash while retaining a separate
`MediaOccurrence` for every source path. Original Local-mode files remain on
the source computer and are not uploaded to permanent S3 storage.

The approved architecture and delivery sequence are in
[the UX-first implementation plan](docs/ImageTracker%20App%20UX-First%20Implementation%20Plan.md).
The current implementation and verification ledger is in
[the Phase 1 status](docs/PHASE1_STATUS.md).

## What works now

- Cognito email/password sign-up, one-time email confirmation, login, session
  status, token refresh, and logout. Verification messages are sent as
  `ImageTracker <info@nektron.ai>`.
- Device registration and Local folder source creation, listing, update, and
  removal.
- Recursive photo and video discovery with exact filenames and local locators.
- Streaming SHA-256 hashing, a local hash/metadata cache, and per-user exact
  deduplication in MySQL.
- EXIF photo metadata and GPS extraction; `ffprobe` adds available video
  dimensions, duration, capture time, and embedded coordinates when installed.
- Batched manifest upserts and explicit deletion events with durable local
  outbox state, stable idempotency keys, and safe interruption recovery.
- Authenticated change-feed, media timeline/search/detail, and processing-job
  API surfaces.
- Read-only legacy `ImageAsset` audit and paged migration preview.

Remote-mode upload and retrieval are not implemented yet. The CLI rejects
`Remote` before changing a source or uploading anything. Reverse geocoding,
AI captions, video-audio transcription, face recognition, and all other media
enrichment are also deferred. The legacy migration command is preview-only;
it never writes mappings or new media rows.

## Development setup

Use `python`, not `python3`:

```bash
python -m venv .venv
.venv/bin/python -m pip install -e ".[dev]"
```

On Windows PowerShell:

```powershell
python -m venv .venv
.venv\Scripts\python.exe -m pip install -e ".[dev]"
```

Run the complete repository verification:

```bash
./scripts/play.sh check
```

Or run the components directly:

```bash
python -m pytest -q
python contracts/validate_openapi.py
imagetracker doctor --json
```

See [`scripts/README.md`](scripts/README.md) for the individual WSL shell
wrappers and their safety boundaries. The toolkit contains no deployment,
database migration, or legacy-importer command.

## Configure the CLI

The easiest setup uses AWS credentials already available to WSL and discovers
the API and Cognito identifiers from the deployed CloudFormation stack:

```bash
imagetracker configure \
  --stack image-tracker-prod \
  --region us-east-2

imagetracker doctor
```

Add `--profile PROFILE` when the credentials are under a named AWS profile.
For isolated development or automation, configuration may instead be supplied
with `IMAGETRACKER_API_URL`, `IMAGETRACKER_AWS_REGION`,
`IMAGETRACKER_COGNITO_USER_POOL_ID`, and
`IMAGETRACKER_COGNITO_CLIENT_ID`. Set `IMAGETRACKER_CONFIG_DIR` to relocate
the CLI configuration and SQLite state directory.

## Create an account and sign in

Omit `--password` to use the private interactive prompt:

```bash
imagetracker auth signup you@example.com
imagetracker auth confirm you@example.com ONE_TIME_CODE
imagetracker auth login you@example.com
imagetracker auth status
```

The session uses the operating-system credential vault when one is available,
with a permission-restricted local credential file as the WSL fallback. To end
the session:

```bash
imagetracker auth logout
```

## Add and synchronize a Local source

Register a folder once, then synchronize it by name, source ID, or local path:

```bash
imagetracker source add "/mnt/d/Pictures/Camera Uploads" \
  --name "Camera Uploads"

imagetracker source list
imagetracker sync "Camera Uploads" --dry-run
imagetracker sync "Camera Uploads"
imagetracker status
```

`--dry-run` scans and hashes without sending a manifest. A normal sync saves
manifest batches locally before sending them, so rerunning the command resumes
unacknowledged work after a network failure or interruption. Unchanged files
reuse the local hash/metadata cache. Deletions are emitted only after the
scanner completes a reliable read of the whole source.

Useful operating commands:

```bash
imagetracker sync "Camera Uploads" --watch
imagetracker sync "Camera Uploads" --force-rehash
imagetracker status --follow
imagetracker source set-mode "Camera Uploads" Local
imagetracker source remove "Camera Uploads"
```

Local cache, source bindings, and manifest outbox state are isolated by the
active Cognito account. If a manifest entry needs attention, the CLI
quarantines that entry without replaying the same broken batch forever:

```bash
imagetracker outbox list
imagetracker outbox discard BATCH_ID
```

Discarding a failed batch releases its rejected revision so a later sync can
submit it with a fresh idempotency key. A partial sync exits with code `5`.
Use `--force-rehash` when file bytes may have changed while size and modified
time were deliberately preserved.

Browse the Local metadata visible on this device and inspect processing work:

```bash
imagetracker media list
imagetracker media search "birthday"
imagetracker media show MEDIA_ASSET_ID
imagetracker jobs list
imagetracker jobs retry JOB_ID
```

Use `--json` on configure, doctor, auth status, source add/list, sync, status,
and legacy commands when scripting. `source remove` unregisters the source and
does not delete files from disk.

## Inspect legacy data safely

The Phase 1 legacy commands connect only to the `ImageTracker` database and use
read-only transactions. The recommended deployed configuration resolves the
database credential from SSM without printing it:

```bash
export IMAGETRACKER_DB_SECRET_PARAMETER=/imagetracker/prod/mysql

imagetracker legacy audit
imagetracker legacy migrate --dry-run --limit 500
imagetracker legacy migrate --dry-run --limit 500 --save-checkpoint
imagetracker legacy migrate --dry-run --after-id 500 --limit 500
```

The preview reports the next legacy ID for the following batch. The optional
checkpoint is local and preview-only; it never represents a MySQL migration
write. Actual legacy migration writes remain disabled in this slice.
`IMAGETRACKER_ADMIN_MYSQL_DSN`, `MYSQL_DSN`, or scoped `MYSQL_*`
variables are supported for local administration, but the database name must
resolve exactly to `ImageTracker`.

The root `ImageTracker.py` importer remains available for its existing legacy
workflow, but it is not called by the new CLI or shell playground. Its writes
are separate from the Phase 1 legacy migration preview.

## Architecture boundaries

- `contracts/v1/openapi.json`: versioned REST/JSON contract for native clients
  and the CLI.
- `services/api`: FastAPI routes, Cognito claim boundary, problem responses,
  and AWS Lambda adapter.
- `services/domain` and `services/data`: transaction-scoped domain logic,
  repositories, SQLAlchemy mappings, durable idempotency, and MySQL access.
- `cli/imagetracker_cli`: configuration, authentication, Local scanner,
  SQLite state/outbox, sync, status, and legacy inspection.
- `infra`: isolated, low-cost AWS Serverless foundation.
- `migrations/007_CreateMediaAppTables.sql` through the additive hardening
  migrations: PascalCase media schema beside the unchanged legacy table.
- `apps/ios`, `apps/android`, and `apps/windows`: native-client boundaries for
  later delivery phases.

The Windows client will be a packaged native WinUI 3 app whose visual identity
is derived from Nektron Write and Nektron Mail. Product aesthetics and UX are
release criteria, not a later polish pass.

## Compatibility invariants

- Work directly on `main`; do not create implementation branches.
- Preserve original filenames verbatim.
- Preserve PascalCase MySQL table and column names.
- Keep `ImageAsset` readable and unchanged while the additive media model is
  introduced.
- Use exact per-user SHA-256 deduplication; never use a path hash as durable
  content identity.
- Keep one occurrence per source path even when multiple paths contain the
  same exact bytes.
- Never upload Local-mode originals or previews to permanent S3 storage.
