# ImageTracker

ImageTracker is a consumer media app for indexing, finding, and reliving
photos and videos. It is also the media source for the future NektronAI
Intelligence Layer.

Phase 1 implements the Local-mode data path: the CLI discovers a folder,
can register its directory metadata immediately, then extracts metadata and
computes exact SHA-256 hashes in parallel. MySQL stores one `MediaAsset` per
user and exact hash while retaining a separate `MediaOccurrence` for every
source path. Original Local-mode files remain on the source computer and are
not uploaded to permanent S3 storage.

The current repository also contains the first bounded enrichment pipeline:
GPS coordinates can be resolved to a useful nearby address, and photos can
receive concise, searchable scene descriptions. These capabilities are now
deployed in the production app stack. Persistent address resolution uses
Amazon Location Service Places V2 so the request can explicitly declare stored
use.

The approved architecture and delivery sequence are in
[the UX-first implementation plan](docs/ImageTracker%20App%20UX-First%20Implementation%20Plan.md).
The current implementation and verification ledger is in
[the Phase 1 status](docs/PHASE1_STATUS.md).

## What works now

The deployed Local core provides:

- Cognito email/password sign-up, one-time email confirmation, login, session
  status, token refresh, and logout. Verification messages are sent as
  `ImageTracker <info@nektron.ai>`.
- Device registration and Local folder source creation, listing, update, and
  removal.
- Parallel photo and video discovery with exact filenames and local locators.
- Progressive fast-add, parallel SHA-256/metadata extraction, bulk local cache
  writes, and per-user exact
  deduplication in MySQL.
- EXIF photo metadata and GPS extraction; `ffprobe` adds available video
  dimensions, duration, capture time, and embedded coordinates when installed.
- Timeout-safe 100-row manifest upserts and explicit deletion events with
  durable local outbox state, stable idempotency keys, automatic splitting of
  older oversized requests, and safe interruption recovery.
- Authenticated change-feed, media timeline/search/detail, and processing-job
  API surfaces.
- Read-only legacy `ImageAsset` audit and paged migration preview.

The deployed enrichment build adds:

- Asynchronous Amazon Location Service Places V2 reverse geocoding with full
  address and provider provenance (`AmazonLocationPlacesV2`). A resolved
  address may be reused only for the same user and only when another coordinate
  is within 5 metres.
- Automatic photo scene descriptions using `gpt-5.6-terra`, high image detail,
  Flex processing, reasoning effort `none`, and a versioned search prompt
  capped at 24 words. Each provider attempt reserves both one request and
  USD 0.010 before staging; the worker reconciles that reservation to sanitized
  token usage at USD 2.00/M input, USD 0.20/M cached input, and USD 12.00/M
  output. The per-user monthly USD ceiling is USD 230.
- A durable CLI scene-preview outbox, signed temporary uploads, bounded SQS
  processing, quota deferral, visible retry states, and safe staging cleanup.

Remote-mode upload and retrieval are not implemented yet. The CLI rejects
`Remote` before changing a source or uploading anything. Video-audio
transcription, face recognition, and other media enrichment remain deferred.
The legacy migration command is preview-only; it never writes mappings or new
media rows.

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
wrappers and their safety boundaries. Deployment remains outside the shell
toolkit. Database-writing wrappers are explicit and dry-run by default: one is
limited to enrichment schema migration, and one performs the trusted WSL
one-file manifest import.

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
imagetracker sync "Camera Uploads" --fast-add
imagetracker sync "Camera Uploads" --scan-workers 64
imagetracker enrich "Camera Uploads" --limit 100
imagetracker status
```

`--fast-add` inventories new files from directory metadata without reading
their contents, so a very large library appears quickly. A later normal sync
performs exact SHA-256 deduplication, EXIF extraction, and video probing in the
background-friendly deep-index phase. Worker selection is auto-tuned up to 64;
use `--scan-workers`/`-j` to benchmark another bounded value.

Metadata delivery defaults to `--transport auto`. Small or mixed changes keep
the proven 100-row request path. At 10 saved batches or 1,000 hash-enriched
Local upserts, the CLI creates one deterministic, compressed manifest, uploads
it directly to private S3, follows a dedicated asynchronous MySQL import, and
applies the checksum-bound result to local state in resumable pages. Deletions,
pending hashes, rejected bulk rows, and unsupported relinks automatically keep
or return to the batch path without discarding saved work:

```bash
imagetracker sync "Camera Uploads" --transport auto
imagetracker sync "Camera Uploads" --transport bulk
imagetracker sync "Camera Uploads" --transport bulk --bulk-max-rows 1000
imagetracker sync "Camera Uploads" --transport batch
```

`Ctrl+C` stops watching, not the durable server import. The next sync refreshes
the authoritative server phase before deciding whether to upload, continue,
apply the result, or fall back. `imagetracker status --follow` shows the bulk
phase, processed rows, percentage, and whether a value is freshly fetched or
cached.
`--bulk-max-rows` processes one bounded prefix and stops before rescanning, so
production rollout can advance deliberately through 1,000-row and 10,000-row
canaries before removing the limit.

For trusted WSL administration on the shared DeepTrading infrastructure, the
one-file path bypasses API transport batching while preserving account/source
scope and local originals:

```bash
./scripts/mysql-one-file-import.sh "Camera Uploads" --apply \
  --admin-env-file /path/to/ignored/.env.prod
```

It creates one ignored CSV, executes one MySQL `LOAD DATA LOCAL INFILE`, merges
only missing pending-hash occurrences and their change rows set-wise, and
commits once. It refuses to race a live manifest sender unless the stopped
outbox is explicitly replaced with `--replace-pending-outbox`.

`sync` is metadata-only by default: it scans, hashes, extracts metadata, and
sends resumable manifests without staging paid scene-description work. Use the
separate bounded command when you are ready to process due previews, or add
`--with-enrichment` to stage up to 100 after a metadata sync:

```bash
imagetracker enrich "Camera Uploads" --limit 100
imagetracker sync "Camera Uploads" --with-enrichment
```

`--dry-run` scans and hashes without sending a manifest. A normal sync saves
manifest batches locally before sending them, so rerunning the command resumes
unacknowledged work after a network failure or interruption. Unchanged files
reuse the local hash/metadata cache. Deletions are emitted only after the
scanner completes a reliable read of the whole source.

On the service side, each manifest prefetches existing assets, descriptions,
and jobs once and batches new asset, occurrence, job, and change-feed writes at
transaction boundaries. This keeps database round trips tied to the batch
rather than multiplying them for every photo while preserving exact-hash
deduplication and idempotent resume behavior.

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

GPS in a manifest queues reverse geocoding. Each eligible photo also receives a
server-owned description job, but metadata sync leaves its local preview safely
queued. `enrich` creates deterministic JPEG previews and stages only the number
requested with `--limit`, without rescanning files or draining manifests. Useful
WSL commands for watching or repairing that work are:

```bash
./scripts/cli.sh status --follow
./scripts/cli.sh outbox descriptions --state All
./scripts/cli.sh jobs list
./scripts/cli.sh media list
./scripts/cli.sh media show MEDIA_ASSET_ID --json
./scripts/cli.sh media search "birthday cake"

# Retry client-side preview preparation/staging after fixing its issue.
./scripts/cli.sh outbox retry-description JOB_ID

# Retry a server job that is Failed or waiting on quota.
./scripts/cli.sh jobs retry JOB_ID
./scripts/cli.sh enrich "Camera Uploads" --limit 100
```

`DeferredQuota` is an intentional waiting state, not a failed upload. Once one
scene-description request reaches the monthly ceiling, the CLI defers the
remaining due previews together instead of asking for thousands of upload
plans. `media show --json` exposes the durable address, description, and their
provider/model provenance after processing succeeds.

Use `--json` on configure, doctor, auth status, source add/list, sync, status,
outbox inspection, media list/show/search, jobs list/retry, and legacy commands
when scripting. `source remove` unregisters the source and does not delete files
from disk.

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

## Enrichment privacy and cost boundaries

- Reverse geocoding sends latitude and longitude to Amazon Location Service
  Places V2. It never sends the photo, filename, local path, user identity, or
  scene description. `ReverseGeocode` is called with `IntendedUse=Storage`,
  `MaxResults=1`, and address-oriented place types. The returned result is
  normalized before the address components and `AmazonLocationPlacesV2`
  provenance are stored in MySQL.
- Address reuse is account-scoped: a fully resolved result may satisfy another
  coordinate for the same user within 5 metres, but it is never shared between
  users. The default hard ceiling is 1,000 provider calls per user per calendar
  month. At the 2026-08-28 `us-east-2` list price of USD 4 per 1,000 stored
  reverse-geocode requests, that ceiling bounds this component to about USD 4
  per user per month; verify current
  [Amazon Location pricing](https://aws.amazon.com/location/pricing/) before
  changing the limit.
- Scene analysis never uploads the Local original. The CLI applies EXIF
  orientation, never enlarges the image, reduces its longest edge to at most
  1,024 pixels, renders a deterministic metadata-free JPEG, and uploads only
  that preview to a private `staging/` key.
- OpenAI receives the preview through a short-lived signed HTTPS URL. The
  Responses API request uses `store: false`; the worker deletes the S3 preview
  after a safe terminal outcome, and the bucket's one-day lifecycle rule is the
  cleanup backstop.
- Scene descriptions have a 100,000-call secondary ceiling and a harder USD
  230 per-user monthly ceiling. Each attempt reserves USD 0.010 before a
  preview upload URL is issued, then settles against bounded provider usage;
  Flex processing lowers unit cost and single-worker concurrency prevents
  request bursts.
- A Local asset remains `LocalOnly`: its path remains the original reference,
  while its durable S3 bucket, original-object key, and preview-object key stay
  null. Temporary staging does not silently convert it to Remote mode.

## Enrichment configuration and release gate

Amazon Location Service uses the active AWS identity locally and the scoped
Lambda IAM role after deployment; it has no API key or SSM secret. OpenAI key
resolution checks the process environment, then the ignored repository `.env`,
then the SSM SecureString named at runtime:

```text
/imagetracker/prod/openai
```

Never place provider values in tracked files or deployment arguments. The
production OpenAI parameter has been provisioned. Reverse geocoding has no
additional credential or SSM prerequisite.

Use WSL Ubuntu with the existing AWS credentials to run the complete tests,
validate/package the stack, review the IAM and artifacts, and only then deploy.
The five-minute `RetryDueJobs` recovery schedule is enabled so a committed job
cannot be lost with an early or failed SQS delivery. Reconciliation, quota-reset,
and trash-purge schedules remain disabled. See
[the infrastructure guide](infra/README.md) for the exact validation and
deployment commands.

The production geocode controls are deliberately small and explicit:

```text
IMAGETRACKER_GEOCODE_REUSE_RADIUS_METERS=5
IMAGETRACKER_GEOCODE_MONTHLY_CALL_LIMIT=1000
IMAGETRACKER_SCENE_DESCRIPTION_MONTHLY_CALL_LIMIT=100000
IMAGETRACKER_SCENE_DESCRIPTION_MONTHLY_USD_LIMIT=230.000000
IMAGETRACKER_SCENE_DESCRIPTION_RESERVED_USD_PER_REQUEST=0.010000
```

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
- Permit only short-lived `TemporaryProcessing` preview staging for an
  authorized Local photo-description job; it must never populate durable S3
  asset locators or change Local storage mode.
