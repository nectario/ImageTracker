# ImageTracker

ImageTracker is becoming a consumer media app for indexing, finding, and
reliving photos and videos. It supports two storage modes per source:

- **Local:** originals remain on the device or computer; searchable metadata is
  stored in the `ImageTracker` MySQL database.
- **Remote:** the exact original is stored once per user in private Amazon S3
  and is available across the user's devices.

The current repository contains the Phase 0 product foundation plus the legacy
local photo importer. The approved architecture and delivery sequence are in
[the UX-first implementation plan](docs/ImageTracker%20App%20UX-First%20Implementation%20Plan.md).

## Phase 0 foundation

- `contracts/v1/openapi.json`: versioned REST/JSON contract for native clients
  and the CLI.
- `services/api`: FastAPI health surface and AWS Lambda adapter.
- `services/common`: shared configuration and public state values.
- `cli/imagetracker_cli`: Typer/Rich CLI foundation.
- `infra`: isolated low-cost AWS Serverless foundation.
- `migrations/007_CreateMediaAppTables.sql`: additive PascalCase media schema;
  the legacy `ImageAsset` table is not altered.
- `apps/ios`, `apps/android`, and `apps/windows`: explicit native-client
  boundaries for later delivery phases.

The Windows client will be a packaged native WinUI 3 app whose visual language
is derived from Nektron Write and Nektron Mail. Generic template aesthetics are
not considered finished product UI.

## Development setup

Use `python`, not `python3`:

```bash
python -m venv .venv
.venv/bin/python -m pip install -e ".[dev]"
```

On Windows PowerShell, activate the environment using the normal Windows venv
activation script and run the same `python -m pip` command.

Run verification:

```bash
python -m pytest -q
python contracts/validate_openapi.py
imagetracker doctor --json
```

The WSL shell toolkit wraps these commands and the safe deployed-resource
checks:

```bash
./scripts/play.sh help
./scripts/play.sh check
```

See [`scripts/README.md`](scripts/README.md) for individual commands and safety
boundaries. The toolkit intentionally contains no deployment or importer
command.

Infrastructure remains independently packageable from `infra/`; see
`infra/README.md`. Always validate the package before deployment, and configure
each required SSM SecureString before enabling the feature that consumes it.
Secret values must never be committed or included in deployment archives.

## CLI foundation

Phase 0 provides non-networking commands:

```bash
imagetracker version
imagetracker doctor
imagetracker doctor --json
```

Upload, sync, media, job, and migration commands arrive in Phase 1 against the
versioned API. User-facing CLI commands will not connect directly to MySQL.

## Legacy local importer

The root `ImageTracker.py` remains operational during the transition and keeps
its existing behavior. It recursively scans a local folder, preserves exact
filenames, extracts EXIF metadata, optionally enriches locations/captions, and
upserts `ImageAsset`.

```bash
python ImageTracker.py \
  --directory "/mnt/d/Pictures/Camera Uploads" \
  --cutoff-date "2026-01-01"
```

Force reprocessing remains available but should be used carefully:

```bash
python ImageTracker.py \
  --directory "/mnt/d/Pictures/Camera Uploads" \
  --cutoff-date "2026-01-01" \
  --force
```

Manual location tagging remains in the separate root utility:

```bash
python tag_location.py --gps 40.0,-74.0 --category "Example" --dry-run
```

Useful tagging options are `--radius-meters`, `--dry-run`, and
`--where-category-is-null`. Manual categories remain authoritative.

## Configuration

Copy `.env.example` only for legacy/local development and fill in values
without committing `.env`. Prefer project-scoped MySQL names:

- `IMAGETRACKER_MYSQL_HOST`
- `IMAGETRACKER_MYSQL_PORT`
- `IMAGETRACKER_MYSQL_USER`
- `IMAGETRACKER_MYSQL_PASSWORD`
- `IMAGETRACKER_MYSQL_DATABASE=ImageTracker`

The service foundation accepts only the `ImageTracker` database scope. Cloud
credentials are represented by SSM parameter names rather than plaintext
provider keys in repository configuration.

Optional legacy enrichments still use:

- `OPENAI_API_KEY` and `OPENAI_VISION_MODEL`
- `GOOGLE_MAPS_API_KEY`
- `LOCATION_NORMALIZATION_RULES_PATH`

## Compatibility invariants

- Work directly on `main`; do not create implementation branches.
- Preserve original filenames verbatim.
- Preserve PascalCase MySQL table and column names.
- Keep `ImageAsset` readable and unchanged while the additive media model is
  introduced.
- Use exact per-user SHA-256 deduplication in the new model; do not use path
  hashes as durable content identity.
- Do not upload Local-mode originals or previews to permanent S3 storage.
