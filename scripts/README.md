# ImageTracker shell toolkit

These Bash scripts are intended for WSL Ubuntu and resolve the repository root
without depending on the caller's working directory.

Start with:

```bash
cd /mnt/c/Development/Projects/ImageTracker
./scripts/play.sh help
./scripts/play.sh check
```

Individual commands:

```bash
./scripts/setup.sh
./scripts/cli.sh
./scripts/cli.sh doctor --json
./scripts/test.sh
./scripts/api-smoke.sh
./scripts/aws-smoke.sh
./scripts/db-smoke.sh
./scripts/package-infra.sh
./scripts/store-openai-key.sh
./scripts/migrate-db.sh
./scripts/migrate-db.sh --apply
./scripts/mysql-one-file-import.sh "My Photos"
./scripts/mysql-one-file-import.sh "My Photos" --apply \
  --admin-env-file /path/to/ignored/.env.prod
```

Safety boundaries:

- `aws-smoke.sh` performs read-only CloudFormation queries and expects the
  unauthenticated API health request to return HTTP 401.
- `db-smoke.sh` retrieves the dedicated application credential from SSM in
  memory and opens an explicitly read-only transaction. It prints counts only.
- `package-infra.sh` builds ignored `.build` output and never deploys it.
- `store-openai-key.sh` copies a non-empty `OPENAI_API_KEY` from the current
  WSL process into the ignored repository `.env` without displaying it. It does
  not create or update an AWS SSM parameter.
- `migrate-db.sh` is the narrowly scoped schema write command. Without
  `--apply` it reports the 012/013 plan only; with `--apply` it requires an idle
  `ImageTracker` processing database, applies one atomic ALTER at a time, and
  reconciles a missing migration ledger row after an interrupted DDL.
- `mysql-one-file-import.sh` builds one complete CSV for a Local source.
  Without `--apply` it only reports the ignored local file. With `--apply` it
  requires an empty manifest outbox, loads that single file into a temporary
  MySQL table with `LOAD DATA LOCAL INFILE`, inserts only missing pending-hash
  occurrences and their change rows set-wise, and commits once. Use
  `--admin-env-file` when the normal app credential deliberately lacks
  temporary-table privileges; values are loaded in memory and never printed.
- No script invokes `ImageTracker.py`, `tag_location.py`, or `serverless deploy`.

Override defaults only when intentionally checking another environment:

```bash
IMAGETRACKER_AWS_REGION=us-east-2 \
IMAGETRACKER_STACK_NAME=image-tracker-prod \
IMAGETRACKER_DB_SECRET_PARAMETER=/imagetracker/prod/mysql \
./scripts/play.sh check
```
