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
```

Safety boundaries:

- `aws-smoke.sh` performs read-only CloudFormation queries and expects the
  unauthenticated API health request to return HTTP 401.
- `db-smoke.sh` retrieves the dedicated application credential from SSM in
  memory and opens an explicitly read-only transaction. It prints counts only.
- `package-infra.sh` builds ignored `.build` output and never deploys it.
- No script invokes `ImageTracker.py`, `tag_location.py`, `serverless deploy`,
  or database migrations.

Override defaults only when intentionally checking another environment:

```bash
IMAGETRACKER_AWS_REGION=us-east-2 \
IMAGETRACKER_STACK_NAME=image-tracker-prod \
IMAGETRACKER_DB_SECRET_PARAMETER=/imagetracker/prod/mysql \
./scripts/play.sh check
```
