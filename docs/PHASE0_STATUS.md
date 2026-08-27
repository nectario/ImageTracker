# Phase 0 Foundation Status

Verified on 2026-08-27.

## Repository foundation

- Versioned OpenAPI 3.0.3 contract: 27 operations, 87 schemas, 23 paths.
- FastAPI/Mangum health service and shared configuration/state primitives.
- Typer/Rich CLI foundation with non-secret `version` and `doctor` commands.
- Native iOS, Android, and Windows client boundaries reserved in the monorepo.
- Packaged WinUI 3 is the Windows target; its finished visual identity must be
  derived from Nektron Write and Nektron Mail.

## Database

- A compressed logical backup of `ImageAsset` and `SchemaMigration` was created
  locally under the ignored `.backups/` directory before applying DDL.
- Migrations `007` through `011` are applied in the `ImageTracker` database.
  They create the app model and harden upload checksums, hash provenance,
  change-feed tombstones, and legacy-map ownership.
- Fifteen additive application tables exist beside the unchanged legacy table.
- `ImageAsset` remains at 2,099 rows after migration.
- A dedicated application user is restricted to the `ImageTracker` database;
  its generated credential exists only in the prod SSM SecureString parameter.

The shared RDS instance had automated backup retention disabled at this check.
The local logical backup is therefore important until the broader DeepTrading
backup policy is addressed separately.

## AWS prod foundation

- CloudFormation stack state: `UPDATE_COMPLETE` in `us-east-2`.
- API Gateway rejects unauthenticated `/v1/health` requests with HTTP 401.
- Direct Lambda smoke invocation returns HTTP 200 with `status=Ok` and the
  versioned contract body.
- Cognito email/password user pool and native app client are present; email
  usernames are case-insensitive and the pool contains no users yet.
- Media bucket has Block Public Access, SSE-S3, and four lifecycle rules.
- Processing SQS queue and dead-letter queue are present.
- All four maintenance schedules are disabled until Phase 1 adds consumers.
- The tag-filtered USD 50 monthly budget exists.
- No NAT gateway, RDS Proxy, ECS service, load balancer, CDN, vector database,
  or SageMaker endpoint was introduced.

Resource identifiers, endpoints, account identifiers, and secret values are
intentionally omitted from this document.

## Verification

```text
pytest: 37 passed
OpenAPI structural validation: passed
OpenAPI specification validation: passed
Python wheel build: passed
Serverless package: passed
AWS CloudFormation template validation: passed
Deployed resource smoke audit: passed
```

## Phase boundary

Phase 0 does not implement uploads, media ingestion APIs, worker consumption,
mobile UI, or Windows UI. Those remain Phase 1 and later work. The disabled
schedules prevent background cost or unconsumed queue growth before then.
