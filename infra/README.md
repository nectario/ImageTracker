# ImageTracker AWS foundation

This directory defines the small, independent ImageTracker AWS stack. It is
safe to package without contacting the database or reading application secrets.
Nothing here deploys automatically.

## What the stack creates

- A Python 3.12 Lambda behind an API Gateway HTTP API. `GET /v1/health` is
  protected by the Cognito JWT authorizer.
- A Cognito email/password user pool and public native-app client. MFA is off;
  email confirmation uses one code, refresh tokens last 365 days, and messages
  are sent through the verified SES identity `ImageTracker <info@nektron.ai>`.
- One private, SSE-S3 media bucket. Incomplete multipart uploads expire after
  seven days, Local-mode staging objects after one day, and trash after 30 days.
  Remote originals transition to S3 Intelligent-Tiering.
- One encrypted processing queue plus a 14-day dead-letter queue. A bounded
  worker consumes one message at a time with one reserved concurrent Lambda
  execution. Individual message failures are returned to SQS for retry and
  move to the dead-letter queue after eight receives.
- Retry, reconciliation, quota-reset, and trash-purge EventBridge rules. Only
  `RetryDueJobs` is **enabled by default** to recover a durable MySQL job after
  a lost or early SQS delivery. The other three rules remain disabled.
- A tag-filtered monthly AWS budget, defaulting to USD 50. An email subscriber
  is added only when `budgetEmail` is supplied.

The stack deliberately has no VPC attachment, NAT gateway, new database, RDS
Proxy, ECS service, load balancer, CDN, vector database, or SageMaker endpoint.
The Phase 1 API reaches only the existing `ImageTracker` database using its
dedicated credential and the bundled Amazon RDS regional CA.

## Application-code boundary

The handler is owned by [`../services/api`](../services/api), not by the
infrastructure project. Serverless resolves handlers relative to its service
directory, so `scripts/stage_service.py` creates an ignored `.build` directory
containing the full shared `services` package:

```text
.build/
  serverless.yml
  location_normalization_rules.json
  services/
```

The packaged handlers are `services/api/handler.handler` and
`services/worker/handler.handler`. Runtime Python packages are declared in
`services/api/requirements.txt` and installed into the staging directory. The
root-level location-normalization rules are staged at the Lambda task root so
both local and deployed processing use the same address corrections. No
duplicate service implementation is maintained here.

## Bounded reverse geocoding

GPS reverse-geocoding is asynchronous and demand-driven. A manifest creates a
location enrichment job; the SQS worker sends only latitude and longitude to
Amazon Location Service Places V2, never the photo or video. `ReverseGeocode`
sets `IntendedUse=Storage`, `MaxResults=1`, and filters to address-oriented place
types because ImageTracker permanently stores and searches the result. Durable
rows use provider identifier `AmazonLocationPlacesV2`. Nearby coordinates reuse
a stored result within 5 metres before any provider call is made. Provider
calls are capped at 1,000 per user per month, while the worker's concurrency of
one prevents a scan from producing an uncontrolled request burst. At the
2026-08-28 `us-east-2` list price of USD 4 per 1,000 stored reverse-geocode
requests, the default ceiling bounds this component to about USD 4 per user per
month. No new always-on compute service is introduced.

The production controls are
`IMAGETRACKER_GEOCODE_REUSE_RADIUS_METERS=5` and
`IMAGETRACKER_GEOCODE_MONTHLY_CALL_LIMIT=1000`. The Lambda execution role grants
only `geo-places:ReverseGeocode`, scoped to the regional
`provider/default` resource.

Scene descriptions use `gpt-5.6-sol` with high-detail, 1024-pixel previews,
Flex processing, and a separate 1,000-call per-user monthly ceiling. Local
originals remain on-device; only metadata-free temporary JPEG previews enter
the one-day staging prefix.

Geocode jobs use the Lambda execution role rather than a provider key. The
worker lazily reads `/imagetracker/<stage>/openai` only for description jobs, so
an unavailable OpenAI credential does not prevent Amazon Location jobs from
being composed. It uses the same MySQL secret and bundled Amazon RDS CA as the
API. Partial SQS batch responses are enabled with a batch size of one so
transient failures are retried without acknowledging the message.

## Credential-free validation

From this directory on Windows or WSL:

```bash
python scripts/validate_foundation.py --stage prod
python scripts/stage_service.py
```

The first command checks cost and architecture guardrails without AWS access.
The second verifies and stages the shared handler without installing anything.

Full Serverless validation requires Node.js, npm, and Python with pip. The build
installs CPython 3.12 `manylinux2014_x86_64` wheels explicitly, so it remains
reproducible when WSL's host interpreter advances beyond Lambda's version. The
intended environment is WSL Ubuntu because it already holds the project's AWS
credentials:

```bash
cd /mnt/c/Development/Projects/ImageTracker/infra
npm ci
npm run validate
npm run package
```

`npm run package` produces CloudFormation and Lambda artifacts under
`.build/.serverless`; it does not deploy them. Review those artifacts before the
first deployment, then run the credential-free artifact assertions:

```bash
python scripts/validate_foundation.py --stage prod \
  --packaged-template .build/.serverless/cloudformation-template-update-stack.json
```

The deploy script calls `scripts/deploy_packaged.py` after validation. That
helper passes absolute config and package paths to Serverless, avoiding its
different relative-path behavior between package, deploy, and post-deploy
hooks.

## Configuration and deployment

Use a short lowercase stage name containing only letters, numbers, and hyphens.
Bucket names include the stage, AWS account ID, and region; the remaining
resources are stage- and account-scoped.

The runtime is given SSM **parameter names**, not resolved secret values. Create
these SecureString parameters through the normal AWS administration flow before
enabling database or provider functionality:

```text
/imagetracker/<stage>/mysql
/imagetracker/<stage>/openai
/imagetracker/<stage>/elevenlabs
```

The DSN must select only the existing `ImageTracker` database. Do not reuse a
connection string whose default database is another DeepTrading schema.

For `prod`, `/imagetracker/prod/openai` has been provisioned. Amazon Location
Service Places V2 uses IAM authentication and has no additional SSM
prerequisite. Review the packaged policy to confirm the worker has only the
required reverse-geocode permission, and verify that requests retain
`IntendedUse=Storage`. Provider values must never be placed in tracked `.env`
files, Serverless parameters, CloudFormation output, or shell history.

Before deploying this enrichment build, pause ImageTracker sync/importer work,
verify the queue is empty, then preview and apply the two additive migrations:

```bash
cd /mnt/c/Development/Projects/ImageTracker
./scripts/migrate-db.sh
./scripts/migrate-db.sh --apply
./scripts/db-smoke.sh
```

Migration 012 adds the provider circuit columns and migration 013 widens
international provider/address fields. The wrapper refuses a non-`ImageTracker`
database or active processing rows and reconciles a DDL that committed before
its ledger marker.

After schema verification, deployment is explicit. Parameters must be supplied
to the release command so they are present during package rendering:

```bash
npm run deploy -- --param="budgetEmail=you@example.com"
```

Other supported parameters are:

- `monthlyBudgetUsd` (default `50`)
- `allowedOrigin` (default `*`; native clients do not depend on browser CORS)
- `retryScheduleState` (`ENABLED` for durable job recovery)
- `maintenanceSchedulesState` (`DISABLED` for deferred maintenance jobs)

AWS cost-allocation tags `Application` and `Environment` must be activated in
Billing once before the tag-filtered budget can report complete costs. Stack
resources use `Application=ImageTracker`, `Product=NektronAI`, the deployment
stage, and `ManagedBy=ServerlessFramework`.

The S3 bucket and Cognito user pool use CloudFormation retention policies. A
stack removal therefore does not destroy user media or accounts; retained
resources must be reviewed and removed separately when intentionally retiring a
stage.
