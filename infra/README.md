# ImageTracker AWS foundation

This directory defines the small, independent ImageTracker AWS stack. It is
safe to package without contacting the database or reading application secrets.
Nothing here deploys automatically.

## What the stack creates

- A Python 3.12 Lambda behind an API Gateway HTTP API. `GET /v1/health` is
  protected by the Cognito JWT authorizer.
- A Cognito email/password user pool and public native-app client. MFA is off;
  email confirmation uses one code and refresh tokens last 365 days.
- One private, SSE-S3 media bucket. Incomplete multipart uploads expire after
  seven days, Local-mode staging objects after one day, and trash after 30 days.
  Remote originals transition to S3 Intelligent-Tiering.
- One encrypted processing queue plus a 14-day dead-letter queue.
- Retry, reconciliation, quota-reset, and trash-purge EventBridge rules. They
  are **disabled by default** until a bounded processing worker is connected, preventing
  an unconsumed queue backlog.
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
  services/
```

The deployed handler remains `services/api/handler.handler`. Runtime Python
packages are declared in `services/api/requirements.txt` and installed into the
staging directory. No duplicate API implementation is maintained here.

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
/imagetracker/<stage>/google
/imagetracker/<stage>/openai
/imagetracker/<stage>/elevenlabs
```

The DSN must select only the existing `ImageTracker` database. Do not reuse a
connection string whose default database is another DeepTrading schema.

After review, deployment is explicit:

```bash
npm run deploy -- --param="budgetEmail=you@example.com"
```

Other supported parameters are:

- `monthlyBudgetUsd` (default `50`)
- `allowedOrigin` (default `*`; native clients do not depend on browser CORS)
- `maintenanceSchedulesState` (`DISABLED` until a processing consumer exists)

AWS cost-allocation tags `Application` and `Environment` must be activated in
Billing once before the tag-filtered budget can report complete costs. Stack
resources use `Application=ImageTracker`, `Product=NektronAI`, the deployment
stage, and `ManagedBy=ServerlessFramework`.

The S3 bucket and Cognito user pool use CloudFormation retention policies. A
stack removal therefore does not destroy user media or accounts; retained
resources must be reviewed and removed separately when intentionally retiring a
stage.
