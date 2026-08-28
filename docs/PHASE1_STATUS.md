# Phase 1 Local Mode and Enrichment Status

Implementation snapshot: 2026-08-28. The Local-mode core is deployed to
`image-tracker-prod` in `us-east-2`; the original acceptance evidence below was
measured against that stack on 2026-08-27. Reverse geocoding and scene
description are implemented in the current repository but have not been
deployed or run against the user's production library.

Production schema migrations `012` and `013` were applied and verified on
2026-08-28. The enrichment package subsequently passed the complete 258-test
suite, repository artifact validation, and AWS CloudFormation validation. The
application/worker deployment remains held for explicit production approval.

## Scope delivered in the repository

- Cognito-backed account sessions and authenticated device registration.
- Local folder source create/list/update/remove behavior.
- Recursive photo/video scanning, exact SHA-256 hashing, metadata extraction,
  and a persistent SQLite cache.
- Durable manifest outbox batches with stable idempotency keys and retry-safe
  acknowledgement, structured failure quarantine, inspection, and release.
- MySQL-backed accounts, devices, sources, assets, occurrences, locations,
  change feed, processing jobs, and request idempotency.
- Exact per-user deduplication: one `MediaAsset` may have multiple
  `MediaOccurrence` rows for distinct source paths.
- Timeline, search, detail, changes, and job service endpoints required by
  future native clients.
- Read-only `ImageAsset` audit and bounded, resumable-by-ID migration preview.

## Enrichment candidate in the repository

- A GPS-bearing manifest creates an idempotent `Geocode` processing job. Before
  calling Amazon Location Service Places V2, the service looks for a complete
  same-user resolution within 5 metres and copies the full normalized address
  and provider provenance when one exists.
- A bounded SQS worker processes geocode jobs one message at a time.
  `ReverseGeocode` receives coordinates only and declares
  `IntendedUse=Storage`, `MaxResults=1`, and address-oriented place types. The
  durable provider identifier is `AmazonLocationPlacesV2`. The default hard
  limit is 1,000 provider calls per user per calendar month; exhausted work
  becomes `DeferredQuota`.
- Every eligible photo without a current description receives one idempotent
  `Description` job in `Preparing`. The CLI keeps an account-scoped durable
  outbox for preparing and staging its preview.
- The preview is a deterministic, metadata-free JPEG with a maximum 1,024-pixel
  long edge. It is checksum-bound to a single-part signed PUT under `staging/`,
  then exposed to the worker through a short-lived signed GET.
- The scene request uses `gpt-5.6-sol`, high image detail, Flex processing,
  reasoning effort `none`, `store: false`, prompt version `scene-search-v1`,
  and at most 24 words. Provider/model/prompt provenance is stored with the
  resulting description.
- Scene requests have a separate 1,000-call per-user monthly hard limit.
  Reservation happens before the service issues an upload URL, so quota
  exhaustion does not cause the CLI to stage an unbounded backlog.
- The worker deletes the staging preview after successful or safe terminal
  processing. A one-day S3 lifecycle policy handles cleanup if an immediate
  delete fails.
- Local originals remain path-referenced. `MediaAsset.StorageState` remains
  `LocalOnly`, and its durable `S3Bucket`, `OriginalS3ObjectKey`, and
  `PreviewS3ObjectKey` remain null throughout temporary processing.

## Local-mode flow

1. `imagetracker configure` discovers the API and Cognito identifiers from the
   selected CloudFormation stack.
2. Cognito authenticates the user; API Gateway and the API claim boundary bind
   every operation to that account.
3. `imagetracker source add` registers the installation and Local folder while
   saving the path binding only in the CLI's private SQLite state.
4. The scanner walks the folder without following symlinks, hashes recognized
   media in chunks, extracts available metadata, and caches unchanged results.
5. Changed occurrences and reliable deletions are saved as manifest batches in
   the local outbox before transmission.
6. The API applies each idempotent manifest transaction to MySQL. Equal hashes
   reuse the same per-user asset while each path retains its own occurrence.
7. Successful acknowledgement removes the local outbox batch. Interrupted or
   pending batches resume on the next sync; rejected entries are quarantined
   for inspection instead of replaying forever.

At no point in this Phase 1 flow is a Local original or preview written to
permanent S3 storage. The enrichment candidate adds only an authorized,
short-lived `TemporaryProcessing` preview; that object is not an asset locator
and does not make Local media remotely available.

## Cost posture

Phase 1 reuses the existing DeepTrading infrastructure and the Phase 0
serverless foundation. The Local path performs hashing and metadata extraction
on the user's computer, then sends compact manifests to on-demand API compute
and the existing MySQL database.

- No NAT gateway, load balancer, CDN, RDS Proxy, ECS service, vector database,
  or always-on SageMaker endpoint is introduced.
- The enrichment worker is bounded to one concurrent Lambda and one SQS message
  per batch. A five-minute retry/recovery sweep is enabled; reconciliation,
  quota-reset, and trash-purge schedules remain disabled.
- Amazon Location reverse geocoding reuses same-user results within 5 metres
  and has a 1,000-call monthly per-user ceiling. At the 2026-08-28 `us-east-2`
  list price, that caps this component at about USD 4 per user per month.
- OpenAI scene descriptions use reduced 1,024-pixel previews, Flex processing,
  and an independent 1,000-call monthly per-user ceiling.
- Exact deduplication avoids repeated asset records now and repeated S3
  originals when Remote mode is implemented.
- The existing tag-filtered USD 50 monthly AWS budget remains the initial
  guardrail for ImageTracker-specific resources.
- The live acceptance run used 150–151 MB of the 512 MB Lambda allocation;
  after the cold start, measured requests completed in roughly 26–126 ms.

## Deliberately deferred

- Remote-mode upload, multipart resume, S3 retrieval, previews, and cross-device
  original availability.
- Deployment and production-library acceptance of reverse geocoding and scene
  descriptions.
- ElevenLabs video-audio transcription, face detection, recognition, and
  person clustering.
- Legacy migration writes; Phase 1 exposes read-only audit plus an optional
  local preview cursor that never claims rows were migrated.
- iOS, Android, and packaged WinUI 3 product interfaces.

The API contract may reserve later endpoint shapes. A reserved contract is not
evidence that its runtime capability is enabled.

## Acceptance evidence ledger

This ledger describes the already deployed Local core, not the undeployed
enrichment candidate.

The deployed checks are reproducible with
[`infra/scripts/live_phase1_smoke.py`](../infra/scripts/live_phase1_smoke.py).
It creates one suppressed disposable Cognito user, scopes every database row to
that subject, and confirms both Cognito and MySQL cleanup before returning.

| Criterion | Expected result | Evidence |
| --- | --- | --- |
| Focused Phase 1 tests | API, domain/data, CLI, and shell tests pass | Pass; included in the 131-test final suite |
| Full repository tests | Entire pytest suite passes | Pass; 131 tests |
| Contract validation | Structural and specification checks pass | Pass; 27 operations, 87 schemas, 23 paths |
| Infrastructure package | Foundation validator and Serverless packaging pass | Pass; CloudFormation and root-level Lambda archive validated before deploy |
| Authenticated live API | Cognito user can call Phase 1 routes; unauthenticated call is rejected | Pass; two concurrent first-use calls returned the same account with 200; no token returned 401 |
| Exact duplicate behavior | Two different paths with identical bytes produce one asset and two occurrences | Pass live; `MediaAsset=1`, `MediaOccurrence=2`, distinct paths=2 |
| Repeat safety | Repeating the same manifest creates no duplicate asset or occurrence | Pass live; durable replay matched and a new-key rerun returned `unchanged=2` |
| Resume safety | An unacknowledged local outbox batch is sent successfully on the next sync | Pass in integration tests; failed entries are separately quarantined and releasable |
| Local storage boundary | Local sync performs zero original/preview writes to S3 | Pass live; bucket inventory remained 0 objects/0 bytes and every asset S3 locator was null |
| Reliable deletion | Completed scan emits a deletion; incomplete traversal does not infer deletion | Pass in scanner/domain integration tests; unreadable traversal exits partial |
| Legacy safety | Audit/preview use a read-only transaction and report zero writes | Pass live; 2,099 rows audited, one temporal-review row identified, bounded preview reported `writesPerformed=0` |

## Enrichment deployment prerequisites

- `/imagetracker/prod/openai` is provisioned as an SSM SecureString. The secret
  value is intentionally not recorded in repository documentation.
- Amazon Location Service Places V2 uses the Lambda execution role and requires
  no provider key or SSM parameter. The packaged IAM policy must grant only the
  `geo-places:ReverseGeocode` action scoped to `provider/default`, and every
  request must retain `IntendedUse=Storage` because ImageTracker persists and
  searches addresses. Production uses
  `IMAGETRACKER_GEOCODE_REUSE_RADIUS_METERS=5` and
  `IMAGETRACKER_GEOCODE_MONTHLY_CALL_LIMIT=1000`.
- The complete repository suite, OpenAPI validation, Lambda archive validation,
  Serverless packaging, and live disposable-user acceptance must pass with the
  required AWS access and OpenAI credential. Live acceptance must confirm
  address and description provenance, null durable S3 locators for Local
  assets, and staging cleanup.
- Deployment must run from WSL Ubuntu using the existing scoped AWS
  credentials. Provider keys must not appear in shell history, tracked files,
  CloudFormation parameters, logs, or test output.

## Release gate

Phase 1 Local mode has passed its repository, package, deployed authentication,
database reconciliation, exact-deduplication, cleanup, and S3 no-write gates.
The enrichment candidate is implemented but remains held at the IAM review,
final verification, deployment, and live-acceptance gates. Native UX work
remains outside this phase boundary.
