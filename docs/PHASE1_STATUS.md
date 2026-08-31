# Phase 1 Local Mode and Enrichment Status

Implementation snapshot: 2026-08-31. The Local-mode core is deployed to
`image-tracker-prod` in `us-east-2`; the original acceptance evidence below was
measured against that stack on 2026-08-27. Reverse geocoding and scene
description are now deployed in the bounded production worker. Only a synthetic
disposable photo—not the user's production library—was used for acceptance.

Production schema migrations `012` and `013` were applied and verified on
2026-08-28. The enrichment package subsequently passed the complete 275-test
suite, repository artifact validation, and AWS CloudFormation validation. The
authorized application/worker deployment completed successfully.

The 2026-08-29 performance release deployed `gpt-5.6-terra`, added
500-entry manifest prefetch/batched pending-occurrence writes, and introduced
progressive fast-add with parallel discovery and file metadata reads. On the
96-core/192-thread workstation, 160,769 media files were prepared in 31.04
seconds at 5,180 files/second; 64 workers outperformed 96 and remains the
auto-tuned cap.

The trusted WSL one-file importer was exercised against the real source on
2026-08-29: one 46.17 MiB CSV carried 160,767 rows through one
`LOAD DATA LOCAL INFILE` operation and one commit. The set merge inserted
95,267 missing occurrences and left 65,500 existing rows untouched. Local
known-occurrence state reconciled to 160,767, pending API batches fell to zero,
and the Local media S3 bucket remained empty.

The CLI now mirrors the user's Ubuntu Powerlevel10k palette with warm burnt
orange, ochre, muted teal, sage green, cream, and graphite. Scene previews also
accept MPO multi-picture JPEGs commonly emitted by Sony cameras, use their
primary frame, and automatically retry jobs previously cancelled as
`UnsupportedPhoto` when the upgraded decoder can now read the file.

A real deep-index replay exposed the API's synchronous ceiling: a 500-entry
hash-enriched manifest reached the Lambda timeout at exactly 28.0 seconds and
returned HTTP 500. The CLI now uses 100-entry transport batches, automatically
replaced 238 saved oversized requests with 1,188 durable smaller requests, and
successfully processed measured production batches in 4.7–8.4 seconds. Saved
manifests resume before optional scene enrichment, retryable preview deferrals
no longer produce a false failure exit, and a per-source process lock prevents
overlapping CLI syncs.

The 2026-08-30 repository fast path was deployed to production at 02:18 UTC on
2026-08-31. Existing descriptions and jobs are prefetched once, new
hash-addressed assets are prepared as a batch, and new occurrences,
description jobs, and their change rows flush at batch boundaries instead of
repeatedly per photo. A 50-photo regression test bounds the manifest to six
SELECT statements, and mixed valid and rejected entries retain their original
outcomes without creating stray assets, occurrences, or jobs.

Two bounded production runs then exercised 16 new 100-entry manifests plus one
idempotent replay. The new non-replay calls averaged 8.91 seconds and peaked at
16.28 seconds, compared with 10.10 seconds average and 18.07 seconds maximum
for the final 36 successful pre-deployment manifests: a measured improvement
of about 12 percent on this mixed workload. All post-deployment API requests
avoided 5xx responses, the local manifest failure count remained zero, and an
interrupted committed request replayed safely. This is a useful reduction, but
not yet the desired WOW result; MySQL auto-increment/ORM insert work now
dominates, so the next large gain requires a truly set-based staging-table or
asynchronous bulk-ingest path.

That set-based path is deployed, and migration 014 is recorded in production.
Migration 014 adds durable import,
raw/normalized staging, asset-work, and failure-audit tables plus an online
occurrence index. The authenticated API issues a checksum-bound private S3
upload, a dedicated one-concurrent 15-minute Lambda performs one validated
`LOAD DATA LOCAL INFILE` and set merge, and a durable result artifact lets the
CLI resume SQLite reconciliation after interruption. `sync --transport auto`
uses bulk above 10 batches or 1,000 eligible rows and preserves the existing
batch outbox for all fallbacks. Ordinary batch and bulk manifests are now
strictly metadata-only: they store raw coordinates but cannot create, requeue,
or dispatch any enrichment job. The device-scoped `enrich --limit N` command
uses a separate idempotent preparation mutation; `--with-enrichment` is an
explicit convenience alias after metadata fully drains.

The current candidate passes 370 tests and validates 34 OpenAPI operations,
97 schemas, and 30 paths. Production migration 014 and the self-cleaning
four-row MySQL canary pass. A 1,000-row bulk stage also completed with zero
metadata rejects, but exposed that the previous worker automatically dispatched
Amazon Location jobs. The general enrichment event mapping and recovery rule
were immediately disabled. Commit `ce2ff53` now encodes that pause in
CloudFormation and rejects explicit preparation before creating jobs while
paused. Its production 10,000-row metadata gate completed in 10.30 seconds with
10,000 updates, zero rejects, and no change to `ProcessingJob` count/max ID,
job-state totals, either provider ledger, the general processing queue, or the
local scene outbox. The CLI supports staged gates through `--bulk-max-rows`; a
completed segment stops before rescanning and leaves uncaptured batches
pending. After explicit approval, one final 77,967-row import consumed all 780
remaining batches in 72.25 seconds using 206 MiB of the 1,024 MiB worker. It
also produced zero rejects and zero enrichment, provider-ledger, scene-outbox,
or queue delta. No pending or failed manifest batches remain. The optional
fresh filesystem rescan that follows an empty outbox was interrupted before it
created a scan or manifest record, keeping this rollout scoped to saved work.

## Scope delivered in the repository

- Cognito-backed account sessions and authenticated device registration.
- Local folder source create/list/update/remove behavior.
- Parallel photo/video discovery, progressive fast-add, exact SHA-256 hashing,
  metadata extraction, and bulk writes to a persistent SQLite cache.
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

- A GPS-bearing manifest stores raw coordinates and may reuse an already-stored
  same-user resolution within 5 metres without a provider call. Only explicit
  enrichment preparation creates an idempotent `Geocode` processing job.
- A bounded SQS worker processes geocode jobs one message at a time.
  `ReverseGeocode` receives coordinates only and declares
  `IntendedUse=Storage`, `MaxResults=1`, and address-oriented place types. The
  durable provider identifier is `AmazonLocationPlacesV2`. The default hard
  limit is 1,000 provider calls per user per calendar month; exhausted work
  becomes `DeferredQuota`.
- Explicit enrichment preparation creates one idempotent `Description` job for
  each selected eligible photo. The CLI durably saves the request identity and
  returned local staging tasks before preparing previews.
- The preview is a deterministic, metadata-free JPEG with a maximum 1,024-pixel
  long edge. It is checksum-bound to a single-part signed PUT under `staging/`,
  then exposed to the worker through a short-lived signed GET.
- The scene request uses `gpt-5.6-terra`, high image detail, Flex processing,
  reasoning effort `none`, `store: false`, prompt version `scene-search-v1`,
  and at most 24 words. Provider/model/prompt provenance is stored with the
  resulting description.
- Scene requests reserve both one request and USD 0.010 before the service
  issues an upload URL. A 100,000-request ceiling remains as a secondary
  guardrail, while the hard per-user monthly spend ceiling is USD 230. Success
  reconciles the reservation from sanitized input/cached-input/output usage at
  USD 2.00/M, USD 0.20/M, and USD 12.00/M respectively. Missing usage or a
  called-provider failure consumes the conservative USD reservation; failures
  before a provider call release it.
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
4. The scanner walks folders concurrently without following symlinks. Fast-add
   can register directory metadata immediately; the deep-index pass hashes and
   extracts metadata with bounded workers, then caches results in bulk.
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
  per batch. Its event mapping, API preparation switch, and five-minute
  retry/recovery sweep are disabled during metadata rollout. The bulk worker
  and bulk recovery schedule remain enabled; other maintenance schedules remain
  disabled.
- Amazon Location reverse geocoding reuses same-user results within 5 metres
  and has a 1,000-call monthly per-user ceiling. At the 2026-08-28 `us-east-2`
  list price, that caps this component at about USD 4 per user per month.
- OpenAI scene descriptions use reduced 1,024-pixel previews, Flex processing,
  a 100,000-call secondary ceiling, and a harder USD 230 monthly per-user
  ceiling enforced through conservative pre-call reservation and actual usage
  settlement.
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

The original rows below describe the Local core; enrichment evidence is added
after them.

The deployed checks are reproducible with
[`infra/scripts/live_phase1_smoke.py`](../infra/scripts/live_phase1_smoke.py).
It creates one suppressed disposable Cognito user, scopes every database row to
that subject, and confirms both Cognito and MySQL cleanup before returning.

| Criterion | Expected result | Evidence |
| --- | --- | --- |
| Focused Phase 1 tests | API, domain/data, CLI, and shell tests pass | Pass; included in the 370-test final suite |
| Full repository tests | Entire pytest suite passes | Pass; 370 tests |
| Contract validation | Structural and specification checks pass | Pass; 34 operations, 97 schemas, 30 paths |
| Infrastructure package | Foundation validator and Serverless packaging pass | Pass; CloudFormation and root-level Lambda archive validated before deploy |
| Authenticated live API | Cognito user can call Phase 1 routes; unauthenticated call is rejected | Pass; two concurrent first-use calls returned the same account with 200; no token returned 401 |
| Exact duplicate behavior | Two different paths with identical bytes produce one asset and two occurrences | Pass live; `MediaAsset=1`, `MediaOccurrence=2`, distinct paths=2 |
| Repeat safety | Repeating the same manifest creates no duplicate asset or occurrence | Pass live; durable replay matched and a new-key rerun returned `unchanged=2` |
| Resume safety | An unacknowledged local outbox batch is sent successfully on the next sync | Pass in integration tests; failed entries are separately quarantined and releasable |
| Bulk MySQL canary | Exact deduplication, rejection audit, replay, and cleanup pass | Pass live; four rows produced two assets, three occurrences, one duplicate link, one expected rejection, and zero residue |
| Staged bulk metadata | 1,000 then 10,000 rows complete without metadata rejection | Pass live; 1,000/1,000 then 10,000/10,000, zero rejects and zero failed local batches |
| Complete saved backlog | Remaining batches clear in one bounded import | Pass live; 77,967/77,967 updates from 780 batches in 72.25 seconds, zero rejects, pending batches `0`, failed batches `0` |
| Metadata/enrichment boundary | Metadata changes no job, provider-ledger, scene-outbox, or general-queue state | Pass live at 10,000 rows; `ProcessingJob=73,155`, max ID `74,844`, provider rows and 240 paused messages remained exact |
| Local storage boundary | Local sync performs zero original/preview writes to S3 | Pass live; bucket inventory remained 0 objects/0 bytes and every asset S3 locator was null |
| Reliable deletion | Completed scan emits a deletion; incomplete traversal does not infer deletion | Pass in scanner/domain integration tests; unreadable traversal exits partial |
| Legacy safety | Audit/preview use a read-only transaction and report zero writes | Pass live; 2,099 rows audited, one temporal-review row identified, bounded preview reported `writesPerformed=0` |
| Stored reverse geocode | Synthetic GPS resolves through Amazon Location Places V2 | Pass live; provider `AmazonLocationPlacesV2`, street address `338-350 5th Ave` |
| Scene description | Metadata-free preview is described by the pinned model | Pass live; provider `OpenAI`, model `gpt-5.6-terra`, searchable sentence persisted |
| Temporary preview cleanup | Local asset remains LocalOnly and staging is deleted | Pass live; durable S3 locators null, zero staging objects after processing |
| Worker cost/recovery controls | Queue, concurrency, DLQ, retry rule, and alert are bounded | Pass live; enrichment mapping/retry disabled during metadata rollout, bulk mapping/retry enabled, both DLQs empty, 80% budget alert to `info@nektron.ai` |

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

Phase 1 Local mode and its bounded enrichment worker have passed repository,
package, authentication, database, exact-deduplication, cleanup, S3-boundary,
deployment, and live-acceptance gates. Native UX work remains outside this
phase boundary.
