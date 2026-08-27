# Phase 1 Local Mode Status

Implementation snapshot: 2026-08-27. The Local-mode build is deployed to
`image-tracker-prod` in `us-east-2` and the evidence below was measured against
that stack.

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
permanent S3 storage.

## Cost posture

Phase 1 reuses the existing DeepTrading infrastructure and the Phase 0
serverless foundation. The Local path performs hashing and metadata extraction
on the user's computer, then sends compact manifests to on-demand API compute
and the existing MySQL database.

- No NAT gateway, load balancer, CDN, RDS Proxy, ECS service, vector database,
  or always-on SageMaker endpoint is introduced.
- Existing processing queues and schedules remain bounded/disabled until a
  later enrichment phase has a quota-aware consumer.
- Exact deduplication avoids repeated asset records now and repeated S3
  originals when Remote mode is implemented.
- The existing tag-filtered USD 50 monthly AWS budget remains the initial
  guardrail for ImageTracker-specific resources.
- The live acceptance run used 150–151 MB of the 512 MB Lambda allocation;
  after the cold start, measured requests completed in roughly 26–126 ms.

## Deliberately deferred

- Remote-mode upload, multipart resume, S3 retrieval, previews, and cross-device
  original availability.
- Reverse geocoding and human-friendly GPS place interpretation.
- AI captions, ElevenLabs video-audio transcription, face detection,
  recognition, and person clustering.
- Legacy migration writes; Phase 1 exposes read-only audit plus an optional
  local preview cursor that never claims rows were migrated.
- iOS, Android, and packaged WinUI 3 product interfaces.

The API contract may reserve later endpoint shapes. A reserved contract is not
evidence that its runtime capability is enabled.

## Acceptance evidence ledger

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

## Release gate

Phase 1 Local mode has passed its repository, package, deployed authentication,
database reconciliation, exact-deduplication, cleanup, and S3 no-write gates.
Production enrichment and native UX work remain outside this phase boundary.
