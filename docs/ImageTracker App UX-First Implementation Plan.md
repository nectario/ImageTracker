# ImageTracker App — UX-First Implementation Plan

## 1. Product and Architecture Summary

Build ImageTracker as a consumer media app whose first release includes native iOS and Android apps, a polished Python CLI, and a small Python serverless backend living inside the existing DeepTrading AWS/RDS infrastructure.

Core decisions:

- Mobile first, with feature-parity native SwiftUI and Jetpack Compose apps in v1.
- “Upload and relive” experience: timeline, search, map, media details, transcripts, and clear sync status.
- Separate ImageTracker email/password accounts through Amazon Cognito; no organizations, MFA, or enterprise RBAC in v1.
- Local or Remote storage selected per source/device; Local is the onboarding default.
- Local mode keeps originals on the source device/computer and stores metadata in the `ImageTracker` MySQL database.
- Remote mode stores one exact original in private S3 and makes it available across devices.
- Local-only assets are hidden on other devices rather than showing empty placeholders.
- Photos and videos are first-class `MediaAsset` types.
- Video audio is transcribed automatically through ElevenLabs within hard monthly quotas.
- Exact duplicates are stored once per user, while each device/path remains a separate source occurrence.
- Deletions mirror source-library deletions, with a 30-day recoverable trash period.
- MySQL remains the initial Intelligence Layer and queryable evidence source; semantic agents come later.
- Reuse the existing `ImageTracker` database and DeepTrading AWS account in `us-east-2`; do not create another RDS instance.
- Keep incremental infrastructure below $50/month by avoiding new always-on services, NAT gateways, load balancers, RDS Proxy, vector databases, and persistent SageMaker endpoints.

## 2. Key Implementation Changes

### Monorepo and compatibility

Organize the repository into:

- Native iOS and Android applications.
- Python API and asynchronous worker.
- Python Typer/Rich CLI.
- Versioned OpenAPI contract and generated Swift/Kotlin/Python clients.
- A minimal independent Serverless Framework stack.
- Existing importer and tagging utilities retained as compatibility entry points during migration.

Keep `ImageAsset` unchanged and readable throughout migration. It becomes legacy source data after cutover rather than being renamed or destructively altered.

### Consumer UX

Onboarding contains only the necessary decisions:

1. Create account, enter the one-time Cognito email verification code, and remain signed in afterward.
2. Explain and request full photo-library permission.
3. Create a device source using Local mode by default; clearly offer Remote mode as “Available on all devices.”
4. Start full-library indexing. Remote historical uploads default to Wi‑Fi plus charging; users may later enable cellular uploads.

Primary navigation:

- **Timeline:** local-first chronological media grid grouped by day.
- **Search:** filename, description, transcript, category, date, media type, and location search.
- **Map:** clustered location browsing with date/media filters.
- **Activity:** compact upload/indexing/processing status, failures, quotas, and automatic retries.
- **Settings:** source mode, network behavior, provider limits, account, trash, and device management.

Media details show:

- Original filename and source/device.
- Photo or video playback.
- Local date/time, UTC context, GPS/location, and category.
- AI description and provenance.
- Video transcript with speaker labels and timestamp navigation.
- Processing state and one actionable retry only when automatic recovery is exhausted.

User-facing states remain simple: `Preparing`, `Uploading`, `Processing`, `Ready`, `Waiting for monthly quota`, and `Needs attention`. Internal storage and processing states remain separate.

The app immediately renders local PhotoKit/MediaStore content from its device cache; server synchronization must not block the initial timeline. Light/dark mode, accessibility, dynamic text, localization-ready strings, and platform-native navigation are required.

### Native clients

**iOS**

- SwiftUI application using PhotoKit identifiers and change tracking.
- Background `URLSession` for Remote uploads and background task scheduling for manifest/hash work.
- Keychain token storage and a persistent native sync queue.
- Treat limited/revoked photo permission as an availability change, never as evidence that assets were deleted.

**Android**

- Jetpack Compose application using MediaStore identifiers/change observation.
- WorkManager for resumable background work and the platform-required foreground notification for long-running uploads.
- Keystore token storage and Room-backed sync state.
- Apply the same deletion safeguards and API contract as iOS.

Both apps share behavior and contract fixtures, not UI code.

### Local and Remote source behavior

`MediaSource.StorageMode` is `Local` or `Remote`.

Local mode:

- Store source metadata, exact filename, device-local locator, content hash, extracted metadata, and provenance in MySQL.
- Do not persist originals or previews in S3.
- Display the asset only to the owning source device.
- For captions/transcription, temporarily upload the asset to a private staging prefix, send a short-lived URL to the provider, and delete the staging object after completion. A one-day S3 lifecycle rule is the safety net.

Remote mode:

- Preserve and upload the exact original without recompression.
- Upload a small client-generated preview to avoid server image-processing expense.
- Make the original, preview, and derived metadata available to every device on the account through short-lived signed URLs.
- Upload directly from the client to S3; media bytes never flow through the API Lambda.

Mode changes:

- Local → Remote uploads missing originals once and retains existing occurrence history.
- Remote → Local stops future uploads but does not immediately purge existing S3 objects. “Remove cloud copies” is a separate destructive action that moves eligible objects into 30-day trash.
- If any Remote occurrence still references an asset, the S3 original remains available.

### Identity and exact deduplication

Use a two-level identity model:

- `MediaAsset`: one logical content object per user.
- `MediaOccurrence`: every phone-library item, Windows path, or imported legacy row where that content appears.

Deduplication rules:

- Stream SHA-256 on the source device/computer.
- Unique key: `(UserId, ContentSha256)`.
- Never deduplicate across different users.
- Exact duplicate bytes create another `MediaOccurrence` but no second S3 upload.
- Edited files or other byte-level variants create separate assets.
- Perceptual/near-duplicate detection is out of v1.
- Remote upload cannot start until the exact hash is known.
- Concurrent duplicate discovery is handled through a database upload lease; only one occurrence receives an upload plan.
- S3 keys are content-addressed and never contain personal filenames.
- If the last occurrence disappears, the asset enters trash; deletion is cancelled if any occurrence reappears during the 30-day window.

For a large initial library, source identifiers and metadata are indexed immediately while hashing proceeds in the background on charging power. The UI shows local items immediately; cross-device deduplication becomes authoritative as hashes complete.

### MySQL model

Create new PascalCase tables inside the existing `ImageTracker` database:

- `UserAccount`: Cognito subject and account state.
- `Device`: app installation, platform, last activity, and sync cursor.
- `MediaSource`: device/folder/library identity, mode, permission state, and sync settings.
- `MediaAsset`: user, SHA-256, media type, MIME type, byte size, canonical metadata, local/UTC capture time, storage state, S3 locator, and lifecycle timestamps.
- `MediaOccurrence`: exact filename, platform asset ID or local path, source revision, last-seen time, and deletion state.
- `MediaLocation`: raw GPS, reverse-geocoded values, normalization provenance, and confidence.
- `MediaDescription`: generated description, provider/model/prompt version, status, and timestamps.
- `MediaTranscript`: full searchable text, language, provider/model/request ID, duration, and status.
- `MediaTranscriptSegment`: ordered start/end times, speaker, text, confidence, and optional word-timing JSON.
- `UploadSession`: upload lease, multipart state, checksum, expiry, and completion.
- `ProcessingJob`: job type, attempt count, retry time, failure classification, and status.
- `ProviderUsageMonth`: per-provider/user processed units and hard quota.
- `MediaChange`: monotonic incremental-sync feed for apps and CLI.
- `LegacyImageAssetMap`: legacy ID to new asset/occurrence and migration status.

Important constraints:

- Unique `(UserId, ContentSha256)` on assets.
- Unique `(MediaSourceId, SourceItemId)` on occurrences.
- Every query is scoped by `UserId`.
- FULLTEXT indexes cover descriptions and transcript text.
- Date, media type, category, location, processing state, and change-cursor indexes support the v1 screens.
- Preserve exact filenames per occurrence.
- Persist extraction/provider provenance so agents can distinguish EXIF, device metadata, file mtime, Google, manual edits, and AI output.

### API contract

Use REST/JSON with OpenAPI as the source of truth.

Primary resources:

- `MediaSource`
- `MediaAssetSummary` and `MediaAssetDetail`
- `MediaOccurrence`
- `UploadPlan`
- `ProcessingJob`
- `MediaTranscript` and `MediaTranscriptSegment`
- `ChangePage`

Endpoint groups:

- `/v1/me` and device registration.
- `/v1/sources` create/list/update/remove and mode changes.
- `/v1/sources/{id}/manifest` for batched occurrence discovery and deletions.
- `/v1/uploads/plan`, multipart-part signing, completion, and cancellation.
- `/v1/changes` for cursor-based incremental synchronization.
- `/v1/media` timeline/filter/search and `/v1/media/{id}` details.
- `/v1/media/{id}/trash` and `/restore`.
- `/v1/jobs` list/status/retry.
- `/v1/webhooks/elevenlabs` for asynchronous transcription completion.
- Restricted `/v1/admin` operations for audit, legacy migration, quota changes, and repair.

Every mutating endpoint accepts an idempotency key. Pagination is keyset/cursor-based rather than offset-based.

### Backend and AWS deployment

Create a separate minimal Python Serverless service in the existing DeepTrading AWS account:

- API Gateway HTTP API with Cognito JWT authorization.
- Python 3.12 FastAPI/Mangum API Lambda.
- SQS processing queue and dead-letter queue.
- Small-concurrency worker Lambdas for metadata, geocoding, caption submission, transcription submission, webhook completion, and cleanup.
- EventBridge schedules for retry, reconciliation, quota reset, and 30-day trash purge.
- Private S3 storage for Remote originals, previews, and temporary Local-mode processing.
- Existing public DeepTrading RDS/MySQL capacity, but only the `ImageTracker` database.
- Dedicated least-privilege `ImageTracker` DB user and a 1–2 connection pool per Lambda runtime.
- SSM SecureString parameters for DB/provider credentials; never bundle `.env` files or expose provider keys to clients.
- No new RDS, ECS, Elastic Beanstalk environment, load balancer, NAT gateway, RDS Proxy, CDN, or vector store.

S3 policies:

- Block Public Access.
- Default SSE-S3 only; no custom KMS architecture in v1.
- Signed PUT/multipart uploads and signed GET/range requests.
- Server-side checksum validation for multipart completion.
- Abort incomplete multipart uploads after seven days, matching AWS’s cost-control guidance. [AWS multipart uploads](https://docs.aws.amazon.com/us_en/AmazonS3/latest/userguide/mpuoverview.html), [AWS lifecycle cleanup](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpu-abort-incomplete-mpu-lifecycle-config.html)
- Delete Local-mode staging bytes after provider completion, with one-day lifecycle fallback.
- Delete trashed Remote originals after 30 days.
- Use S3 Intelligent-Tiering for originals and Standard storage for small previews.
- Add an AWS budget alert for ImageTracker-tagged resources.

Cognito email/password tokens are passed to API Gateway in the authorization header and verified by the Cognito authorizer. [AWS Cognito/API Gateway integration](https://docs.aws.amazon.com/apigateway/latest/developerguide/apigateway-enable-cognito-user-pool.html)

### Enrichment, transcription, and search

Processing order:

1. Metadata and hash.
2. Temporal/GPS validation.
3. Persistent geocode/timezone cache lookup.
4. Reverse geocoding only on a cache miss.
5. Photo description.
6. Video transcription.
7. Search-index update.

ElevenLabs integration:

- Provider adapter with ElevenLabs as the default.
- Submit the S3 original or Local staging object through a short-lived `source_url`.
- Use asynchronous webhooks with job correlation metadata and signature verification.
- Store full text plus normalized timestamped speaker segments.
- Selecting a search result opens the video at the segment start time.
- Direct audio extraction is not part of v1 because ElevenLabs currently accepts video URLs and supports asynchronous timestamped results. [ElevenLabs transcript API](https://elevenlabs.io/docs/api-reference/speech-to-text/convert?explorer=true), [ElevenLabs webhook workflow](https://elevenlabs.io/docs/eleven-api/guides/how-to/speech-to-text/batch/webhooks)

Quota behavior:

- Captions and transcriptions run automatically for new media.
- Hard monthly provider-unit limits are configured server-side.
- No automatic provider overage.
- Jobs beyond the limit become `DeferredQuota`, remain visible in Activity, and resume after reset or an operator quota increase.
- Provider authentication, quota, and unsupported-format errors trip a circuit breaker rather than retrying every asset.
- Retry transient failures with bounded exponential backoff.

SageMaker remains behind the provider interface for later batch embeddings, experiments, or provider replacement. No persistent endpoint is deployed in v1.

Search uses MySQL FULLTEXT plus structured filters. Natural-language question answering, vector search, face recognition, and a separate Intelligence Layer service are explicitly deferred.

### CLI

Build a first-class Python CLI using Typer, Rich, generated API types, and OS keyring storage. Package standalone Windows and Linux executables; development commands continue to use `python`.

Command surface:

```text
imagetracker auth login|logout|status
imagetracker source add|list|set-mode|remove
imagetracker sync [SOURCE] [--watch] [--dry-run]
imagetracker upload FILE...
imagetracker status [--follow]
imagetracker media list|show|search|trash|restore
imagetracker jobs list|retry
imagetracker legacy audit|migrate
imagetracker admin db-audit|quota|reprocess
imagetracker doctor
```

CLI requirements:

- Same API and dedupe path as mobile.
- Exact filename preservation and resumable multipart upload.
- Rich progress with hashed, skipped-duplicate, uploaded, processing, deferred, and failed counts.
- `--json` for automation.
- `--no-input` for unattended sync.
- Prompts only for destructive actions; `--yes` supports intentional automation.
- Stable exit codes for configuration, authentication, partial-sync, network, and service failures.
- User commands never connect directly to MySQL; restricted admin/audit commands may use an explicitly read-only DB connection.

### Windows phase

After the mobile v1 contract stabilizes, create a packaged C# WinUI 3 app using the Windows App SDK.

- Derive its complete visual identity from Nektron Write and Nektron Mail, including typography, color, spacing, iconography, motion, density, and interaction character.
- Treat aesthetic and UX review as a release gate; generic template styling is not an acceptable finished result.
- Reuse the same API, source, hash, upload, change-feed, and processing semantics.
- Add folder selection, recursive watching, Local/Remote mode, resumable upload, timeline/search/map, media detail, and Activity.
- Use one `NavigationView` shell, built-in WinUI controls, virtualized media collections, light/dark/high-contrast support, keyboard accessibility, and native notifications.
- Keep a single main window and Store-compatible packaged deployment.
- The CLI remains the automation and migration interface; the WinUI app is the polished interactive experience.

## 3. Delivery Sequence

### Phase 0 — Foundation and baseline

- Verify GitHub write/push access before mutations.
- Record a DB backup and preserve `ImageAsset`.
- Rotate credential-like values already exposed in tracked DeepTrading configuration; do not start a broader security redesign.
- Scaffold the monorepo, OpenAPI contract, minimal Serverless stack, Cognito pool, S3 bucket, and new migrations.
- Add cost/resource tags and budget alarms.

Exit criteria: empty new schema deploys safely beside `ImageAsset`; authenticated health endpoint works; no other DeepTrading database is reachable by the app user.

### Phase 1 — Data core, API, and CLI

- Implement accounts, sources, assets/occurrences, hashing/deduplication, manifest sync, change feed, jobs, and CLI.
- Implement Local mode end to end.
- Add the legacy audit/migration command with resumable checkpoints and dry-run reporting.

Exit criteria: two paths containing identical bytes create one asset/two occurrences; Local sync sends no original to S3; CLI can resume safely after interruption.

### Phase 2 — Native mobile Local mode

- Build the complete iOS and Android onboarding, local cache, timeline, search shell, map shell, details, and Activity.
- Implement full-library discovery, staged background hashing, incremental changes, and safe deletion detection.
- Keep local assets visible only on their source device.

Exit criteria: both apps index a large library without blocking normal use, survive restart/network loss, and never interpret permission loss as deletion.

### Phase 3 — Remote mode and cloud lifecycle

- Implement presigned single/multipart uploads, upload leases, exact dedupe, previews, signed playback, mode upgrades, mirror deletions, trash, and restore.
- Add incomplete-upload, staging, and trash lifecycle rules.

Exit criteria: duplicate content is never uploaded twice; interrupted video upload resumes; Local → Remote works without duplicating metadata; last-occurrence deletion is recoverable for 30 days.

### Phase 4 — Enrichment and relive experience

- Correct EXIF sub-IFD capture-time handling before processing new media.
- Add persistent geocode/timezone caching, automatic descriptions, ElevenLabs transcription, quota controls, circuit breakers, transcript search, and timestamp seek.
- Complete timeline, map, search, and polished media detail UX.

Exit criteria: transcript search opens the right video moment; quota exhaustion defers rather than fails jobs; no provider error produces per-asset request storms.

### Phase 5 — Legacy migration and mobile beta

- Create the initial ImageTracker account and source for the existing archive.
- Compute hashes from original files and migrate all legacy rows as occurrences.
- Preserve current values; mark the one confirmed temporal inconsistency for review rather than silently changing it.
- Queue the 556 missing descriptions under quota.
- Keep `ImageAsset` read-only and maintain row-level migration mapping.
- Run iOS and Android beta with the same acceptance suite.

Expected migration invariant: 2,099 legacy occurrences; `MediaAsset` count may be lower only where byte hashing proves exact duplicates.

### Phase 6 — Windows app

- Verify the WinUI development environment and scaffold the packaged app from the official template.
- Audit the in-development Nektron Write and Nektron Mail products and codify their shared design tokens and interaction patterns before composing screens.
- Build the Windows uploader/library using the stable v1 API.
- Validate background folder watching, keyboard/touch UX, responsive layouts, accessibility, build, packaged launch, and upgrade behavior.

## 4. Test and Acceptance Plan

### Automated backend tests

- EXIF date/GPS fixtures including sub-IFDs, offsets, DST boundaries, HEIC, JPG, PNG, and video metadata.
- SHA-256 dedupe across paths/devices, concurrent upload leases, edited variants, and failed uploads.
- Local/Remote mode transitions and staging deletion.
- Manifest idempotency, incremental cursors, permission-loss safeguards, occurrence deletion, 30-day trash, restore, and last-reference rules.
- User ownership isolation on every query and upload.
- Quota accounting, deferred jobs, retries, circuit breakers, and webhook idempotency/signature validation.
- Transcript segmentation, speaker/timestamp search, and seek offsets.
- Fresh-schema migrations and migration from a sanitized legacy fixture.
- API schema compatibility for generated Swift/Kotlin/Python clients.

### Native and CLI tests

- iOS PhotoKit and Android MediaStore permission states, full/limited access, change detection, offline restart, and background constraints.
- Timeline/search/map/detail visual and accessibility tests in light, dark, high contrast, larger text, and narrow layouts.
- Large-library indexing without UI-thread stalls.
- Wi‑Fi/charging defaults and cellular override.
- CLI interactive, JSON, unattended, interruption/resume, duplicate skip, and exit-code scenarios.

### End-to-end scenarios

1. New user signs up, grants access, accepts Local default, and immediately sees their device timeline.
2. Same exact media appears on two devices: one asset, two occurrences, zero duplicate upload.
3. Source switches to Remote: original uploads once and becomes playable elsewhere.
4. Large video upload is interrupted and resumes.
5. Local video is temporarily staged, transcribed, searchable, and staging bytes disappear.
6. Provider quota is reached: pending jobs defer without repeated prompts and resume next cycle.
7. Device deletes its final occurrence: media enters trash, restores successfully, and expires after 30 days.
8. Permission is revoked: no media is deleted.
9. Legacy migration preserves 2,099 occurrences, exact filenames, categories, locations, and provenance while flagging the temporal anomaly.
10. An agent or operator can query MySQL evidence and trace a result back to the asset and occurrence.

### UX and operational acceptance

- After sign-in, onboarding requires only photo permission and the remembered storage-mode choice.
- Automatic recovery handles ordinary network/provider failures without user confirmation.
- Activity accurately distinguishes indexing, hashing, duplicate skips, uploads, provider processing, quota deferral, and actionable failures.
- Local content appears from device cache immediately.
- Remote media playback uses signed range-capable URLs.
- No new fixed AWS service materially threatens the sub-$50 incremental monthly target.
- CloudWatch metrics report upload bytes, duplicate bytes avoided, S3 bytes, job latency/failures, provider units, deferred backlog, and database connection usage.

## 5. Assumptions and Explicit Deferrals

- The new monorepo supersedes the earlier root-script-only constraint; legacy scripts remain compatible during transition.
- The single deployment environment lives in the existing DeepTrading AWS account and `us-east-2`.
- New tables live only in the `ImageTracker` database, despite other databases sharing the RDS instance.
- Current DeepTrading infrastructure spend is existing baseline; the $50 target applies to incremental ImageTracker infrastructure.
- Local is the default for every new source.
- Local-only assets are hidden from other devices, though their MySQL evidence remains available for future authorized intelligence queries.
- Dedupe is exact, per user, and content-hash based; no global or perceptual dedupe.
- Original media is never recompressed.
- Remote deletion follows last-occurrence semantics with 30-day trash.
- Initial Remote backlog uploads only on Wi‑Fi and charging.
- Automatic AI enrichment has hard quotas and no automatic overage.
- Standard S3 encryption, HTTPS, Cognito, scoped DB access, and private buckets are sufficient for v1; custom encryption, zero-knowledge design, enterprise policy systems, and comprehensive DeepTrading security remediation are deferred.
- SageMaker is optional and on-demand only.
- Natural-language agents, embeddings/vector search, face recognition, household sharing, enterprise roles, and cross-product identity are deferred until the media app and evidence tables are reliable.
