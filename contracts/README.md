# ImageTracker API contracts

`v1/openapi.json` is the source of truth for the first ImageTracker HTTP API. It
uses OpenAPI 3.0.3 so Swift, Kotlin, and Python client generators can consume it
without relying on OpenAPI 3.1-only JSON Schema features.

## Conventions

- JSON properties use `camelCase`; database column naming is independent.
- Resource identifiers are UUIDs. Timestamps ending in `Utc` are RFC 3339 UTC
  values. `capturedAtLocal` is a local wall-clock value and is paired with time
  zone and/or offset metadata when known.
- Authenticated client requests use a Cognito bearer token. Media and change-feed
  reads also send `X-ImageTracker-Device-Id` so Local-only media can be filtered
  to the owning device.
- Every client mutation requires an `Idempotency-Key`. Reusing a key with an
  identical request replays the original response; reusing it with a different
  request returns `409 IDEMPOTENCY_KEY_REUSED`.
- Collection pagination uses an opaque `cursor` and `nextCursor`. Clients must
  never parse or synthesize cursor values.
- Errors use `application/problem+json` with a stable machine-readable `code`.
- The ElevenLabs webhook is authenticated by its provider signature instead of
  Cognito and is idempotent by provider event/request identity.
- Multipart clients recover by reading the upload session, comparing completed
  parts, and requesting fresh URLs for at most 100 unfinished parts at a time.
- `assetContentSha256` identifies the exact original media; `objectSha256`
  validates whichever object is being uploaded, including a preview or
  temporary Local-mode staging object.
- Local sources never receive durable Original/Preview uploads. An authorized
  `TemporaryProcessing` job may receive a short-lived staging upload without
  changing the source or asset to Remote mode.

## Validation

Run the dependency-free structural validation from the repository root:

```powershell
python contracts/validate_openapi.py
```

The validator parses the JSON, resolves every local component reference, checks
operation IDs, and enforces the mutation/idempotency and cursor conventions. A
full OpenAPI linter or generated-client smoke test can be added when the backend
toolchain is scaffolded.
