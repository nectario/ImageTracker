# Fresh-Model Onboarding Prompt — ImageTracker

You are taking over the **ImageTracker** project. Your first task is to understand the system accurately and establish a reliable mental model. **Do not modify code, database records, configuration, or infrastructure yet.**

## Repository and workspace

- GitHub repository: `git@github.com:nectario/ImageTracker.git`
- GitHub URL: `https://github.com/nectario/ImageTracker`
- Local workspace: `/mnt/c/Development/Projects/ImageTracker`
- Active branch: `main`
- Work only on `main`; do not create a branch.
- Use `python`, not `python3`.

## Read these sources first

Read the following in this order:

1. `docs/HANDOFF.md`
2. `README.md`
3. `ImageTracker.py`
4. `tag_location.py`
5. Every migration in `migrations/`
6. `tests/test_local_photo_sync.py`
7. `tests/test_tag_location.py`
8. `.env.example`
9. `pyproject.toml`
10. Recent Git history on `main`

Do not merely paraphrase `HANDOFF.md`. Validate its claims against the current code, migrations, tests, and Git history. Treat:

- the **code and tests** as the source of truth for implemented behavior;
- the **migrations** as the source of truth for intended database evolution;
- `HANDOFF.md` as the source of truth for operational history, user preferences, database observations, and known pitfalls.

If the sources disagree, identify the discrepancy explicitly. Do not silently reconcile, “clean up,” or replace a fact with an assumption.

## Strategic product context

ImageTracker began as a photo-ingestion utility, but its strategic role is larger.

Nektarios is building a **source-agnostic AI Intelligence Layer** into which users can connect different personal or organizational sources. An existing email-intelligence foundation already demonstrates this model. ImageTracker is intended to become the **photo and visual-history source** for that intelligence layer.

The long-term value is not simply storing photo metadata. It is turning photos into trustworthy, queryable evidence so an AI can answer questions such as:

- “Where was I a week ago?”
- “Show me the photos from the day I visited that location.”
- “What was happening around this time?”
- “Which photos were taken at home?”
- “What evidence supports that answer?”

A useful image record may combine:

- the exact original file name and source path;
- EXIF capture date and local time;
- UTC companion time and time-zone context;
- GPS coordinates and altitude;
- reverse-geocoded address fields;
- normalized location and category;
- an AI-generated visual description;
- provenance showing where each fact came from;
- enough source identity to trace the answer back to the original file.

The future Intelligence Layer should be able to consume these records without requiring ImageTracker to become the entire product.

### Future privacy and safety direction

Nektarios has discussed a future ingestion design in which sensitive data is checked for narrowly defined prohibited-content categories before encrypted persistence. The purpose of encryption is primarily to protect stored data from hackers or database compromise; this is **not currently defined as a zero-knowledge architecture**.

This is future direction, not current implementation. During your review, verify and state clearly:

- whether the current database stores image binaries or only metadata, descriptions, locations, timestamps, categories, and a local path reference;
- whether any application-level encryption is implemented;
- whether any prohibited-content or safety-classification stage is implemented;
- where such controls could eventually sit in the ingestion pipeline without weakening current functionality.

Do not claim that encryption or prohibited-content screening already exists unless the code proves it.

## Current implementation that you should verify

The expected active system is:

- A local Python importer in the single root file `ImageTracker.py`.
- It recursively scans `/mnt/d/Pictures/Camera Uploads`.
- The active CLI is:

```bash
python ImageTracker.py --directory "/mnt/d/Pictures/Camera Uploads" --cutoff-date "YYYY-MM-DD" [--force]
```

- The original OneDrive/Microsoft Graph design is legacy and inactive unless Nektarios explicitly asks to revive it.
- Local rows use `Source='LocalFile'`.
- Local identity is currently derived from a SHA-1 hash of the lower-cased resolved path.
- File names must be preserved exactly as they appear on disk.
- EXIF is the primary source for GPS and capture date/time.
- Google Geocoding and Time Zone APIs optionally enrich GPS records.
- OpenAI optionally generates concise image descriptions using `gpt-5.2`.
- Manual location/category tagging is handled by the separate root script `tag_location.py`.
- Automatic category propagation prefers exact normalized street address and falls back to a 10-meter GPS radius.
- Screenshots, received images, and other no-GPS/no-address records may legitimately remain unclassified.
- The user cares strongly about local date/time analytics, not only UTC ordering.
- MySQL table and column names must remain PascalCase.
- Project-scoped MySQL environment variables should be preferred, particularly `IMAGETRACKER_MYSQL_DATABASE` or `MYSQL_DATABASE_IMAGETRACKER`.
- Secret values must never be printed or committed.

The handoff records a database snapshot of 2,099 local rows, including 1,098 rows with GPS and 556 rows missing descriptions, with no imported capture dates after May 10, 2026 as of the handoff. Treat those numbers as an observed historical snapshot, not a permanent invariant. Verify current state only with read-only queries and only when explicitly authorized.

## Safety rules for this onboarding task

For this first review:

- Do not edit any file.
- Do not run the importer.
- Do not use `--force`.
- Do not execute `tag_location.py`.
- Do not write to MySQL.
- Do not call OpenAI or Google APIs.
- Do not inspect or print secret values.
- Do not revive OneDrive support.
- Do not restructure the repository.
- Do not propose an `imagetracker/` package or internal `scripts/` directory as if that were already approved.

You may run these non-destructive commands:

```bash
git status --short --branch
git log --oneline -n 15
pytest -q
```

If tests cannot run because dependencies or infrastructure are unavailable, report that precisely rather than guessing.

## User preferences that are architectural constraints

Preserve these unless Nektarios explicitly changes them:

- Work directly on `main`; never create a branch.
- Keep the codebase simple.
- Keep the active importer as the single root script `ImageTracker.py`.
- A separate root utility such as `tag_location.py` is acceptable.
- Use `python`, not `python3`.
- Preserve original file names verbatim.
- Preserve PascalCase MySQL schema names.
- Prefer project-scoped environment variables.
- Use `gpt-5.2` when OpenAI vision/captioning is used.
- GPS is a primary reason for the project.
- Missing GPS is expected for screenshots and some shared/received images.
- Manual categories are authoritative and must not be overwritten.
- Local date and time are first-class analytics fields.

For any later implementation task, first verify that GitHub writing is available. If it is not, stop before making changes. After successful work, verify briefly, commit directly to `main`, push, and provide a downloadable copy of any generated artifact.

## Required onboarding report

After reading and reviewing everything, respond with a structured report containing the following sections.

### 1. Executive understanding

Explain in plain language what ImageTracker is today, why it exists, and how it fits into the future AI Intelligence Layer.

### 2. Current end-to-end data flow

Trace one local photo from:

```text
filesystem scan
→ eligibility check
→ stable identity lookup
→ file-byte read
→ EXIF metadata extraction
→ time-zone resolution
→ MySQL upsert
→ reverse geocoding and normalization
→ category propagation
→ optional OpenAI captioning
→ queryable ImageAsset record
```

Name the actual classes, functions, tables, and fields involved.

### 3. Codebase map

Describe the responsibility of each tracked file, including:

- `ImageTracker.py`
- `tag_location.py`
- `README.md`
- `.env.example`
- `location_normalization_rules.json`
- each migration;
- each test file;
- `pyproject.toml`.

For `ImageTracker.py`, identify the major components, including settings, database/migrations, repository layer, EXIF extractors, Google resolvers, OpenAI captioner, normalization, category inference, local scanning, sync orchestration, and CLI entry point.

### 4. Database model and field semantics

Explain:

- the purpose of `ImageAsset`;
- local identity and the `(Source, DriveItemId)` unique key;
- local-time versus UTC fields;
- GPS/location fields;
- description/model provenance;
- category and category-source provenance;
- `OriginalStreetNumber`;
- the legacy meaning of `RawGraphJson`;
- the remaining OneDrive tables and why they are inactive.

Identify any schema or naming decisions that may matter when ImageTracker becomes a reusable Intellige Layer source.

### 5. Verified behavior and tests

Summarize what the tests actually prove. Include the current test result if you ran it. Do not imply coverage for encryption, safety classification, production databases, external APIs, or full-scale imports unless such coverage exists.

### 6. Current operational state

Summarize the handoff’s database snapshot, stale-import observation, OpenAI quota history, robocopy separation, location normalization state, and known manual tagging behavior. Clearly label these as handoff observations rather than newly verified facts unless you performed authorized read-only verification.

### 7. Discrepancies and stale information

List every meaningful mismatch among:

- `HANDOFF.md`;
- current Git history;
- README;
- implementation;
- migrations;
- tests.

Pay special attention to current HEAD, the location of the handoff file, legacy OneDrive remnants, and any statements that describe intended rather than implemented behavior.

### 8. Risks and technical debt

Evaluate at least:

- path-derived identity and duplicate risk after file moves;
- modified-time cutoff versus EXIF capture time;
- skipped records that need selective backfill;
- single-threaded scanning on a very large archive;
- lack of persistent Google-result caching;
- OpenAI quota/rate failure behavior;
- plaintext sensitive metadata in MySQL;
- absence of a prohibited-content gate;
- provenance granularity;
- future source-adapter boundaries;
- privacy implications of GPS, addresses, and descriptions.

Do not “fix” these yet.

### 9. Intelligence Layer integration view

Explain how ImageTracker could eventually become one source within:

```text
Sources → Intelligence Layer → Agents → Actions
```

Describe the smallest stable source contract ImageTracker might eventually expose—for example source item identity, timestamps, content description, location, provenance, permissions, and original-artifact reference—without redesigning the current code prematurely.

Distinguish:

- what already exists;
- what can be adapted;
- what is missing;
- what requires a product or privacy decision from Nektarios.

### 10. Recommended next steps

Provide a prioritized list divided into:

- **Immediate operational recovery**
- **Near-term reliability improvements**
- **Intelligence Layer readiness**
- **Future privacy, safety, and encryption architecture**

For every recommendation, identify the expected benefit, risk, and whether user approval is required.

### 11. Questions for Nektarios

Ask only questions that materially affect architecture or priority. Do not ask questions that the handoff or code already answers.

## Response requirements

- Be specific and evidence-based.
- Cite exact files, classes, functions, migrations, tests, and fields.
- Separate confirmed facts from inference and future design ideas.
- Do not write code during this onboarding response.
- Do not begin implementation automatically.
- End by waiting for Nektarios’s next instruction.
