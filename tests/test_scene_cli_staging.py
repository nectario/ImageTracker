from __future__ import annotations

import hashlib
import json
import os
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Mapping

import httpx
from PIL import Image
from typer.testing import CliRunner

import cli.imagetracker_cli.app as cli_app_module
from cli.imagetracker_cli.api_client import ApiClient, ApiError, ApiProblem
from cli.imagetracker_cli.app import app
from cli.imagetracker_cli.media import stream_sha256
from cli.imagetracker_cli.state import LocalState, SourceBinding
from cli.imagetracker_cli.sync import SyncEngine, SyncSummary


SOURCE_ID = "b4bc0eb8-b696-4af6-8af9-6dcc180e53a4"
OCCURRENCE_ID = "046535bb-8734-457b-b38e-7632d320f67f"
ASSET_ID = "7d5e4dbf-b48a-4277-b81f-c54096dab7c8"
JOB_ID = "0bf78575-4619-4725-9349-294886511f74"


def _binding(root: Path) -> SourceBinding:
    return SourceBinding(SOURCE_ID, "source-key", str(root), "Photos", "Local")


def _queue_photo(
    state: LocalState,
    photo: Path,
    *,
    media_type: str = "Photo",
    job_id: str = JOB_ID,
    occurrence_id: str = OCCURRENCE_ID,
    asset_id: str = ASSET_ID,
    source_item_id: str = "photo:item",
    source_id: str = SOURCE_ID,
) -> None:
    content_sha = stream_sha256(photo)
    entry = {
        "operation": "Upsert",
        "sourceItemId": source_item_id,
        "sourceRevision": hashlib.sha256(b"revision").hexdigest(),
        "fileName": photo.name,
        "localLocator": str(photo),
        "mediaType": media_type,
        "mimeType": "image/jpeg",
        "byteSize": photo.stat().st_size,
        "contentSha256": content_sha,
    }
    scan_id = state.begin_scan(source_id, photo.parent)
    state.queue_batches(
        source_id,
        scan_id,
        [
            {
                "kind": "Full",
                "permissionState": "NotApplicable",
                "deletionDetectionReliable": True,
                "entries": [entry],
            }
        ],
    )
    batch = state.pending_batches(source_id)[0]
    state.acknowledge_batch(
        batch,
        {
            "counts": {"rejected": 0},
            "results": [
                {
                    "sourceItemId": source_item_id,
                    "outcome": "CreatedOccurrence",
                    "occurrenceId": occurrence_id,
                    "mediaAssetId": asset_id,
                    "descriptionJobId": job_id,
                    "uploadRequired": False,
                }
            ],
        },
    )


def _photo(path: Path) -> None:
    Image.new("RGB", (1600, 900), (18, 80, 140)).save(path, "JPEG", quality=91)


class UploadApi:
    def __init__(self, disposition: str = "UploadRequired"):
        self.disposition = disposition
        self.plan_payload: Mapping[str, Any] | None = None
        self.plan_key: str | None = None
        self.put_content: bytes | None = None
        self.put_headers: Mapping[str, str] | None = None
        self.completion: Mapping[str, Any] | None = None
        self.plan_calls = 0
        self.cancelled_jobs: list[tuple[str, str, str]] = []

    def create_upload_plan(self, payload: Mapping[str, Any], *, key: str):
        self.plan_calls += 1
        self.plan_payload = payload
        self.plan_key = key
        base = {
            "disposition": self.disposition,
            "strategy": "SinglePart" if self.disposition != "Deferred" else "None",
            "mediaAssetId": ASSET_ID,
            "occurrenceId": OCCURRENCE_ID,
            "deduplicated": False,
        }
        if self.disposition == "Deferred":
            return {**base, "retryAfterSeconds": 600}
        return {
            **base,
            "uploadSessionId": "cda0e6a5-e939-4a88-87d3-2fc33950be70",
            "singlePart": {
                "url": "https://storage.example/private-signed-url",
                "method": "PUT",
                "headers": {
                    "Content-Type": "image/jpeg",
                    "Content-Length": str(payload["objectByteSize"]),
                    "x-amz-checksum-sha256": "checksum",
                },
                "expiresAtUtc": "2099-08-28T22:00:00Z",
            },
        }

    def put_signed_upload(self, _url: str, content: bytes, *, headers: Mapping[str, str]):
        self.put_content = content
        self.put_headers = dict(headers)
        return '"etag"'

    def complete_upload(
        self, _upload_session_id: str, payload: Mapping[str, Any], *, key: str
    ):
        self.completion = payload
        assert key.startswith(f"scene-complete:{JOB_ID}:")
        return {"mediaAssetId": ASSET_ID, "storageState": "LocalOnly", "processingJobs": [JOB_ID]}

    def cancel_job(self, job_id: str, *, reason: str, key: str):
        self.cancelled_jobs.append((job_id, reason, key))
        return {"jobId": job_id, "jobType": "Description", "status": "Cancelled"}


def test_manifest_ack_atomically_queues_account_scoped_photo_and_source_removal_clears_it(
    tmp_path: Path,
):
    root = tmp_path / "library"
    root.mkdir()
    photo = root / "one.jpg"
    _photo(photo)
    state = LocalState(tmp_path / "state.sqlite3")
    state.bind_source(
        {
            "sourceId": SOURCE_ID,
            "sourceKey": "source-key",
            "displayName": "Photos",
            "storageMode": "Local",
        },
        root,
    )
    _queue_photo(state, photo)

    task = state.list_description_outbox(state="Pending")[0]
    assert task.job_id == JOB_ID
    assert task.occurrence_id == OCCURRENCE_ID
    assert task.media_asset_id == ASSET_ID
    assert task.local_path == str(photo)
    assert task.asset_content_sha256 == stream_sha256(photo)

    state.remove_binding(SOURCE_ID)
    assert state.list_description_outbox(state="All") == []


def test_video_manifest_result_never_queues_scene_preview(tmp_path: Path):
    video = tmp_path / "clip.jpg"
    _photo(video)
    state = LocalState(tmp_path / "state.sqlite3")
    _queue_photo(state, video, media_type="Video")
    assert state.list_description_outbox(state="All") == []


def test_duplicate_asset_keeps_an_alternate_local_staging_candidate(
    tmp_path: Path,
):
    first = tmp_path / "first.jpg"
    second = tmp_path / "second.jpg"
    _photo(first)
    second.write_bytes(first.read_bytes())
    state = LocalState(tmp_path / "state.sqlite3")
    _queue_photo(state, first)
    alternate_occurrence = "08b58456-34ba-4e20-82c7-4f143707fa09"
    _queue_photo(
        state,
        second,
        occurrence_id=alternate_occurrence,
        source_item_id="photo:alternate",
    )

    original = state.list_description_outbox(state="Pending")[0]
    assert original.occurrence_id == OCCURRENCE_ID
    scan_id = state.begin_scan(SOURCE_ID, tmp_path)
    state.queue_batches(
        SOURCE_ID,
        scan_id,
        [
            {
                "kind": "Incremental",
                "permissionState": "NotApplicable",
                "deletionDetectionReliable": True,
                "entries": [
                    {
                        "operation": "Deleted",
                        "sourceItemId": "photo:item",
                        "sourceRevision": "deleted-revision",
                    }
                ],
            }
        ],
    )
    deletion_batch = state.pending_batches(SOURCE_ID)[0]
    state.acknowledge_batch(
        deletion_batch,
        {
            "counts": {"deleted": 1, "rejected": 0},
            "results": [
                {
                    "sourceItemId": "photo:item",
                    "outcome": "DeletedOccurrence",
                    "occurrenceId": OCCURRENCE_ID,
                    "mediaAssetId": ASSET_ID,
                    "uploadRequired": False,
                }
            ],
        },
    )

    fallback = state.list_description_outbox(state="Pending")[0]
    assert fallback.job_id == JOB_ID
    assert fallback.occurrence_id == alternate_occurrence
    assert fallback.local_path == str(second)


def test_new_asset_revision_retires_obsolete_failed_description_task(
    tmp_path: Path,
):
    photo = tmp_path / "changing.jpg"
    _photo(photo)
    state = LocalState(tmp_path / "state.sqlite3")
    _queue_photo(state, photo)
    state.quarantine_description(
        JOB_ID,
        code="source_photo_changed",
        message="Old accepted bytes are no longer present.",
    )
    Image.new("RGB", (1600, 900), (180, 25, 45)).save(
        photo, "JPEG", quality=91
    )
    replacement_job = "d53db3f4-b500-482d-8b96-d34e3999dd27"
    replacement_occurrence = "b1373285-81c4-40f5-b165-b353d507436e"
    replacement_asset = "c504dd8f-c2b0-4f85-a8bf-9dce87122ff8"
    _queue_photo(
        state,
        photo,
        job_id=replacement_job,
        occurrence_id=replacement_occurrence,
        asset_id=replacement_asset,
    )

    tasks = state.list_description_outbox(state="All")
    assert len(tasks) == 1
    assert tasks[0].job_id == replacement_job
    assert tasks[0].state == "Pending"


def test_scene_staging_uploads_only_deterministic_preview_with_exact_headers(tmp_path: Path):
    photo = tmp_path / "large.jpg"
    _photo(photo)
    state = LocalState(tmp_path / "state.sqlite3")
    _queue_photo(state, photo)
    api = UploadApi()
    engine = SyncEngine(api, state)  # type: ignore[arg-type]

    outcome = engine._stage_description(state.due_description_tasks(SOURCE_ID)[0])

    assert outcome == "Sent"
    assert api.plan_payload is not None
    assert api.plan_payload["purpose"] == "TemporaryProcessing"
    assert api.plan_payload["processingJobId"] == JOB_ID
    assert api.plan_payload["assetContentSha256"] == stream_sha256(photo)
    assert api.put_content is not None
    assert len(api.put_content) < photo.stat().st_size
    with Image.open(Path(photo)) as original, Image.open(BytesIO(api.put_content)) as preview:
        assert max(original.size) == 1600
        assert max(preview.size) == 1024
        assert preview.format == "JPEG"
    assert api.put_headers == {
        "Content-Type": "image/jpeg",
        "Content-Length": str(len(api.put_content)),
        "x-amz-checksum-sha256": "checksum",
    }
    assert api.completion == {
        "objectSha256": hashlib.sha256(api.put_content).hexdigest(),
        "parts": [],
    }
    assert state.description_counts()["Sent"] == 1


def test_signed_preview_put_uses_plan_headers_without_cognito_authorization():
    seen: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["headers"] = dict(request.headers)
        seen["body"] = request.content
        return httpx.Response(200, headers={"ETag": '"preview"'})

    api = ApiClient(
        "https://api.example",
        SimpleNamespace(),  # type: ignore[arg-type]
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
    )
    etag = api.put_signed_upload(
        "https://storage.example/signed",
        b"preview-only",
        headers={
            "Content-Type": "image/jpeg",
            "Content-Length": "12",
            "x-amz-checksum-sha256": "checksum",
        },
    )

    assert etag == '"preview"'
    assert seen["body"] == b"preview-only"
    assert "authorization" not in seen["headers"]
    assert seen["headers"]["content-type"] == "image/jpeg"
    assert seen["headers"]["content-length"] == "12"
    assert seen["headers"]["x-amz-checksum-sha256"] == "checksum"


def test_changed_original_is_quarantined_before_preview_upload(tmp_path: Path):
    photo = tmp_path / "one.jpg"
    _photo(photo)
    state = LocalState(tmp_path / "state.sqlite3")
    _queue_photo(state, photo)
    Image.new("RGB", (800, 600), (200, 20, 20)).save(photo, "JPEG", quality=70)
    api = UploadApi()

    outcome = SyncEngine(api, state)._stage_description(  # type: ignore[arg-type]
        state.due_description_tasks(SOURCE_ID)[0]
    )

    assert outcome == "Skipped"
    assert api.plan_calls == 0
    task = state.list_description_outbox(state="Skipped")[0]
    assert task.error["code"] == "source_photo_changed"
    assert str(photo) not in json.dumps(task.error)
    assert api.cancelled_jobs[0][1] == "SourceChanged"


def test_same_size_same_mtime_replacement_is_rehashed_before_ai_staging(
    tmp_path: Path,
):
    photo = tmp_path / "preserved-metadata.jpg"
    _photo(photo)
    state = LocalState(tmp_path / "state.sqlite3")
    _queue_photo(state, photo)
    original_stat = photo.stat()
    changed = bytearray(photo.read_bytes())
    changed[-10] ^= 0x01
    photo.write_bytes(changed)
    os.utime(
        photo,
        ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns),
    )
    assert photo.stat().st_size == original_stat.st_size
    assert photo.stat().st_mtime_ns == original_stat.st_mtime_ns
    api = UploadApi()

    outcome = SyncEngine(api, state)._stage_description(  # type: ignore[arg-type]
        state.due_description_tasks(SOURCE_ID)[0]
    )

    assert outcome == "Skipped"
    assert api.plan_calls == 0
    assert state.list_description_outbox(state="Skipped")[0].error["code"] == (
        "source_photo_changed"
    )


class JobStatusApi(UploadApi):
    def __init__(self, status: str):
        super().__init__()
        self.status = status
        self.job_calls = 0

    def get_job(self, job_id: str):
        assert job_id == JOB_ID
        self.job_calls += 1
        return {"jobId": job_id, "status": self.status}


def test_sent_preview_reconciles_server_restage_and_completion(tmp_path: Path):
    photo = tmp_path / "one.jpg"
    _photo(photo)
    state = LocalState(tmp_path / "state.sqlite3")
    _queue_photo(state, photo)
    state.mark_description_sent(JOB_ID)
    with state._connect() as connection:
        connection.execute(
            "UPDATE DescriptionOutbox SET NextAttemptAtUtc = ? WHERE JobId = ?",
            ("2000-01-01T00:00:00Z", JOB_ID),
        )

    preparing = JobStatusApi("Preparing")
    SyncEngine(preparing, state)._reconcile_sent_descriptions(  # type: ignore[arg-type]
        _binding(tmp_path)
    )
    assert preparing.job_calls == 1
    assert state.list_description_outbox(state="Pending")[0].job_id == JOB_ID

    state.mark_description_sent(JOB_ID)
    with state._connect() as connection:
        connection.execute(
            "UPDATE DescriptionOutbox SET NextAttemptAtUtc = ? WHERE JobId = ?",
            ("2000-01-01T00:00:00Z", JOB_ID),
        )
    succeeded = JobStatusApi("Succeeded")
    SyncEngine(succeeded, state)._reconcile_sent_descriptions(  # type: ignore[arg-type]
        _binding(tmp_path)
    )
    assert succeeded.job_calls == 1
    sent = state.list_description_outbox(state="Sent")[0]
    assert sent.next_attempt_at_utc is None
    assert state.due_sent_description_tasks(SOURCE_ID) == []


def test_quota_deferral_persists_next_attempt_without_upload(tmp_path: Path):
    photo = tmp_path / "one.jpg"
    second = tmp_path / "two.jpg"
    _photo(photo)
    _photo(second)
    state = LocalState(tmp_path / "state.sqlite3")
    _queue_photo(state, photo)
    _queue_photo(
        state,
        second,
        job_id="e7b66774-4ef2-4f8e-812f-49cc81079d48",
        occurrence_id="11d50787-8074-4489-b1fa-606c92ed8df8",
        asset_id="4bc06d38-a7d8-4421-9c2c-13f68543a02b",
        source_item_id="photo:second",
        source_id="e71737c2-9c87-4bbc-a1d8-d4c301889f8a",
    )
    api = UploadApi("Deferred")
    summary = SyncSummary(SOURCE_ID, str(tmp_path), False)
    SyncEngine(api, state)._flush_description_outbox(_binding(tmp_path), summary)  # type: ignore[arg-type]

    tasks = state.list_description_outbox(state="Deferred")
    assert len(tasks) == 2
    assert len({task.next_attempt_at_utc for task in tasks}) == 1
    assert all(
        task.error
        == {
            "code": "MONTHLY_DESCRIPTION_QUOTA",
            "message": "Scene description is waiting for monthly quota.",
            "retryable": True,
        }
        for task in tasks
    )
    assert api.plan_calls == 1
    assert api.put_content is None
    assert state.due_description_tasks(SOURCE_ID) == []
    assert state.due_description_tasks("e71737c2-9c87-4bbc-a1d8-d4c301889f8a") == []
    counts = state.description_counts()
    summary = SyncSummary(SOURCE_ID, str(tmp_path), False)
    SyncEngine(api, state)._add_description_attention_counts(_binding(tmp_path), summary)  # type: ignore[arg-type]
    assert counts["Deferred"] == 2
    assert summary.description_deferred == 1
    assert summary.failed == 0


class FailingPlanApi:
    def __init__(self, status: int):
        self.status = status

    def create_upload_plan(self, *_args: Any, **_kwargs: Any):
        raise ApiError(ApiProblem(self.status, "failed", "private detail", "FAILED"))


class LeaseRecoveryApi(UploadApi):
    stale_session_id = "dd8fda4e-7976-42ea-83d9-78ee19c4d9ee"

    def __init__(self, session_status: str, *, object_present: bool = True):
        super().__init__()
        self.session_status = session_status
        self.object_present = object_present
        self.cancelled: list[str] = []
        self.recovered_completion_calls = 0

    def create_upload_plan(self, payload: Mapping[str, Any], *, key: str):
        if self.plan_calls == 0:
            self.plan_calls += 1
            self.plan_payload = payload
            return {
                "disposition": "LeaseHeld",
                "strategy": "SinglePart",
                "mediaAssetId": ASSET_ID,
                "occurrenceId": OCCURRENCE_ID,
                "uploadSessionId": self.stale_session_id,
                "deduplicated": False,
                "retryAfterSeconds": 60,
            }
        return super().create_upload_plan(payload, key=key)

    def get_upload_session(self, upload_session_id: str):
        assert upload_session_id == self.stale_session_id
        return {"uploadSessionId": upload_session_id, "status": self.session_status}

    def complete_upload(
        self, upload_session_id: str, payload: Mapping[str, Any], *, key: str
    ):
        if upload_session_id == self.stale_session_id:
            self.recovered_completion_calls += 1
            if not self.object_present:
                raise ApiError(
                    ApiProblem(
                        409,
                        "Object missing",
                        "The temporary preview has not been uploaded.",
                        "UploadedObjectNotFound",
                    )
                )
            return {
                "mediaAssetId": ASSET_ID,
                "storageState": "LocalOnly",
                "processingJobs": [JOB_ID],
            }
        return super().complete_upload(upload_session_id, payload, key=key)

    def cancel_upload(self, upload_session_id: str, *, reason: str, key: str):
        assert reason and key.startswith(f"scene-cancel:{JOB_ID}:")
        self.cancelled.append(upload_session_id)


def test_lost_put_response_completes_existing_uploaded_object_without_second_put(tmp_path: Path):
    photo = tmp_path / "one.jpg"
    _photo(photo)
    state = LocalState(tmp_path / "state.sqlite3")
    _queue_photo(state, photo)
    api = LeaseRecoveryApi("Uploading", object_present=True)

    outcome = SyncEngine(api, state)._stage_description(  # type: ignore[arg-type]
        state.due_description_tasks(SOURCE_ID)[0]
    )

    assert outcome == "Sent"
    assert api.recovered_completion_calls == 1
    assert api.put_content is None
    assert api.cancelled == []


def test_stale_lease_without_object_is_cancelled_and_replanned(tmp_path: Path):
    photo = tmp_path / "one.jpg"
    _photo(photo)
    state = LocalState(tmp_path / "state.sqlite3")
    _queue_photo(state, photo)
    api = LeaseRecoveryApi("Uploading", object_present=False)

    outcome = SyncEngine(api, state)._stage_description(  # type: ignore[arg-type]
        state.due_description_tasks(SOURCE_ID)[0]
    )

    assert outcome == "Sent"
    assert api.cancelled == [api.stale_session_id]
    assert api.plan_calls == 2
    assert api.put_content is not None
    assert state.description_counts()["Sent"] == 1


def test_lost_complete_response_recovers_from_completed_session(tmp_path: Path):
    photo = tmp_path / "one.jpg"
    _photo(photo)
    state = LocalState(tmp_path / "state.sqlite3")
    _queue_photo(state, photo)
    api = LeaseRecoveryApi("Completed")

    outcome = SyncEngine(api, state)._stage_description(  # type: ignore[arg-type]
        state.due_description_tasks(SOURCE_ID)[0]
    )

    assert outcome == "Sent"
    assert api.recovered_completion_calls == 0
    assert api.put_content is None
    assert state.description_counts()["Sent"] == 1


def test_transient_staging_failure_stays_pending_and_permanent_is_quarantined(tmp_path: Path):
    for status, expected_state in ((503, "Pending"), (422, "Failed")):
        folder = tmp_path / str(status)
        folder.mkdir()
        photo = folder / "private-name.jpg"
        _photo(photo)
        state = LocalState(folder / "state.sqlite3")
        _queue_photo(state, photo)
        summary = SyncSummary(SOURCE_ID, str(folder), False)
        SyncEngine(FailingPlanApi(status), state)._flush_description_outbox(  # type: ignore[arg-type]
            _binding(folder), summary
        )
        task = state.list_description_outbox(state=expected_state)[0]
        assert str(photo) not in json.dumps(task.error)
        assert task.error["retryable"] is (status == 503)
        if status == 503:
            assert summary.failed == 1


def test_status_and_description_outbox_never_render_local_paths(tmp_path: Path, monkeypatch):
    photo = tmp_path / "secret" / "one.jpg"
    photo.parent.mkdir()
    _photo(photo)
    state = LocalState(tmp_path / "state.sqlite3")
    _queue_photo(state, photo)
    runtime = SimpleNamespace(
        state=state,
        api=SimpleNamespace(list_jobs=lambda limit: []),
    )
    monkeypatch.setattr(cli_app_module, "_runtime", lambda: runtime)

    status = CliRunner().invoke(app, ["status", "--json"])
    listed = CliRunner().invoke(app, ["outbox", "descriptions", "--state", "All", "--json"])
    assert status.exit_code == listed.exit_code == 0
    assert json.loads(status.stdout)["pendingDescriptionPreviews"] == 1
    assert str(photo.parent) not in listed.stdout
    assert json.loads(listed.stdout)[0]["fileName"] == "one.jpg"
