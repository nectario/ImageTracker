from __future__ import annotations

import argparse
from dataclasses import replace
from uuid import UUID

import pytest

from cli.imagetracker_cli.bulk import write_manifest_gzip
from infra.scripts import live_bulk_mysql_canary as canary
from services.bulk.manifest import ManifestGuardrails, parse_manifest_gzip


RUN_ID = UUID("00000000-0000-0000-0000-00000000ca11")
ACCOUNT_ID = 17
ACCOUNT_PUBLIC_ID = "00000000-0000-0000-0000-00000000ac17"


class FakeCursor:
    def __init__(
        self,
        *,
        migration_present: bool = True,
        active_accounts: int = 1,
    ) -> None:
        self.migration_present = migration_present
        self.active_accounts = active_accounts
        self.calls: list[tuple[str, tuple[object, ...]]] = []
        self._rows: list[dict[str, object]] = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql: str, params=()):
        statement = " ".join(sql.split())
        self.calls.append((statement, tuple(params)))
        self._rows = []
        if statement.startswith("SELECT DATABASE()"):
            self._rows = [{"Value": "ImageTracker"}]
        elif statement.startswith("SELECT @@local_infile"):
            self._rows = [{"Value": 1}]
        elif statement.startswith("SELECT Id, PublicId FROM UserAccount"):
            self._rows = [
                {"Id": ACCOUNT_ID + index, "PublicId": ACCOUNT_PUBLIC_ID}
                for index in range(self.active_accounts)
            ]
        elif "FROM SchemaMigration" in statement:
            self._rows = [{"Value": int(self.migration_present)}]
        elif "FROM information_schema.TABLES" in statement:
            self._rows = [
                {"TABLE_NAME": table}
                for table in sorted(canary.EXPECTED_SCHEMA_TABLES)
            ]
        elif statement.startswith("SELECT COUNT(*)"):
            self._rows = [{"Value": 0}]

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)


class FakeConnection:
    def __init__(self, cursor: FakeCursor) -> None:
        self.cursor_value = cursor
        self.rollbacks = 0
        self.closed = 0

    def cursor(self):
        return self.cursor_value

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed += 1


def _args(*, apply: bool = False) -> argparse.Namespace:
    return argparse.Namespace(
        apply=apply,
        region="us-east-2",
        profile=None,
        parameter="/imagetracker/prod/mysql",
    )


def test_parser_is_dry_run_by_default_and_uses_app_parameter():
    args = canary._parser().parse_args([])

    assert args.apply is False
    assert args.parameter.endswith("/mysql")


def test_dry_run_is_read_only_and_reports_exact_preflight(monkeypatch):
    target = canary.CanaryTarget.build(RUN_ID)
    cursor = FakeCursor()
    connection = FakeConnection(cursor)
    monkeypatch.setattr(canary, "_require_wsl", lambda: None)

    report = canary.run(
        _args(),
        connection_factory=lambda: connection,
        target=target,
    )

    assert report["mode"] == "dry-run"
    assert report["wouldApply"] is True
    assert report["target"] == {
        "kind": "synthetic-local-source",
        "uuidPrefixed": True,
        "rows": 4,
        "gps": False,
        "extension": ".nef",
    }
    assert connection.rollbacks >= 1
    assert connection.closed == 1
    statements = [sql.upper() for sql, _ in cursor.calls]
    assert not any(
        statement.startswith(("INSERT ", "UPDATE ", "DELETE ", "REPLACE "))
        for statement in statements
    )


def test_apply_preflight_refuses_missing_migration_without_dml(monkeypatch):
    target = canary.CanaryTarget.build(RUN_ID)
    cursor = FakeCursor(migration_present=False)
    connection = FakeConnection(cursor)
    monkeypatch.setattr(canary, "_require_wsl", lambda: None)

    with pytest.raises(canary.CanaryError, match="Migration 014"):
        canary.run(
            _args(apply=True),
            connection_factory=lambda: connection,
            target=target,
        )

    assert not any(
        sql.upper().startswith(("INSERT ", "UPDATE ", "DELETE "))
        for sql, _ in cursor.calls
    )


def test_preflight_requires_exactly_one_active_account(monkeypatch):
    target = canary.CanaryTarget.build(RUN_ID)
    connection = FakeConnection(FakeCursor(active_accounts=2))
    monkeypatch.setattr(canary, "_require_wsl", lambda: None)

    with pytest.raises(canary.CanaryError, match="exactly one active"):
        canary.run(
            _args(),
            connection_factory=lambda: connection,
            target=target,
        )


def test_target_validation_rejects_my_photos_and_non_uuid_prefix():
    target = canary.CanaryTarget.build(RUN_ID)

    with pytest.raises(canary.CanaryError, match="My Photos"):
        canary._validate_target(replace(target, source_name="My Photos"))
    with pytest.raises(canary.CanaryError, match="UUID-prefixed"):
        canary._validate_target(replace(target, source_key="unsafe-source"))


def test_manifest_is_four_no_gps_nef_rows_with_one_rejection_candidate():
    target = canary.CanaryTarget.build(RUN_ID)
    entries = canary._manifest_entries(target)

    assert len(entries) == 4
    assert all(entry["fileName"].endswith(".nef") for entry in entries)
    assert all("location" not in entry for entry in entries)
    assert entries[0]["contentSha256"] == entries[1]["contentSha256"]
    assert len({entry["contentSha256"] for entry in entries}) == 3
    assert entries[3]["byteSize"] == 0


def test_generated_artifact_canonicalizes_three_valid_rows_and_one_rejection(
    tmp_path,
):
    target = canary.CanaryTarget.build(RUN_ID)
    artifact = write_manifest_gzip(
        tmp_path / "canary.ndjson.gz",
        source_id=target.source_public_id,
        snapshot_id=target.snapshot_id,
        entries=canary._manifest_entries(target),
    )

    parsed = parse_manifest_gzip(
        artifact.path,
        tmp_path / "canary.csv",
        expected_sha256=artifact.compressed_sha256,
        expected_compressed_bytes=artifact.compressed_bytes,
        expected_entry_count=4,
        guardrails=ManifestGuardrails(max_entries=4),
    )

    assert parsed.entry_count == 4
    assert parsed.rejected_count == 1


@pytest.mark.parametrize(
    "parameter",
    ["MYSQL_DSN", "/deeptrading/prod/mysql", "/imagetracker/prod/admin"],
)
def test_parameter_validation_accepts_only_app_mysql_path(parameter: str):
    with pytest.raises(canary.CanaryError, match="app credential"):
        canary._validate_parameter_name(parameter)
