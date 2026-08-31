from __future__ import annotations

from pathlib import Path
import re
from typing import Any

import infra.scripts.migrate_enrichment as runner


ROOT = Path(__file__).resolve().parents[1]
MIGRATION = ROOT / "migrations" / "014_CreateManifestImportTables.sql"


def test_runner_registers_and_validates_additive_migration_014() -> None:
    assert runner.MIGRATIONS["014"] == MIGRATION
    statements = runner._migration_statements("014", MIGRATION)
    assert len(statements) == 5
    assert all(
        statement.startswith("CREATE TABLE IF NOT EXISTS")
        for statement in statements[:4]
    )
    assert statements[4].startswith("ALTER TABLE `MediaOccurrence`")


def test_014_shape_reconciliation_requires_every_table_and_index(monkeypatch) -> None:
    sql = MIGRATION.read_text(encoding="utf-8")
    columns = {
        table: set(
            re.findall(
                r"^\s+`([^`]+)`\s+",
                sql.split(f"CREATE TABLE IF NOT EXISTS `{table}`", 1)[1].split(
                    ") ENGINE=InnoDB", 1
                )[0],
                flags=re.MULTILINE,
            )
        )
        for table in (
            "ManifestImport",
            "ManifestImportEntry",
            "ManifestImportAssetWork",
            "ManifestImportFailure",
        )
    }
    indexes = {
        "ManifestImport": {
            "Ux_ManifestImport_PublicId", "Ux_ManifestImport_User_Idempotency",
            "Ux_ManifestImport_User_Source_Snapshot",
            "Ux_ManifestImport_User_Source_Active",
            "Ix_ManifestImport_Status_NextAttempt",
        },
        "ManifestImportEntry": {
            "Ux_ManifestImportEntry_Import_Row",
            "Ix_ManifestImportEntry_Import_SourceItem",
            "Ix_ManifestImportEntry_Import_Hash",
        },
        "ManifestImportAssetWork": {
            "PRIMARY", "Ix_ManifestImportAssetWork_Import_ResolvedAsset",
        },
        "ManifestImportFailure": {
            "Ux_ManifestImportFailure_Import_Row",
            "Ix_ManifestImportFailure_User_Import",
        },
        "MediaOccurrence": {"Ix_MediaOccurrence_User_Asset_DeletionState"},
    }
    monkeypatch.setattr(
        runner,
        "_columns",
        lambda _connection, table: {name: None for name in columns.get(table, set())},
    )
    monkeypatch.setattr(
        runner, "_indexes", lambda _connection, table: set(indexes.get(table, set()))
    )
    constraints = {
        "ManifestImport": {"Fk_ManifestImport_MediaSource"},
        "ManifestImportEntry": {"Fk_ManifestImportEntry_ManifestImport"},
        "ManifestImportAssetWork": {
            "Fk_ManifestImportAssetWork_CanonicalEntry"
        },
        "ManifestImportFailure": {"Fk_ManifestImportFailure_ManifestImport"},
    }
    monkeypatch.setattr(
        runner,
        "_constraints",
        lambda _connection, table: set(constraints.get(table, set())),
    )

    assert runner._satisfied(object(), "014")
    indexes["MediaOccurrence"].clear()
    assert not runner._satisfied(object(), "014")
    indexes["MediaOccurrence"].add("Ix_MediaOccurrence_User_Asset_DeletionState")
    constraints["ManifestImportEntry"].clear()
    assert not runner._satisfied(object(), "014")


class _RecordingCursor:
    def __init__(self, statements: list[str]) -> None:
        self._statements = statements

    def __enter__(self) -> "_RecordingCursor":
        return self

    def __exit__(self, *_args: Any) -> None:
        return None

    def execute(self, statement: str, _parameters: Any = None) -> None:
        self._statements.append(statement)


class _RecordingConnection:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def cursor(self) -> _RecordingCursor:
        return _RecordingCursor(self.statements)


def test_014_apply_replays_create_tables_but_skips_an_existing_index(monkeypatch) -> None:
    connection = _RecordingConnection()
    monkeypatch.setattr(
        runner,
        "_indexes",
        lambda _connection, _table: {
            "Ix_MediaOccurrence_User_Asset_DeletionState"
        },
    )

    runner._apply_migration(connection, "014", MIGRATION)  # type: ignore[arg-type]

    assert len(connection.statements) == 4
    assert all(
        statement.startswith("CREATE TABLE IF NOT EXISTS")
        for statement in connection.statements
    )


def test_014_apply_adds_only_the_missing_online_index(monkeypatch) -> None:
    connection = _RecordingConnection()
    monkeypatch.setattr(runner, "_indexes", lambda _connection, _table: set())

    runner._apply_migration(connection, "014", MIGRATION)  # type: ignore[arg-type]

    assert len(connection.statements) == 5
    assert connection.statements[-1].startswith("ALTER TABLE `MediaOccurrence`")
    assert "LOCK=NONE" in connection.statements[-1]


def test_only_pending_legacy_alters_require_an_idle_database(monkeypatch) -> None:
    monkeypatch.setattr(runner, "_satisfied", lambda _connection, _version: False)

    assert not runner._requires_idle(object(), {"012", "013"})
    assert runner._requires_idle(object(), {"012"})
    assert runner._requires_idle(object(), {"013"})

    # 014 is deliberately absent in every case: its replay-safe table creates
    # and LOCK=NONE index do not make queued enrichment work a blocker.
    assert not runner._requires_idle(object(), {"012", "013", "011"})
