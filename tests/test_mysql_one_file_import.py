from __future__ import annotations

import csv
from pathlib import Path

import infra.scripts.mysql_one_file_import as importer


class FakeCursor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []
        self.rowcount = 0
        self._row: dict[str, object] | None = None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql: str, params=None):
        normalized = " ".join(sql.split())
        self.calls.append((normalized, params))
        self.rowcount = 0
        self._row = None
        if normalized.startswith("SELECT Id, UserId, DeviceId, PublicId"):
            self._row = {
                "Id": 22,
                "UserId": 11,
                "DeviceId": 33,
                "PublicId": "00000000-0000-0000-0000-000000000022",
            }
        elif normalized.startswith("SELECT COUNT(*) AS RowCount"):
            self._row = {"RowCount": 2}
        elif normalized.startswith("INSERT INTO MediaOccurrence"):
            self.rowcount = 2
        elif normalized.startswith("INSERT INTO MediaChange") and "SELECT" in normalized:
            self.rowcount = 2
        else:
            self.rowcount = 1

    def fetchone(self):
        return self._row


class FakeConnection:
    def __init__(self) -> None:
        self.cursor_value = FakeCursor()
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def cursor(self):
        return self.cursor_value

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


def test_manifest_csv_is_one_round_trippable_file(tmp_path: Path):
    path = tmp_path / "manifest.csv"
    entries = (
        {
            "sourceItemId": "path:one",
            "sourceRevision": "revision-one",
            "fileName": 'one, "quoted".jpg',
            "localLocator": r"C:\Photos\one, quoted.jpg",
            "byteSize": 123,
        },
        {
            "sourceItemId": "path:two",
            "sourceRevision": "revision-two",
            "fileName": "two.jpg",
            "localLocator": "/mnt/d/Pictures/two.jpg",
            "byteSize": 456,
        },
    )

    count = importer.write_manifest_csv(path, entries)

    assert count == 2
    assert list(tmp_path.glob("*.csv")) == [path]
    with path.open(encoding="utf-8", newline="") as handle:
        rows = list(
            csv.reader(
                handle,
                escapechar="\\",
                doublequote=False,
            )
        )
    assert tuple(rows[0]) == importer.CSV_COLUMNS
    assert rows[1][4] == 'one, "quoted".jpg'
    assert rows[1][5] == r"C:\Photos\one, quoted.jpg"
    assert rows[2][2:] == [
        "path:two",
        "revision-two",
        "two.jpg",
        "/mnt/d/Pictures/two.jpg",
        "456",
    ]


def test_one_file_loader_uses_one_load_and_one_commit(tmp_path: Path, monkeypatch):
    path = tmp_path / "manifest.csv"
    path.write_text("header\n", encoding="utf-8")
    connection = FakeConnection()
    captured: dict[str, object] = {}

    monkeypatch.setattr(importer, "_secret", lambda *_args: {"database": "ImageTracker"})

    def connect(_secret, *, local_infile=False, timeout_seconds=30):
        captured["local_infile"] = local_infile
        captured["timeout_seconds"] = timeout_seconds
        return connection

    monkeypatch.setattr(importer, "_connect", connect)

    result = importer.load_one_file(
        path=path,
        source_public_id="00000000-0000-0000-0000-000000000022",
        region="us-east-2",
        db_parameter="/imagetracker/prod/mysql",
    )

    load_calls = [
        call
        for call in connection.cursor_value.calls
        if call[0].startswith("LOAD DATA LOCAL INFILE")
    ]
    assert captured["local_infile"] is True
    assert captured["timeout_seconds"] == 600
    assert len(load_calls) == 1
    assert result == {
        "loaded": 2,
        "inserted": 2,
        "alreadyPresent": 0,
        "changeRows": 2,
    }
    assert connection.commits == 1
    assert connection.rollbacks == 0
    assert connection.closed is True


def test_admin_env_file_is_scoped_to_imagetracker_without_echoing_values(
    tmp_path: Path,
):
    path = tmp_path / ".env.prod"
    path.write_text(
        "\n".join(
            (
                "MYSQL_HOST=db.example.test",
                "MYSQL_PORT=3306",
                "MYSQL_DATABASE=AnotherDatabase",
                "MYSQL_USERNAME=admin-user",
                "MYSQL_PASSWORD=private-value",
            )
        )
        + "\n",
        encoding="utf-8",
    )

    secret = importer.admin_secret_from_env_file(path)

    assert secret == {
        "host": "db.example.test",
        "port": "3306",
        "user": "admin-user",
        "password": "private-value",
        "database": "ImageTracker",
        "tls": True,
    }
