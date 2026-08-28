"""Apply only the additive ImageTracker enrichment migrations 012 and 013.

The runner is deliberately narrow and crash-reconcilable. If MySQL commits an
ALTER before the SchemaMigration marker is written, a rerun verifies the target
shape and records the missing ledger row instead of repeating the ALTER.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
from typing import Any

import boto3
import pymysql
from pymysql.cursors import DictCursor

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ImageTracker import _split_sql_statements  # noqa: E402


MIGRATIONS = {
    "012": ROOT / "migrations" / "012_AddProviderCircuit.sql",
    "013": ROOT / "migrations" / "013_WidenLocationProviderFields.sql",
}
REQUIRED_BASE = {"007", "008", "009", "010", "011"}
CA_PATH = ROOT / "services" / "data" / "certs" / "us-east-2-bundle.pem"


def _secret(region: str, parameter_name: str) -> dict[str, Any]:
    response = boto3.client("ssm", region_name=region).get_parameter(
        Name=parameter_name,
        WithDecryption=True,
    )
    try:
        value = json.loads(response["Parameter"]["Value"])
    except (KeyError, TypeError, json.JSONDecodeError) as exc:
        raise RuntimeError("The ImageTracker database parameter is invalid") from exc
    if not isinstance(value, dict) or value.get("database") != "ImageTracker":
        raise RuntimeError("The database credential must be scoped to ImageTracker")
    return value


def _admin_environment() -> dict[str, Any] | None:
    values = {
        "host": os.environ.get("MYSQL_HOST"),
        "port": os.environ.get("MYSQL_PORT", "3306"),
        "user": os.environ.get("MYSQL_USERNAME")
        or os.environ.get("MYSQL_USERID"),
        "password": os.environ.get("MYSQL_PASSWORD"),
    }
    supplied = [bool(value) for value in values.values()]
    if not any(supplied):
        return None
    if not all(supplied):
        raise RuntimeError("The MYSQL_* administrative environment is incomplete")
    return {**values, "database": "ImageTracker", "tls": True}


def _connect(secret: dict[str, Any]) -> pymysql.Connection:
    user = secret.get("user") or secret.get("username")
    password = secret.get("password")
    host = secret.get("host")
    if not all(isinstance(value, str) and value for value in (user, password, host)):
        raise RuntimeError("The ImageTracker database parameter is incomplete")
    options: dict[str, Any] = {}
    if bool(secret.get("tls", True)):
        options["ssl"] = {"ca": str(CA_PATH), "check_hostname": True}
    return pymysql.connect(
        host=host,
        port=int(secret.get("port", 3306)),
        user=user,
        password=password,
        database="ImageTracker",
        charset="utf8mb4",
        autocommit=False,
        connect_timeout=10,
        read_timeout=30,
        write_timeout=30,
        cursorclass=DictCursor,
        **options,
    )


def _versions(connection: pymysql.Connection) -> set[str]:
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT `Version` FROM `SchemaMigration` ORDER BY `Version`"
        )
        return {str(row["Version"]) for row in cursor.fetchall()}


def _columns(connection: pymysql.Connection, table: str) -> dict[str, int | None]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT COLUMN_NAME, CHARACTER_MAXIMUM_LENGTH
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = 'ImageTracker' AND TABLE_NAME = %s
            """,
            (table,),
        )
        return {
            str(row["COLUMN_NAME"]): (
                int(row["CHARACTER_MAXIMUM_LENGTH"])
                if row["CHARACTER_MAXIMUM_LENGTH"] is not None
                else None
            )
            for row in cursor.fetchall()
        }


def _satisfied(connection: pymysql.Connection, version: str) -> bool:
    if version == "012":
        columns = _columns(connection, "ProviderUsageMonth")
        return {
            "CircuitState",
            "CircuitOpenedAtUtc",
            "CircuitFailureCode",
        }.issubset(columns)
    if version == "013":
        columns = _columns(connection, "MediaLocation")
        return (
            (columns.get("ProviderPlaceId") or 0) >= 500
            and (columns.get("PostalCode") or 0) >= 50
        )
    raise ValueError(f"Unsupported migration version: {version}")


def _assert_idle(connection: pymysql.Connection) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
              (SELECT COUNT(*) FROM `UploadSession`
               WHERE `Status` IN ('Preparing','Uploading','Completing')) AS ActiveUploads,
              (SELECT COUNT(*) FROM `ProcessingJob`
               WHERE `Status` IN ('Queued','Running')) AS ActiveJobs
            """
        )
        row = cursor.fetchone()
    if int(row["ActiveUploads"] or 0) or int(row["ActiveJobs"] or 0):
        raise RuntimeError(
            "ImageTracker processing must be idle before schema migration"
        )


def _record(
    connection: pymysql.Connection, version: str, migration_path: Path
) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO `SchemaMigration` (`Version`, `Name`, `AppliedAtUtc`)
            VALUES (%s, %s, %s)
            """,
            (
                version,
                migration_path.name,
                datetime.now(timezone.utc).replace(tzinfo=None),
            ),
        )
    connection.commit()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument(
        "--region",
        default=os.environ.get("IMAGETRACKER_AWS_REGION", "us-east-2"),
    )
    parser.add_argument(
        "--parameter",
        default=os.environ.get(
            "IMAGETRACKER_DB_SECRET_PARAMETER", "/imagetracker/prod/mysql"
        ),
    )
    args = parser.parse_args()

    connection = _connect(
        _admin_environment() or _secret(args.region, args.parameter)
    )
    actions: list[dict[str, str]] = []
    try:
        versions = _versions(connection)
        if not REQUIRED_BASE.issubset(versions):
            missing = sorted(REQUIRED_BASE - versions)
            raise RuntimeError(
                f"Required base migrations are missing: {', '.join(missing)}"
            )
        _assert_idle(connection)
        for version, path in MIGRATIONS.items():
            if version in versions:
                actions.append({"version": version, "action": "AlreadyApplied"})
                continue
            satisfied = _satisfied(connection, version)
            action = "ReconcileLedger" if satisfied else "ApplyAlter"
            actions.append({"version": version, "action": action})
            if not args.apply:
                continue
            if not satisfied:
                statements = _split_sql_statements(path.read_text(encoding="utf-8"))
                if len(statements) != 1:
                    raise RuntimeError(
                        f"Migration {version} must contain exactly one atomic ALTER"
                    )
                with connection.cursor() as cursor:
                    cursor.execute(statements[0])
                if not _satisfied(connection, version):
                    raise RuntimeError(
                        f"Migration {version} did not produce its required schema"
                    )
            _record(connection, version, path)
            versions.add(version)
    finally:
        connection.close()

    print(
        json.dumps(
            {
                "database": "ImageTracker",
                "mode": "apply" if args.apply else "dry-run",
                "actions": actions,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
