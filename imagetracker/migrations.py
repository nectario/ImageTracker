from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import List

from pymysql.connections import Connection


def _split_sql_statements(script: str) -> List[str]:
    statements: List[str] = []
    current: List[str] = []
    in_single = False
    in_double = False

    for char in script:
        if char == "'" and not in_double:
            in_single = not in_single
        elif char == '"' and not in_single:
            in_double = not in_double

        if char == ";" and not in_single and not in_double:
            statement = "".join(current).strip()
            if statement:
                statements.append(statement)
            current = []
            continue

        current.append(char)

    tail = "".join(current).strip()
    if tail:
        statements.append(tail)

    return statements


class MigrationRunner:
    def __init__(self, migrations_dir: Path):
        self._migrations_dir = migrations_dir

    def apply_all(self, conn: Connection) -> None:
        self._ensure_schema_migration_table(conn)

        with conn.cursor() as cursor:
            cursor.execute("SELECT `Version` FROM `SchemaMigration`")
            existing_versions = {row["Version"] for row in cursor.fetchall()}

        migration_files = sorted(self._migrations_dir.glob("*.sql"))
        for migration_file in migration_files:
            version = migration_file.stem.split("_", 1)[0]
            if version in existing_versions:
                continue

            sql = migration_file.read_text(encoding="utf-8")
            statements = _split_sql_statements(sql)
            with conn.cursor() as cursor:
                for statement in statements:
                    cursor.execute(statement)
                cursor.execute(
                    """
                    INSERT INTO `SchemaMigration` (`Version`, `Name`, `AppliedAtUtc`)
                    VALUES (%s, %s, %s)
                    """,
                    (version, migration_file.name, datetime.utcnow()),
                )

    def _ensure_schema_migration_table(self, conn: Connection) -> None:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS `SchemaMigration` (
                    `Version` VARCHAR(32) NOT NULL PRIMARY KEY,
                    `Name` VARCHAR(255) NOT NULL,
                    `AppliedAtUtc` DATETIME NOT NULL
                )
                """
            )
