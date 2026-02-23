from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, Optional
from urllib.parse import parse_qs, urlparse

import pymysql
from pymysql.connections import Connection

from imagetracker.config import Settings


@dataclass(frozen=True)
class MysqlConnectionConfig:
    host: str
    port: int
    user: str
    password: str
    database: str
    charset: str = "utf8mb4"


class DbError(RuntimeError):
    pass


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def parse_mysql_config(settings: Settings) -> MysqlConnectionConfig:
    if settings.mysql_dsn:
        parsed = urlparse(settings.mysql_dsn)
        if parsed.scheme.lower() not in {"mysql", "mysql+pymysql"}:
            raise DbError("MYSQL_DSN must use mysql:// or mysql+pymysql://")

        query_params = parse_qs(parsed.query)
        charset = query_params.get("charset", ["utf8mb4"])[0]

        if not parsed.hostname or not parsed.username or not parsed.path:
            raise DbError("MYSQL_DSN is missing host, username, or database")

        return MysqlConnectionConfig(
            host=parsed.hostname,
            port=parsed.port or 3306,
            user=parsed.username,
            password=parsed.password or "",
            database=parsed.path.lstrip("/"),
            charset=charset,
        )

    host = _first_env(("IMAGETRACKER_MYSQL_HOST", "MYSQL_HOST"), "127.0.0.1")
    port = int(_first_env(("IMAGETRACKER_MYSQL_PORT", "MYSQL_PORT"), "3306"))
    user = _first_env(("IMAGETRACKER_MYSQL_USER", "MYSQL_USERID", "MYSQL_USER"), "")
    password = _first_env(("IMAGETRACKER_MYSQL_PASSWORD", "MYSQL_PASSWORD"), "")
    database = _first_env(
        ("IMAGETRACKER_MYSQL_DATABASE", "MYSQL_DATABASE_IMAGETRACKER", "MYSQL_DATABASE"),
        "",
    )

    if not user or not database:
        raise DbError(
            "Set MYSQL_DSN or IMAGETRACKER_MYSQL_* "
            "(fallbacks: MYSQL_HOST/MYSQL_PORT/MYSQL_USERID-or-MYSQL_USER/MYSQL_PASSWORD/"
            "MYSQL_DATABASE_IMAGETRACKER-or-MYSQL_DATABASE)"
        )

    return MysqlConnectionConfig(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
    )


def _first_env(names: tuple[str, ...], default: str) -> str:
    import os

    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return default


class Database:
    def __init__(self, config: MysqlConnectionConfig):
        self._config = config

    def connect(self) -> Connection:
        return pymysql.connect(
            host=self._config.host,
            port=self._config.port,
            user=self._config.user,
            password=self._config.password,
            database=self._config.database,
            charset=self._config.charset,
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=False,
        )

    @contextmanager
    def connection(self) -> Iterator[Connection]:
        conn = self.connect()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def fetch_one(self, sql: str, params: Optional[tuple[Any, ...]] = None) -> Optional[Dict[str, Any]]:
        with self.connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params or ())
                row = cursor.fetchone()
                return row

    def execute(self, sql: str, params: Optional[tuple[Any, ...]] = None) -> int:
        with self.connection() as conn:
            with conn.cursor() as cursor:
                return cursor.execute(sql, params or ())
