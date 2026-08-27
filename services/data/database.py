from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, replace
from functools import lru_cache
import json
from pathlib import Path
from threading import RLock
from typing import Any, Iterator, Mapping

import boto3
from sqlalchemy import Engine, create_engine
from sqlalchemy.engine import URL, make_url
from sqlalchemy.orm import Session, sessionmaker

from services.common.settings import AppSettings


class DatabaseConfigurationError(ValueError):
    """Raised when a database secret could escape the ImageTracker schema."""


class SsmParameterResolver:
    """Small process-local cache for decrypted SSM parameters.

    Lambda execution environments are commonly reused. Caching here avoids an
    SSM network call on every request while retaining dependency injection for
    deterministic tests. Secret values are never logged or included in errors.
    """

    def __init__(self, *, region_name: str, client: Any | None = None) -> None:
        self._client = client or boto3.client("ssm", region_name=region_name)
        self._values: dict[str, str] = {}
        self._lock = RLock()

    def resolve(self, parameter_name: str) -> str:
        with self._lock:
            cached = self._values.get(parameter_name)
            if cached is not None:
                return cached

        response = self._client.get_parameter(
            Name=parameter_name,
            WithDecryption=True,
        )
        try:
            value = response["Parameter"]["Value"]
        except (KeyError, TypeError) as exc:
            raise DatabaseConfigurationError(
                "The configured database parameter has no value"
            ) from exc
        if not isinstance(value, str) or not value.strip():
            raise DatabaseConfigurationError(
                "The configured database parameter is empty"
            )

        with self._lock:
            self._values[parameter_name] = value
        return value

    def clear(self) -> None:
        with self._lock:
            self._values.clear()


@lru_cache(maxsize=4)
def default_ssm_resolver(region_name: str) -> SsmParameterResolver:
    return SsmParameterResolver(region_name=region_name)


def _secret_mapping(raw_secret: str) -> Mapping[str, Any] | None:
    try:
        parsed = json.loads(raw_secret)
    except json.JSONDecodeError:
        return None
    if not isinstance(parsed, dict):
        raise DatabaseConfigurationError(
            "The database parameter must be a DSN string or a JSON object"
        )
    return parsed


@dataclass(frozen=True)
class DatabaseConnectionConfig:
    url: URL
    tls_enabled: bool
    ssl_ca: str | None = None


def _as_bool(value: Any, *, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "on", "required"}:
        return True
    if normalized in {"0", "false", "no", "off", "disabled"}:
        return False
    raise DatabaseConfigurationError("The database TLS setting is invalid")


def database_config_from_secret(
    raw_secret: str, *, required_database: str
) -> DatabaseConnectionConfig:
    """Parse a raw DSN or common JSON secret shapes into a restricted config."""

    mapping = _secret_mapping(raw_secret)
    tls_enabled = True
    ssl_ca: str | None = None
    if mapping is None:
        candidate: str | URL = raw_secret.strip()
    else:
        tls_enabled = _as_bool(
            mapping.get("tls", mapping.get("ssl", mapping.get("require_tls"))),
            default=True,
        )
        ca_value = mapping.get("ssl_ca") or mapping.get("sslCa")
        if ca_value is not None:
            if not isinstance(ca_value, str) or not ca_value:
                raise DatabaseConfigurationError("The database TLS CA path is invalid")
            ssl_ca = ca_value
        candidate = ""
        for key in ("dsn", "url", "mysql_dsn", "MYSQL_DSN"):
            value = mapping.get(key)
            if isinstance(value, str) and value.strip():
                candidate = value.strip()
                break
        if not candidate:
            host = mapping.get("host") or mapping.get("hostname")
            username = (
                mapping.get("username")
                or mapping.get("user")
                or mapping.get("userid")
            )
            password = mapping.get("password")
            database = (
                mapping.get("database")
                or mapping.get("dbname")
                or mapping.get("schema")
            )
            if not all(
                isinstance(value, str) and value
                for value in (host, username, password, database)
            ):
                raise DatabaseConfigurationError(
                    "The database parameter is missing connection fields"
                )
            try:
                port = int(mapping.get("port", 3306))
            except (TypeError, ValueError) as exc:
                raise DatabaseConfigurationError(
                    "The database parameter contains an invalid port"
                ) from exc
            if not 1 <= port <= 65535:
                raise DatabaseConfigurationError(
                    "The database parameter contains an invalid port"
                )
            candidate = URL.create(
                drivername="mysql+pymysql",
                username=username,
                password=password,
                host=host,
                port=port,
                database=database,
                query={"charset": "utf8mb4"},
            )

    try:
        url = candidate if isinstance(candidate, URL) else make_url(candidate)
    except Exception as exc:
        raise DatabaseConfigurationError(
            "The database parameter is not a valid MySQL connection string"
        ) from exc

    query = dict(url.query)
    for flag in ("tls", "ssl", "require_tls"):
        if flag in query:
            tls_enabled = _as_bool(query.pop(flag), default=tls_enabled)
    query_ca = query.pop("ssl_ca", None)
    if query_ca:
        ssl_ca = str(query_ca)
    url = url.set(query=query)

    if url.get_backend_name() != "mysql":
        raise DatabaseConfigurationError("Only MySQL database URLs are supported")
    if url.database != required_database or required_database != "ImageTracker":
        raise DatabaseConfigurationError(
            "The app service may connect only to the ImageTracker database"
        )
    if url.drivername == "mysql":
        url = url.set(drivername="mysql+pymysql")
    elif url.drivername != "mysql+pymysql":
        raise DatabaseConfigurationError("The MySQL driver must be PyMySQL")
    if not url.host or not url.username:
        raise DatabaseConfigurationError(
            "The database connection string must include a host and user"
        )
    if url.query.get("charset") != "utf8mb4":
        url = url.update_query_dict({**dict(url.query), "charset": "utf8mb4"})
    return DatabaseConnectionConfig(
        url=url,
        tls_enabled=tls_enabled,
        ssl_ca=ssl_ca,
    )


def database_url_from_secret(raw_secret: str, *, required_database: str) -> URL:
    """Compatibility helper for callers that need only the validated URL."""

    return database_config_from_secret(
        raw_secret,
        required_database=required_database,
    ).url


def create_mysql_engine(config: DatabaseConnectionConfig | URL) -> Engine:
    """Create the deliberately tiny Lambda-safe SQLAlchemy pool."""

    if isinstance(config, URL):
        config = DatabaseConnectionConfig(url=config, tls_enabled=True)
    url = config.url
    if url.get_backend_name() != "mysql" or url.database != "ImageTracker":
        raise DatabaseConfigurationError(
            "The app service may connect only to the ImageTracker MySQL database"
        )
    connect_args: dict[str, Any] = {
        "connect_timeout": 5,
        "read_timeout": 20,
        "write_timeout": 20,
        "charset": "utf8mb4",
    }
    if config.tls_enabled:
        connect_args.update(
            {
                "ssl_verify_cert": True,
                "ssl_verify_identity": True,
            }
        )
        if config.ssl_ca:
            connect_args["ssl_ca"] = config.ssl_ca
    return create_engine(
        url,
        pool_size=1,
        max_overflow=0,
        pool_pre_ping=True,
        pool_recycle=300,
        pool_timeout=5,
        connect_args=connect_args,
    )


@dataclass(frozen=True)
class DatabaseRuntime:
    engine: Engine
    session_factory: sessionmaker[Session]


def build_database_runtime(
    settings: AppSettings,
    *,
    resolver: SsmParameterResolver | None = None,
) -> DatabaseRuntime:
    selected_resolver = resolver or default_ssm_resolver(settings.aws_region)
    raw_secret = selected_resolver.resolve(settings.db_secret_parameter)
    config = database_config_from_secret(
        raw_secret,
        required_database=settings.mysql_database,
    )
    if config.tls_enabled and config.ssl_ca is None:
        bundled_ca = (
            Path(__file__).resolve().parent
            / "certs"
            / f"{settings.aws_region}-bundle.pem"
        )
        if not bundled_ca.is_file():
            raise DatabaseConfigurationError(
                f"No bundled Amazon RDS CA is available for {settings.aws_region}"
            )
        config = replace(config, ssl_ca=str(bundled_ca))
    engine = create_mysql_engine(config)
    factory = sessionmaker(
        bind=engine,
        class_=Session,
        autoflush=False,
        expire_on_commit=False,
    )
    return DatabaseRuntime(engine=engine, session_factory=factory)


@contextmanager
def transaction_scope(
    session_factory: sessionmaker[Session],
) -> Iterator[Session]:
    """Commit one unit of work or roll it back completely on failure."""

    session = session_factory()
    try:
        with session.begin():
            yield session
    except BaseException:
        session.rollback()
        raise
    finally:
        session.close()
