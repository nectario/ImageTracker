#!/usr/bin/env bash

set -Eeuo pipefail
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/_common.sh"

require_environment
cd_repository

step "Running aggregate-only read-only database checks"
"${IMAGETRACKER_PYTHON}" -B - <<'PY'
import json
import os

import boto3
import pymysql


region = os.environ["IMAGETRACKER_AWS_REGION"]
parameter_name = os.environ["IMAGETRACKER_DB_SECRET_PARAMETER"]
secret = json.loads(
    boto3.client("ssm", region_name=region)
    .get_parameter(Name=parameter_name, WithDecryption=True)["Parameter"]["Value"]
)
if secret.get("database") != "ImageTracker":
    raise SystemExit("The configured credential is not scoped to ImageTracker")

connection = pymysql.connect(
    host=secret["host"],
    port=int(secret["port"]),
    user=secret["user"],
    password=secret["password"],
    database=secret["database"],
    charset=secret.get("charset", "utf8mb4"),
    autocommit=False,
    connect_timeout=10,
    read_timeout=30,
    write_timeout=30,
)

try:
    with connection.cursor() as cursor:
        cursor.execute("SET SESSION TRANSACTION READ ONLY")
        cursor.execute("START TRANSACTION READ ONLY")
        cursor.execute("SELECT COUNT(*) FROM `ImageAsset`")
        legacy_rows = cursor.fetchone()[0]
        cursor.execute(
            "SELECT `Version` FROM `SchemaMigration` "
            "WHERE `Version` >= '007' ORDER BY `Version`"
        )
        migration_versions = [row[0] for row in cursor.fetchall()]
        counts = {}
        for table in ("UserAccount", "MediaAsset", "MediaOccurrence", "ProcessingJob"):
            cursor.execute(f"SELECT COUNT(*) FROM `{table}`")
            counts[table] = cursor.fetchone()[0]
    connection.rollback()
finally:
    connection.close()

print(
    json.dumps(
        {
            "legacyImageAssets": legacy_rows,
            "mediaRows": counts,
            "migrationVersions": migration_versions,
        },
        indent=2,
        sort_keys=True,
    )
)
PY
