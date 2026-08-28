#!/usr/bin/env bash

set -Eeuo pipefail
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/_common.sh"

cd_repository
export IMAGETRACKER_ENV_FILE="${IMAGETRACKER_ENV_FILE:-${REPOSITORY_ROOT}/.env}"

[[ -n "${OPENAI_API_KEY:-}" ]] || fail \
    "OPENAI_API_KEY is empty in this shell. Export it, then rerun this command."

python -B - <<'PY'
from __future__ import annotations

import os
from pathlib import Path
import stat


target = Path(os.environ["IMAGETRACKER_ENV_FILE"]).expanduser().resolve(strict=False)
value = os.environ.get("OPENAI_API_KEY", "")
if not value:
    raise SystemExit("OPENAI_API_KEY is empty")
if target.exists() and target.is_symlink():
    raise SystemExit("Refusing to update a symlinked environment file")

lines = target.read_text(encoding="utf-8").splitlines() if target.is_file() else []
replacement = f"OPENAI_API_KEY={value}"
updated: list[str] = []
replaced = False
for line in lines:
    if line.strip().startswith("OPENAI_API_KEY="):
        if not replaced:
            updated.append(replacement)
            replaced = True
        continue
    updated.append(line)
if not replaced:
    if updated and updated[-1] != "":
        updated.append("")
    updated.append(replacement)

target.parent.mkdir(parents=True, exist_ok=True)
temporary = target.with_suffix(target.suffix + ".tmp")
temporary.write_text("\n".join(updated) + "\n", encoding="utf-8")
temporary.chmod(stat.S_IRUSR | stat.S_IWUSR)
temporary.replace(target)
target.chmod(stat.S_IRUSR | stat.S_IWUSR)
print(f"Saved OPENAI_API_KEY to {target} without displaying it.")
PY
