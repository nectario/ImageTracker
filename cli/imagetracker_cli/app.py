from __future__ import annotations

import json
import platform
from importlib.metadata import PackageNotFoundError, version
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from services.common.settings import get_settings


app = typer.Typer(
    name="imagetracker",
    help="Index, upload, inspect, and repair an ImageTracker media library.",
    no_args_is_help=True,
)
console = Console()


def package_version() -> str:
    try:
        return version("imagetracker")
    except PackageNotFoundError:
        return "0.2.0"


@app.command("version")
def version_command() -> None:
    """Print the CLI version."""

    console.print(package_version())


@app.command()
def doctor(
    json_output: Annotated[
        bool,
        typer.Option("--json", help="Emit machine-readable diagnostics."),
    ] = False,
) -> None:
    """Check non-secret local configuration without contacting cloud services."""

    settings = get_settings()
    checks = {
        "python": platform.python_version(),
        "stage": settings.stage,
        "aws_region": settings.aws_region,
        "database_scope": settings.mysql_database,
        "api_configured": bool(settings.api_url),
        "media_bucket_configured": bool(settings.media_bucket),
    }

    if json_output:
        typer.echo(json.dumps(checks, sort_keys=True))
        return

    table = Table(title="ImageTracker doctor", show_header=False)
    table.add_column("Check", style="bold")
    table.add_column("Value")
    for key, value in checks.items():
        table.add_row(key.replace("_", " ").title(), str(value))
    console.print(table)


def main() -> None:
    app()


if __name__ == "__main__":
    main()
