from __future__ import annotations

import json
import os
import platform
import socket
import time
import hashlib
from contextlib import contextmanager
from enum import IntEnum
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Annotated, Any, Iterator, Mapping

import typer
from botocore.exceptions import BotoCoreError, ClientError
from rich.console import Console
from rich.table import Table

from .api_client import ApiError, AuthenticationRequired
from .config import DEFAULT_STACK_NAME, ConfigStore, config_from_stack
from .runtime import Runtime, boto3_session, build_runtime, cloudformation_client
from .sync import SyncEngine, SyncSummary


class ExitCode(IntEnum):
    SUCCESS = 0
    CONFIGURATION = 2
    AUTHENTICATION = 3
    NETWORK = 4
    PARTIAL_SYNC = 5
    SERVICE = 6


app = typer.Typer(
    name="imagetracker",
    help="Index and synchronize a Local ImageTracker media library.",
    no_args_is_help=True,
    rich_markup_mode="rich",
)
auth_app = typer.Typer(help="Create and manage your ImageTracker session.", no_args_is_help=True)
source_app = typer.Typer(help="Manage Local folder sources.", no_args_is_help=True)
legacy_app = typer.Typer(
    help="Inspect legacy ImageAsset evidence and preview migration.",
    no_args_is_help=True,
)
outbox_app = typer.Typer(help="Inspect manifest batches that need attention.", no_args_is_help=True)
media_app = typer.Typer(help="Browse account-visible Local media metadata.", no_args_is_help=True)
jobs_app = typer.Typer(help="Inspect and retry media processing jobs.", no_args_is_help=True)
app.add_typer(auth_app, name="auth")
app.add_typer(source_app, name="source")
app.add_typer(legacy_app, name="legacy")
app.add_typer(outbox_app, name="outbox")
app.add_typer(media_app, name="media")
app.add_typer(jobs_app, name="jobs")

console = Console()
error_console = Console(stderr=True)


def package_version() -> str:
    try:
        return version("imagetracker")
    except PackageNotFoundError:
        return "0.3.0"


def _emit(payload: Any) -> None:
    typer.echo(json.dumps(payload, sort_keys=True, default=str))


def _error(message: str, code: ExitCode) -> None:
    error_console.print(f"[red]Error:[/red] {message}")
    raise typer.Exit(int(code))


@contextmanager
def command_errors() -> Iterator[None]:
    try:
        yield
    except typer.Exit:
        raise
    except AuthenticationRequired as exc:
        _error(str(exc), ExitCode.AUTHENTICATION)
    except ApiError as exc:
        code = ExitCode.NETWORK if exc.problem.status == 0 else ExitCode.SERVICE
        _error(str(exc), code)
    except ClientError as exc:
        error = exc.response.get("Error") or {}
        message = str(error.get("Message") or error.get("Code") or "AWS request failed")
        auth_codes = {
            "NotAuthorizedException",
            "UserNotConfirmedException",
            "CodeMismatchException",
            "ExpiredCodeException",
        }
        code = ExitCode.AUTHENTICATION if error.get("Code") in auth_codes else ExitCode.SERVICE
        _error(message, code)
    except BotoCoreError as exc:
        _error(str(exc), ExitCode.NETWORK)
    except (ValueError, OSError) as exc:
        _error(str(exc), ExitCode.CONFIGURATION)
    except KeyboardInterrupt:
        _error("Interrupted. Saved work will resume on the next sync.", ExitCode.PARTIAL_SYNC)


def _runtime() -> Runtime:
    return build_runtime()


def _platform_name() -> str:
    return "WindowsCLI" if platform.system() == "Windows" else "LinuxCLI"


def _device_payload(runtime: Runtime) -> dict[str, Any]:
    return {
        "installationId": runtime.state.installation_id(),
        "platform": _platform_name(),
        "displayName": socket.gethostname() or "ImageTracker CLI",
        "appVersion": package_version(),
        "osVersion": platform.platform()[:64],
    }


def _register_device(runtime: Runtime) -> Mapping[str, Any]:
    payload = _device_payload(runtime)
    canonical = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    payload_hash = hashlib.sha256(canonical).hexdigest()[:32]
    existing_id = runtime.state.get_setting("device-id")
    existing_hash = runtime.state.get_setting("device-payload-hash")
    if existing_id and existing_hash == payload_hash:
        return {"deviceId": existing_id}
    key = f"device:{payload['installationId']}:{payload_hash}"
    device = runtime.api.register_device(payload, key=key)
    runtime.state.set_setting("device-id", str(device["deviceId"]))
    runtime.state.set_setting("device-payload-hash", payload_hash)
    return device


def _registered_device_id(runtime: Runtime) -> str:
    return str(_register_device(runtime)["deviceId"])


def _legacy_runtime():
    from .legacy import LegacyInspector, load_legacy_db_config, mysql_connection_factory
    from .state import LocalState

    store = ConfigStore()
    config = store.load()
    parameter_client = None
    if os.environ.get("IMAGETRACKER_DB_SECRET_PARAMETER"):
        parameter_client = boto3_session(
            region=config.aws_region,
            profile=config.aws_profile,
        ).client("ssm")
    db_config = load_legacy_db_config(parameter_client=parameter_client)
    return (
        LegacyInspector(mysql_connection_factory(db_config)),
        LocalState(store.legacy_preview_state_path),
    )


@app.command("version")
def version_command() -> None:
    """Print the CLI version."""

    console.print(package_version())


@app.command()
def configure(
    stack: Annotated[str, typer.Option("--stack", help="CloudFormation stack name.")] = DEFAULT_STACK_NAME,
    region: Annotated[str, typer.Option("--region", help="AWS region containing the stack.")] = "us-east-2",
    profile: Annotated[str | None, typer.Option("--profile", help="Optional AWS profile name.")] = None,
    json_output: Annotated[bool, typer.Option("--json", help="Emit machine-readable output.")] = False,
) -> None:
    """Discover the deployed app from CloudFormation and save local settings."""

    with command_errors():
        store = ConfigStore()
        client = cloudformation_client(region=region, profile=profile)
        config = config_from_stack(client, stack_name=stack, region=region, profile=profile)
        store.save(config)
        payload = {
            "configured": True,
            "stack": stack,
            "region": region,
            "apiUrl": config.api_url,
            "configPath": str(store.path),
        }
        if json_output:
            _emit(payload)
        else:
            console.print(f"[green]Configured[/green] {stack} in {region}")
            console.print(f"API: {config.api_url}")


@app.command()
def doctor(
    json_output: Annotated[
        bool,
        typer.Option("--json", help="Emit machine-readable diagnostics."),
    ] = False,
) -> None:
    """Check non-secret local configuration without contacting cloud services."""

    store = ConfigStore()
    with command_errors():
        config = store.load()
        from services.common.settings import get_settings

        service_settings = get_settings()
        token_store_name = "not-initialized"
        signed_in = False
        state_ready = False
        if config.is_configured:
            from .auth import TokenStore
            from .state import LocalState

            token_store = TokenStore(store.fallback_token_path)
            token_store_name = token_store.backend_name
            tokens = token_store.load()
            signed_in = tokens is not None
            subject = tokens.local_subject if tokens else None
            if subject:
                LocalState(store.state_path_for_subject(subject))
                state_ready = True
        checks = {
            "python": platform.python_version(),
            "stage": service_settings.stage,
            "aws_region": config.aws_region,
            "database_scope": "ImageTracker",
            "api_configured": bool(config.api_url),
            "media_bucket_configured": bool(service_settings.media_bucket),
            "cognito_configured": bool(config.cognito_client_id and config.cognito_user_pool_id),
            "signed_in": signed_in,
            "token_storage": token_store_name,
            "local_state_ready": state_ready,
        }
        if json_output:
            _emit(checks)
            return
        table = Table(title="ImageTracker doctor", show_header=False)
        table.add_column("Check", style="bold")
        table.add_column("Value")
        for key, value in checks.items():
            table.add_row(key.replace("_", " ").title(), str(value))
        console.print(table)


@auth_app.command("signup")
def auth_signup(
    email: str,
    password: Annotated[
        str | None,
        typer.Option("--password", help="Password; omit to enter it privately.", hide_input=True),
    ] = None,
) -> None:
    """Create an ImageTracker account and send one verification code."""

    with command_errors():
        runtime = _runtime()
        actual_password = password or typer.prompt("Password", hide_input=True, confirmation_prompt=True)
        response = runtime.auth.signup(email, actual_password)
        destination = ((response.get("CodeDeliveryDetails") or {}).get("Destination") or email)
        console.print(f"[green]Account created.[/green] Verification code sent to {destination}.")
        console.print(f"Confirm with: imagetracker auth confirm {email} CODE")


@auth_app.command("confirm")
def auth_confirm(email: str, code: str) -> None:
    """Confirm the one-time email verification code."""

    with command_errors():
        runtime = _runtime()
        runtime.auth.confirm(email, code)
        console.print("[green]Email confirmed.[/green] You can now sign in.")


@auth_app.command("login")
def auth_login(
    email: str,
    password: Annotated[
        str | None,
        typer.Option("--password", help="Password; omit to enter it privately.", hide_input=True),
    ] = None,
) -> None:
    """Sign in and keep the session in the OS vault or a private WSL file."""

    with command_errors():
        runtime = _runtime()
        actual_password = password or typer.prompt("Password", hide_input=True)
        tokens = runtime.auth.login(email, actual_password)
        console.print(f"[green]Signed in[/green] as {tokens.email or email}.")
        console.print(f"Session storage: {runtime.token_store.backend_name}")


@auth_app.command("logout")
def auth_logout() -> None:
    """End the current session and remove locally stored tokens."""

    with command_errors():
        runtime = _runtime()
        runtime.auth.logout()
        console.print("Signed out.")


@auth_app.command("status")
def auth_status(
    json_output: Annotated[bool, typer.Option("--json", help="Emit machine-readable output.")] = False,
) -> None:
    """Show the current signed-in account."""

    with command_errors():
        runtime = _runtime()
        user = runtime.auth.status()
        if not user:
            if json_output:
                _emit({"signedIn": False})
            else:
                console.print("Not signed in.")
            return
        attributes = {
            item.get("Name"): item.get("Value") for item in user.get("UserAttributes", [])
        }
        payload = {
            "signedIn": True,
            "email": attributes.get("email"),
            "username": user.get("Username"),
            "tokenStorage": runtime.token_store.backend_name,
        }
        if json_output:
            _emit(payload)
        else:
            console.print(f"Signed in as [bold]{payload['email'] or payload['username']}[/bold]")


@source_app.command("add")
def source_add(
    path: Annotated[Path, typer.Argument(help="Folder to index recursively.")],
    name: Annotated[str | None, typer.Option("--name", help="Friendly source name.")] = None,
    mode: Annotated[str, typer.Option("--mode", help="Storage mode; Phase 1 supports Local.")] = "Local",
    json_output: Annotated[bool, typer.Option("--json", help="Emit machine-readable output.")] = False,
) -> None:
    """Register a folder source. Local mode never uploads original media."""

    with command_errors():
        root = path.expanduser().resolve(strict=True)
        if not root.is_dir():
            raise ValueError(f"Source path is not a directory: {root}")
        normalized_mode = mode.capitalize()
        if normalized_mode == "Remote":
            raise ValueError(
                "Remote mode is planned for Phase 3; no files were uploaded or changed."
            )
        if normalized_mode != "Local":
            raise ValueError("Mode must be Local")
        runtime = _runtime()
        device = _register_device(runtime)
        source_key = runtime.state.source_key_for_root(root)
        lifecycle_generation = runtime.state.source_lifecycle_generation(root)
        source_request = {
            "deviceId": device["deviceId"],
            "sourceKey": source_key,
            "sourceType": "Folder",
            "displayName": name or root.name or str(root),
            "storageMode": normalized_mode,
            "permissionState": "NotApplicable",
            "syncSettings": {
                "automaticSync": True,
                "networkPolicy": "WiFiOnly",
                "requireChargingForHistoricalUpload": True,
            },
        }
        response = runtime.api.create_source(
            source_request,
            key=f"source:{source_key}:g{lifecycle_generation}",
        )
        binding = runtime.state.bind_source(response, root)
        payload = {
            "sourceId": binding.source_id,
            "name": binding.display_name,
            "path": binding.root_path,
            "storageMode": binding.storage_mode,
        }
        if json_output:
            _emit(payload)
        else:
            console.print(f"[green]Added[/green] {binding.display_name} ({binding.storage_mode})")
            console.print(binding.root_path)


@source_app.command("list")
def source_list(
    json_output: Annotated[bool, typer.Option("--json", help="Emit machine-readable output.")] = False,
) -> None:
    """List sources and their local folder bindings."""

    with command_errors():
        runtime = _runtime()
        sources = runtime.api.list_sources()
        paths = {item.source_id: item.root_path for item in runtime.state.list_bindings()}
        payload = [dict(item, localPath=paths.get(str(item.get("sourceId")))) for item in sources]
        if json_output:
            _emit(payload)
            return
        table = Table(title="ImageTracker sources")
        table.add_column("Name", style="bold")
        table.add_column("Mode")
        table.add_column("Status")
        table.add_column("Path")
        table.add_column("Source ID")
        for item in payload:
            table.add_row(
                str(item.get("displayName") or ""),
                str(item.get("storageMode") or ""),
                str(item.get("status") or ""),
                str(item.get("localPath") or "—"),
                str(item.get("sourceId") or ""),
            )
        console.print(table)


@source_app.command("set-mode")
def source_set_mode(source: str, mode: str) -> None:
    """Keep a source in Local mode; Remote mode arrives in Phase 3."""

    with command_errors():
        normalized_mode = mode.capitalize()
        if normalized_mode == "Remote":
            raise ValueError(
                "Remote mode is planned for Phase 3; no files were uploaded or changed."
            )
        if normalized_mode != "Local":
            raise ValueError("Mode must be Local")
        runtime = _runtime()
        binding = runtime.state.resolve_binding(source)
        response = runtime.api.update_source(
            binding.source_id,
            {"storageMode": normalized_mode},
            key=f"source-mode:{binding.source_id}:{normalized_mode}",
        )
        runtime.state.update_binding_mode(binding.source_id, str(response["storageMode"]))
        console.print(f"[green]Updated[/green] {binding.display_name} to {response['storageMode']} mode.")


@source_app.command("remove")
def source_remove(
    source: str,
    yes: Annotated[bool, typer.Option("--yes", "-y", help="Skip the destructive confirmation.")] = False,
) -> None:
    """Remove a source registration without deleting files from disk."""

    with command_errors():
        runtime = _runtime()
        binding = runtime.state.resolve_binding(source)
        if not yes and not typer.confirm(f"Remove source '{binding.display_name}'?"):
            console.print("Cancelled.")
            return
        lifecycle_generation = runtime.state.source_lifecycle_generation(binding.root_path)
        runtime.api.remove_source(
            binding.source_id,
            key=f"source-remove:{binding.source_id}:g{lifecycle_generation}",
        )
        runtime.state.remove_binding(binding.source_id)
        console.print(f"Removed {binding.display_name}.")


def _print_sync(summary: SyncSummary) -> None:
    table = Table(title="ImageTracker sync")
    table.add_column("Result", style="bold")
    table.add_column("Count", justify="right")
    rows = {
        "Scanned": summary.scanned,
        "Hashed": summary.hashed,
        "Hash cache hits": summary.cached,
        "Unchanged": summary.unchanged,
        "Upserts": summary.upserts,
        "Deleted occurrences": summary.deletions,
        "Exact duplicates linked": summary.duplicates_linked,
        "Failed": summary.failed,
        "Quarantined batches": summary.quarantined_batches,
        "Quarantined entries": summary.quarantined_entries,
        "Rejected entries": summary.rejected_entries,
        "Manifest batches sent": summary.batches_sent,
    }
    for label, value in rows.items():
        table.add_row(label, str(value))
    console.print(table)
    if summary.dry_run:
        console.print("[yellow]Dry run:[/yellow] no manifest was sent.")


@app.command()
def sync(
    source: Annotated[str | None, typer.Argument(help="Source ID, name, or local path.")] = None,
    dry_run: Annotated[bool, typer.Option("--dry-run", help="Scan and hash without changing the service.")] = False,
    watch: Annotated[bool, typer.Option("--watch", help="Keep watching and synchronize changes.")] = False,
    json_output: Annotated[bool, typer.Option("--json", help="Emit one JSON result per sync cycle.")] = False,
    no_input: Annotated[bool, typer.Option("--no-input", help="Never prompt; suitable for automation.")] = False,
    force_rehash: Annotated[
        bool,
        typer.Option("--force-rehash", help="Ignore the local hash cache and reread every media file."),
    ] = False,
) -> None:
    """Hash and synchronize a Local folder with safe deletion detection."""

    del no_input  # Sync is deliberately non-interactive.
    with command_errors():
        runtime = _runtime()
        while True:
            binding = runtime.state.resolve_binding(source)
            progress = (lambda message: None) if json_output else (
                lambda message: console.print(f"[dim]{message}[/dim]")
            )
            engine = SyncEngine(runtime.api, runtime.state, progress=progress)
            summary = engine.sync(binding, dry_run=dry_run, force_rehash=force_rehash)
            if json_output:
                _emit(summary.as_dict())
            else:
                _print_sync(summary)
            if summary.failed > 0:
                _error(
                    "Sync completed with files or manifest entries needing attention. "
                    "Inspect quarantined batches with 'imagetracker outbox list'.",
                    ExitCode.PARTIAL_SYNC,
                )
            if not watch:
                break
            time.sleep(30)


@app.command()
def status(
    follow: Annotated[bool, typer.Option("--follow", help="Refresh until interrupted.")] = False,
    json_output: Annotated[bool, typer.Option("--json", help="Emit machine-readable output.")] = False,
) -> None:
    """Show local sync queues and server processing activity."""

    with command_errors():
        runtime = _runtime()
        while True:
            payload = {
                "pendingManifestBatches": runtime.state.pending_count(),
                "failedManifestBatches": runtime.state.failed_count(),
                "sources": len(runtime.state.list_bindings()),
                "jobs": runtime.api.list_jobs(limit=50),
                "recentScans": runtime.state.recent_scans(),
            }
            if json_output:
                _emit(payload)
            else:
                console.print(
                    f"[bold]{payload['pendingManifestBatches']}[/bold] queued manifest batches · "
                    f"[bold]{payload['failedManifestBatches']}[/bold] need attention · "
                    f"[bold]{payload['sources']}[/bold] local sources"
                )
                table = Table(title="Processing activity")
                table.add_column("Type")
                table.add_column("State")
                table.add_column("Attempts", justify="right")
                table.add_column("Message")
                for job in payload["jobs"]:
                    table.add_row(
                        str(job.get("jobType") or ""),
                        str(job.get("state") or job.get("status") or ""),
                        str(job.get("attemptCount") or 0),
                        str(job.get("userMessage") or ""),
                    )
                console.print(table)
            if not follow:
                break
            time.sleep(10)


@outbox_app.command("list")
def outbox_list(
    state: Annotated[
        str,
        typer.Option("--state", help="Failed, Pending, Sent, Discarded, or All."),
    ] = "Failed",
    limit: Annotated[int, typer.Option("--limit", min=1, max=1000)] = 100,
    json_output: Annotated[bool, typer.Option("--json", help="Emit machine-readable output.")] = False,
) -> None:
    """Inspect queued and quarantined manifest batches."""

    with command_errors():
        normalized = state.capitalize()
        if normalized not in {"Failed", "Pending", "Sent", "Discarded", "All"}:
            raise ValueError("State must be Failed, Pending, Sent, Discarded, or All")
        batches = _runtime().state.list_outbox(state=normalized, limit=limit)
        payload = [
            {
                "batchId": batch.batch_id,
                "sourceId": batch.source_id,
                "scanId": batch.scan_id,
                "sequence": batch.sequence,
                "state": batch.state,
                "entryCount": len(batch.payload.get("entries") or []),
                "failure": batch.failure,
            }
            for batch in batches
        ]
        if json_output:
            _emit(payload)
            return
        table = Table(title="Manifest outbox")
        table.add_column("State", style="bold")
        table.add_column("Entries", justify="right")
        table.add_column("Issue")
        table.add_column("Batch ID")
        for item in payload:
            failures = ((item["failure"] or {}).get("entries") or [])
            first = failures[0] if failures else {}
            issue = first.get("errorMessage") or first.get("errorCode") or first.get("reason") or ""
            if len(failures) > 1:
                issue = f"{issue} (+{len(failures) - 1} more)"
            table.add_row(
                str(item["state"]),
                str(item["entryCount"]),
                str(issue),
                str(item["batchId"]),
            )
        console.print(table)


@outbox_app.command("discard")
def outbox_discard(
    batch_id: str,
    yes: Annotated[bool, typer.Option("--yes", "-y", help="Skip confirmation.")] = False,
) -> None:
    """Discard a Failed batch and release its rejected revisions for a fresh sync."""

    with command_errors():
        runtime = _runtime()
        if not yes and not typer.confirm(
            "Discard this failed batch? Its rejected revisions may be submitted again on the next sync."
        ):
            console.print("Cancelled.")
            return
        runtime.state.discard_outbox(batch_id)
        console.print(
            f"[green]Discarded[/green] failed batch {batch_id}. "
            "Rejected revisions are eligible for a fresh idempotency key on the next sync."
        )


def _media_table(items: list[Mapping[str, Any]], *, title: str) -> Table:
    table = Table(title=title)
    table.add_column("Captured")
    table.add_column("Type")
    table.add_column("File", style="bold")
    table.add_column("State")
    table.add_column("Media ID")
    for raw in items:
        item = raw.get("asset") if isinstance(raw.get("asset"), Mapping) else raw
        temporal = item.get("temporal") or {}
        table.add_row(
            str(temporal.get("capturedAtLocal") or temporal.get("capturedAtUtc") or "—"),
            str(item.get("mediaType") or ""),
            str(item.get("displayFileName") or ""),
            str(item.get("state") or ""),
            str(item.get("mediaAssetId") or ""),
        )
    return table


@media_app.command("list")
def media_list(
    limit: Annotated[int, typer.Option("--limit", min=1, max=200)] = 50,
    source_id: Annotated[str | None, typer.Option("--source-id")] = None,
    media_type: Annotated[str | None, typer.Option("--type", help="Photo or Video.")] = None,
    json_output: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    """List Local media visible on this registered device."""

    with command_errors():
        if media_type and media_type.capitalize() not in {"Photo", "Video"}:
            raise ValueError("Media type must be Photo or Video")
        runtime = _runtime()
        items = runtime.api.list_media(
            _registered_device_id(runtime),
            limit=limit,
            source_id=source_id,
            media_type=media_type.capitalize() if media_type else None,
        )
        if json_output:
            _emit(items)
        else:
            console.print(_media_table(items, title="Local media"))


@media_app.command("show")
def media_show(
    media_asset_id: str,
    json_output: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    """Show metadata and evidence for one visible media asset."""

    with command_errors():
        runtime = _runtime()
        item = runtime.api.get_media(media_asset_id, _registered_device_id(runtime))
        if json_output:
            _emit(item)
        else:
            console.print(_media_table([item], title="Media detail"))
            console.print_json(data=item)


@media_app.command("search")
def media_search(
    query: str,
    limit: Annotated[int, typer.Option("--limit", min=1, max=200)] = 50,
    json_output: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    """Search filenames and indexed Local media metadata."""

    with command_errors():
        runtime = _runtime()
        items = runtime.api.search_media(
            query,
            _registered_device_id(runtime),
            limit=limit,
        )
        if json_output:
            _emit(items)
        else:
            console.print(_media_table(items, title=f"Search: {query}"))


@jobs_app.command("list")
def jobs_list(
    status_filter: Annotated[str | None, typer.Option("--status")] = None,
    limit: Annotated[int, typer.Option("--limit", min=1, max=200)] = 50,
    json_output: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    """List asynchronous processing activity."""

    with command_errors():
        items = _runtime().api.list_jobs(limit=limit, status=status_filter)
        if json_output:
            _emit(items)
            return
        table = Table(title="Processing jobs")
        table.add_column("Type")
        table.add_column("Status", style="bold")
        table.add_column("Attempts", justify="right")
        table.add_column("Message")
        table.add_column("Job ID")
        for item in items:
            table.add_row(
                str(item.get("jobType") or ""),
                str(item.get("status") or item.get("state") or ""),
                str(item.get("attemptCount") or 0),
                str(item.get("userMessage") or ""),
                str(item.get("jobId") or ""),
            )
        console.print(table)


@jobs_app.command("retry")
def jobs_retry(
    job_id: str,
    json_output: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    """Retry a job that has reached Needs attention."""

    with command_errors():
        item = _runtime().api.retry_job(job_id)
        if json_output:
            _emit(item)
        else:
            console.print(f"[green]Retry queued[/green] for job {job_id}.")


@legacy_app.command("audit")
def legacy_audit(
    json_output: Annotated[bool, typer.Option("--json", help="Emit machine-readable output.")] = False,
) -> None:
    """Run the service's read-only legacy and data-foundation audit."""

    with command_errors():
        inspector, _state = _legacy_runtime()
        payload = inspector.audit()
        if json_output:
            _emit(payload)
            return
        table = Table(title="Legacy audit")
        table.add_column("Check", style="bold")
        table.add_column("Status")
        table.add_column("Detail")
        for check in payload.get("checks", []):
            table.add_row(
                str(check.get("code") or check.get("name") or check.get("check") or ""),
                str(check.get("status") or ""),
                str(check.get("detail") or check.get("message") or ""),
            )
        console.print(table)


@legacy_app.command("migrate")
def legacy_migrate(
    dry_run: Annotated[bool, typer.Option("--dry-run", help="Required: preview without writing data.")] = False,
    after_id: Annotated[
        int | None,
        typer.Option("--after-id", min=0, help="Resume preview after this legacy ImageAsset ID."),
    ] = None,
    limit: Annotated[
        int,
        typer.Option("--limit", min=1, max=1000, help="Maximum rows in this preview batch."),
    ] = 500,
    save_checkpoint: Annotated[
        bool,
        typer.Option(
            "--save-checkpoint",
            help="Save only the local preview cursor; no database rows are changed.",
        ),
    ] = False,
    json_output: Annotated[bool, typer.Option("--json", help="Emit machine-readable output.")] = False,
) -> None:
    """Preview legacy rows; optional checkpoints remain local and preview-only."""

    if not dry_run:
        _error("Legacy migration is preview-only in Phase 1; rerun with --dry-run.", ExitCode.CONFIGURATION)
    with command_errors():
        inspector, state = _legacy_runtime()
        saved_checkpoint_id, processed_count = state.legacy_checkpoint()
        checkpoint_id = saved_checkpoint_id if after_id is None else after_id
        payload = inspector.migration_preview(checkpoint_legacy_id=checkpoint_id, limit=limit)
        payload["checkpointProcessedCount"] = processed_count
        if save_checkpoint:
            next_id = int(payload["nextCheckpointLegacyId"])
            next_processed = processed_count + int(payload["batchRows"])
            state.save_legacy_checkpoint(next_id, next_processed)
            payload["checkpointProcessedCount"] = next_processed
            payload["checkpointSaved"] = True
            payload["checkpointScope"] = "LocalPreviewOnly"
        else:
            payload["checkpointSaved"] = False
        if json_output:
            _emit(payload)
        else:
            console.print("[yellow]Legacy migration preview[/yellow]")
            console.print_json(data=payload)
            if save_checkpoint:
                console.print(
                    "[green]Saved the local preview cursor.[/green] No MySQL rows were changed."
                )


def main() -> None:
    app()


if __name__ == "__main__":
    main()
