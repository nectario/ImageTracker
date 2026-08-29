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
from rich.markup import escape
from rich.table import Table
from rich.theme import Theme

from .api_client import ApiError, AuthenticationRequired
from .config import DEFAULT_STACK_NAME, ConfigStore, config_from_stack
from .media import MediaScanner
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
outbox_app = typer.Typer(help="Inspect resumable sync and scene-preview work.", no_args_is_help=True)
media_app = typer.Typer(help="Browse account-visible Local media metadata.", no_args_is_help=True)
jobs_app = typer.Typer(help="Inspect and retry media processing jobs.", no_args_is_help=True)
app.add_typer(auth_app, name="auth")
app.add_typer(source_app, name="source")
app.add_typer(legacy_app, name="legacy")
app.add_typer(outbox_app, name="outbox")
app.add_typer(media_app, name="media")
app.add_typer(jobs_app, name="jobs")

CLI_THEME = Theme(
    {
        # Exact warm/muted colors from the user's Ubuntu Powerlevel10k prompt.
        # The muted teal already identifies Codex in that theme.
        "accent": "bold #D45C0C",
        "count": "bold #D79921",
        "error": "bold #E74856",
        "info": "#448487",
        "key": "bold #FBF1C7",
        "muted": "#A8A8A8",
        "path": "#D79921",
        "progress": "#448487",
        "success": "bold #5A8A5A",
        "title": "bold #D45C0C",
        "warning": "bold #D79921",
    }
)
console = Console(theme=CLI_THEME)
error_console = Console(stderr=True, theme=CLI_THEME)


def _table(
    *,
    title: str,
    show_header: bool = True,
) -> Table:
    return Table(
        title=title,
        show_header=show_header,
        title_style="title",
        header_style="accent",
        border_style="#767676",
    )


def _state_text(value: Any) -> str:
    rendered = str(value or "")
    normalized = rendered.casefold()
    if normalized in {"ready", "sent", "succeeded", "complete", "active"}:
        style = "success"
    elif normalized in {"failed", "needsattention", "error"}:
        style = "error"
    elif normalized in {"deferred", "deferredquota", "pendingquota"}:
        style = "warning"
    elif normalized in {"discarded", "cancelled"}:
        style = "title"
    else:
        style = "progress"
    return f"[{style}]{escape(rendered)}[/{style}]"


def package_version() -> str:
    try:
        return version("imagetracker")
    except PackageNotFoundError:
        return "0.3.0"


def _emit(payload: Any) -> None:
    typer.echo(json.dumps(payload, sort_keys=True, default=str))


def _error(message: str, code: ExitCode) -> None:
    error_console.print(f"[error]Error:[/error] [key]{message}[/key]")
    raise typer.Exit(int(code))


@contextmanager
def command_errors(*, interrupt_message: str = "Stopped.") -> Iterator[None]:
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
        console.print(f"[muted]{interrupt_message}[/muted]")
        raise typer.Exit(0) from None


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
            console.print(f"[success]Configured[/success] [key]{stack}[/key] in [accent]{region}[/accent]")
            console.print(f"[accent]API:[/accent] [path]{config.api_url}[/path]")


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
            "logical_cpu_threads": os.cpu_count() or 1,
            "recommended_scan_workers": MediaScanner.recommended_worker_count(),
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
        table = _table(title="ImageTracker doctor", show_header=False)
        table.add_column("Check", style="accent")
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
        console.print(f"[success]Account created.[/success] Verification code sent to [accent]{destination}[/accent].")
        console.print(f"Confirm with: imagetracker auth confirm {email} CODE")


@auth_app.command("confirm")
def auth_confirm(email: str, code: str) -> None:
    """Confirm the one-time email verification code."""

    with command_errors():
        runtime = _runtime()
        runtime.auth.confirm(email, code)
        console.print("[success]Email confirmed.[/success] You can now sign in.")


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
        console.print(f"[success]Signed in[/success] as [accent]{tokens.email or email}[/accent].")
        console.print(f"[accent]Session storage:[/accent] {runtime.token_store.backend_name}")


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
            console.print(f"Signed in as [accent]{payload['email'] or payload['username']}[/accent]")


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
            console.print(f"[success]Added[/success] [accent]{binding.display_name}[/accent] ([key]{binding.storage_mode}[/key])")
            console.print(f"[path]{binding.root_path}[/path]")
            console.print(
                "[accent]Fast add:[/accent] "
                f"imagetracker sync {binding.source_id} --fast-add"
            )


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
        table = _table(title="ImageTracker sources")
        table.add_column("Name", style="accent")
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
        console.print(f"[success]Updated[/success] [accent]{binding.display_name}[/accent] to [key]{response['storageMode']}[/key] mode.")


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
    table = _table(title="ImageTracker sync")
    table.add_column("Result", style="accent")
    table.add_column("Count", justify="right", style="key")
    rows = {
        "Scanned": summary.scanned,
        "Hashed": summary.hashed,
        "Hash cache hits": summary.cached,
        "Scanner workers": summary.scan_workers,
        "Scan time": f"{summary.scan_seconds:.2f}s",
        "Scan throughput": f"{summary.scan_files_per_second:,.0f} files/s",
        "Hashes deferred": summary.hash_pending,
        "Unchanged": summary.unchanged,
        "Upserts": summary.upserts,
        "Deleted occurrences": summary.deletions,
        "Exact duplicates linked": summary.duplicates_linked,
        "Failed": summary.failed,
        "Quarantined batches": summary.quarantined_batches,
        "Quarantined entries": summary.quarantined_entries,
        "Rejected entries": summary.rejected_entries,
        "Manifest batches sent": summary.batches_sent,
        "Scene previews staged": summary.descriptions_staged,
        "Scene previews recovered": summary.descriptions_recovered,
        "Scene previews pending": summary.description_pending,
        "Scene previews quota-deferred": summary.description_deferred,
        "Scene previews needing attention": summary.description_quarantined,
    }
    for label, value in rows.items():
        table.add_row(label, str(value))
    console.print(table)
    if summary.dry_run:
        console.print("[warning]Dry run:[/warning] no manifest was sent.")


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
    scan_workers: Annotated[
        int | None,
        typer.Option(
            "--scan-workers",
            "-j",
            min=1,
            max=256,
            help=(
                "Parallel hashing/metadata workers. Default: auto-tuned "
                "(up to 64)."
            ),
        ),
    ] = None,
    fast_add: Annotated[
        bool,
        typer.Option(
            "--fast-add",
            help=(
                "Register new files from directory metadata immediately; "
                "defer content hashing and EXIF extraction to a later normal sync."
            ),
        ),
    ] = False,
) -> None:
    """Hash and synchronize a Local folder with safe deletion detection."""

    del no_input  # Sync is deliberately non-interactive.
    with command_errors(
        interrupt_message=(
            "Stopped. The current discovery/stat pass restarts on the next run; "
            "completed hash-cache batches and queued manifests remain saved."
        )
    ):
        if fast_add and force_rehash:
            raise ValueError("--fast-add cannot be combined with --force-rehash")
        runtime = _runtime()
        while True:
            binding = runtime.state.resolve_binding(source)
            progress = (lambda message: None) if json_output else (
                lambda message: console.print(f"[progress]{message}[/progress]")
            )
            engine = SyncEngine(runtime.api, runtime.state, progress=progress)
            summary = engine.sync(
                binding,
                dry_run=dry_run,
                force_rehash=force_rehash,
                scan_workers=scan_workers,
                fast_add=fast_add,
            )
            if json_output:
                _emit(summary.as_dict())
            else:
                _print_sync(summary)
            if summary.failed > 0:
                _error(
                    "Sync completed with work needing attention or a later retry. "
                    "Inspect it with 'imagetracker outbox list' and "
                    "'imagetracker outbox descriptions'.",
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
            description_counts = runtime.state.description_counts()
            payload = {
                "pendingManifestBatches": runtime.state.pending_count(),
                "failedManifestBatches": runtime.state.failed_count(),
                "pendingDescriptionPreviews": description_counts["Pending"],
                "deferredDescriptionPreviews": description_counts["Deferred"],
                "failedDescriptionPreviews": description_counts["Failed"],
                "sentDescriptionPreviews": description_counts["Sent"],
                "sources": len(runtime.state.list_bindings()),
                "jobs": runtime.api.list_jobs(limit=50),
                "recentScans": runtime.state.recent_scans(),
            }
            if json_output:
                _emit(payload)
            else:
                console.print(
                    f"[count]{payload['pendingManifestBatches']}[/count] queued manifest batches · "
                    f"[count]{payload['failedManifestBatches']}[/count] need attention · "
                    f"[count]{payload['pendingDescriptionPreviews']}[/count] scene previews queued · "
                    f"[count]{payload['deferredDescriptionPreviews']}[/count] quota-deferred · "
                    f"[count]{payload['failedDescriptionPreviews']}[/count] scene previews need attention · "
                    f"[count]{payload['sources']}[/count] local sources"
                )
                table = _table(title="Processing activity")
                table.add_column("Type", style="accent")
                table.add_column("State", style="key")
                table.add_column("Attempts", justify="right")
                table.add_column("Message")
                for job in payload["jobs"]:
                    table.add_row(
                        str(job.get("jobType") or ""),
                        _state_text(job.get("state") or job.get("status")),
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
        table = _table(title="Manifest outbox")
        table.add_column("State", style="accent")
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
                _state_text(item["state"]),
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
            f"[success]Discarded[/success] failed batch [accent]{batch_id}[/accent]. "
            "Rejected revisions are eligible for a fresh idempotency key on the next sync."
        )


@outbox_app.command("descriptions")
def outbox_descriptions(
    state: Annotated[
        str,
        typer.Option("--state", help="Failed, Pending, Deferred, Sent, or All."),
    ] = "Failed",
    limit: Annotated[int, typer.Option("--limit", min=1, max=1000)] = 100,
    json_output: Annotated[bool, typer.Option("--json", help="Emit machine-readable output.")] = False,
) -> None:
    """Inspect scene-description previews without exposing local paths."""

    with command_errors():
        normalized = state.capitalize()
        if normalized not in {"Failed", "Pending", "Deferred", "Sent", "All"}:
            raise ValueError("State must be Failed, Pending, Deferred, Sent, or All")
        tasks = _runtime().state.list_description_outbox(state=normalized, limit=limit)
        payload = [
            {
                "jobId": task.job_id,
                "sourceId": task.source_id,
                "occurrenceId": task.occurrence_id,
                "mediaAssetId": task.media_asset_id,
                "sourceItemId": task.source_item_id,
                "fileName": task.file_name,
                "state": task.state,
                "attemptCount": task.attempt_count,
                "nextAttemptAtUtc": task.next_attempt_at_utc,
                "error": task.error,
            }
            for task in tasks
        ]
        if json_output:
            _emit(payload)
            return
        table = _table(title="Scene-description outbox")
        table.add_column("State", style="accent")
        table.add_column("File")
        table.add_column("Attempts", justify="right")
        table.add_column("Next attempt")
        table.add_column("Issue")
        table.add_column("Job ID")
        for item in payload:
            error = item["error"] or {}
            table.add_row(
                _state_text(item["state"]),
                str(item["fileName"]),
                str(item["attemptCount"]),
                str(item["nextAttemptAtUtc"] or "—"),
                str(error.get("message") or error.get("code") or ""),
                str(item["jobId"]),
            )
        console.print(table)


@outbox_app.command("retry-description")
def outbox_retry_description(job_id: str) -> None:
    """Make a quarantined or deferred scene preview eligible for the next sync."""

    with command_errors():
        _runtime().state.retry_description(job_id)
        console.print(
            f"[success]Queued[/success] scene preview [accent]{job_id}[/accent]. Run 'imagetracker sync' to retry it."
        )


def _media_table(items: list[Mapping[str, Any]], *, title: str) -> Table:
    table = _table(title=title)
    table.add_column("Captured")
    table.add_column("Type")
    table.add_column("File", style="accent")
    table.add_column("Location")
    table.add_column("State")
    table.add_column("Media ID")
    for raw in items:
        item = raw.get("asset") if isinstance(raw.get("asset"), Mapping) else raw
        temporal = item.get("temporal") or {}
        location = item.get("locationDetail") or item.get("location") or {}
        location_label = (
            location.get("streetAddress")
            or location.get("displayName")
            or location.get("city")
            or "—"
        )
        table.add_row(
            str(temporal.get("capturedAtLocal") or temporal.get("capturedAtUtc") or "—"),
            str(item.get("mediaType") or ""),
            str(item.get("displayFileName") or ""),
            str(location_label),
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
        normalized_status = None
        if status_filter:
            statuses = {
                value.casefold(): value
                for value in (
                    "Preparing",
                    "Queued",
                    "Running",
                    "Succeeded",
                    "DeferredQuota",
                    "Failed",
                    "Cancelled",
                )
            }
            normalized_status = statuses.get(status_filter.strip().casefold())
            if normalized_status is None:
                raise ValueError(
                    "Status must be Preparing, Queued, Running, Succeeded, "
                    "DeferredQuota, Failed, or Cancelled"
                )
        items = _runtime().api.list_jobs(limit=limit, status=normalized_status)
        if json_output:
            _emit(items)
            return
        table = _table(title="Processing jobs")
        table.add_column("Type")
        table.add_column("Status", style="accent")
        table.add_column("Attempts", justify="right")
        table.add_column("Message")
        table.add_column("Job ID")
        for item in items:
            table.add_row(
                str(item.get("jobType") or ""),
                _state_text(item.get("status") or item.get("state")),
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
        runtime = _runtime()
        current = runtime.api.get_job(job_id)
        is_description = str(current.get("jobType") or "") == "Description"
        if is_description and runtime.state.description_task(job_id) is None:
            raise ValueError(
                "This scene description must be retried on a device that has "
                "its Local source photo."
            )
        retry_key = (
            f"job-retry:{job_id}:{current.get('status') or 'unknown'}:"
            f"{current.get('attemptCount') or 0}"
        )
        item = runtime.api.retry_job(job_id, key=retry_key)
        if is_description and str(item.get("status") or "") == "Preparing":
            runtime.state.retry_description(job_id, allow_sent=True)
            if (
                str(current.get("errorCode") or "") == "ProviderCircuitOpen"
                or str(current.get("failureClass") or "") == "Authentication"
                or (
                    str(current.get("failureClass") or "") == "Quota"
                    and str(current.get("errorCode") or "")
                    != "MonthlySceneDescriptionLimitReached"
                )
            ):
                runtime.state.retry_all_deferred_descriptions()
        if json_output:
            _emit(item)
        else:
            if is_description and str(item.get("status") or "") == "Preparing":
                console.print(
                    f"[success]Preview retry ready[/success] for job [accent]{job_id}[/accent]. "
                    "Run 'imagetracker sync' on this source device."
                )
            else:
                console.print(f"[success]Retry queued[/success] for job [accent]{job_id}[/accent].")


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
        table = _table(title="Legacy audit")
        table.add_column("Check", style="accent")
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
            console.print("[warning]Legacy migration preview[/warning]")
            console.print_json(data=payload)
            if save_checkpoint:
                console.print(
                    "[success]Saved the local preview cursor.[/success] No MySQL rows were changed."
                )


def main() -> None:
    app()


if __name__ == "__main__":
    main()
