#!/usr/bin/env python3
"""Register and inspect the Olist PostgreSQL Debezium connector."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from urllib import error, request

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TEMPLATE = PROJECT_ROOT / "infra" / "debezium" / "olist-postgres.template.json"
REQUIRED_ENV = ("POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD")
PLACEHOLDER = re.compile(r"\$\{([A-Z][A-Z0-9_]*)\}")


class CdcError(Exception):
    """An expected configuration or Connect API error."""


def read_settings(env_file: Path) -> dict[str, str]:
    """Read the existing .env; explicitly exported variables take precedence."""
    from dotenv import dotenv_values

    if not env_file.is_file():
        raise CdcError(f"Missing environment file: {env_file}")
    settings = {
        key: value
        for key, value in dotenv_values(env_file).items()
        if value is not None
    }
    for key in (*REQUIRED_ENV, "POSTGRES_HOST", "POSTGRES_PORT"):
        if key in os.environ:
            settings[key] = os.environ[key]
    missing = [key for key in REQUIRED_ENV if not settings.get(key)]
    if missing:
        raise CdcError("Missing PostgreSQL settings: " + ", ".join(missing))
    return settings


def render_template(template_file: Path, settings: dict[str, str]) -> dict:
    """Substitute placeholders in memory, then serialize with json.dumps at POST time."""
    if not template_file.is_file():
        raise CdcError(f"Missing connector template: {template_file}")
    try:
        template = json.loads(template_file.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CdcError(f"Invalid connector template: {exc}") from exc

    def substitute(value):
        if isinstance(value, str):
            def replace(match):
                key = match.group(1)
                if not settings.get(key):
                    raise CdcError(f"Missing value for template placeholder: {key}")
                return settings[key]
            return PLACEHOLDER.sub(replace, value)
        if isinstance(value, list):
            return [substitute(item) for item in value]
        if isinstance(value, dict):
            return {key: substitute(item) for key, item in value.items()}
        return value

    payload = substitute(template)
    if not isinstance(payload, dict) or not isinstance(payload.get("name"), str):
        raise CdcError("Connector template needs a name")
    config = payload.get("config")
    if not isinstance(config, dict):
        raise CdcError("Connector template needs a config object")
    if config.get("publication.autocreate.mode") != "disabled":
        raise CdcError("Publication must be created by sql/002_olist_cdc.sql")
    if config.get("database.password") != settings["POSTGRES_PASSWORD"]:
        raise CdcError("Connector password placeholder was not resolved")
    return payload


def connect_api(base_url: str, method: str, path: str, payload: dict | None = None,
                secret: str = "") -> tuple[int, dict]:
    url = base_url.rstrip("/") + path
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    headers = {"Accept": "application/json"}
    if body is not None:
        headers["Content-Type"] = "application/json"
    call = request.Request(url, data=body, headers=headers, method=method)
    try:
        with request.urlopen(call, timeout=15) as response:
            raw = response.read().decode("utf-8")
            return response.status, json.loads(raw) if raw else {}
    except error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")[:1000]
        if secret:
            raw = raw.replace(secret, "<redacted>")
        if exc.code == 404:
            return 404, {}
        raise CdcError(f"Connect API HTTP {exc.code}: {raw}") from exc
    except (error.URLError, TimeoutError) as exc:
        raise CdcError(f"Cannot reach Kafka Connect at {base_url}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise CdcError("Kafka Connect returned invalid JSON") from exc


def register(args) -> None:
    settings = read_settings(args.env_file)
    payload = render_template(args.template, settings)
    name = payload["name"]
    secret = settings["POSTGRES_PASSWORD"]
    code, _ = connect_api(args.connect_url, "GET", f"/connectors/{name}/status", secret=secret)
    if code != 404:
        print(f"Connector {name} already exists; configuration was not changed.")
        return
    connect_api(args.connect_url, "POST", "/connectors", payload, secret)
    print(f"Registered {name}. Kafka Connect now starts the initial snapshot.")


def status(args) -> None:
    template = json.loads(args.template.read_text(encoding="utf-8"))
    name = template["name"]
    code, result = connect_api(args.connect_url, "GET", f"/connectors/{name}/status")
    if code == 404:
        raise CdcError(f"Connector {name} is not registered")
    connector_state = result.get("connector", {}).get("state", "UNKNOWN")
    print(f"connector {name}: {connector_state}")
    for task in result.get("tasks", []):
        print(f"task {task.get('id', '?')}: {task.get('state', 'UNKNOWN')}")
    if connector_state != "RUNNING" or not result.get("tasks") or any(
        task.get("state") != "RUNNING" for task in result.get("tasks", [])
    ):
        raise CdcError("Connector is not fully RUNNING; inspect Connect logs")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("validate", "register", "status"))
    parser.add_argument("--env-file", type=Path, default=PROJECT_ROOT / ".env")
    parser.add_argument("--template", type=Path, default=DEFAULT_TEMPLATE)
    parser.add_argument("--connect-url", default="http://127.0.0.1:8083")
    args = parser.parse_args()
    try:
        if args.action == "validate":
            payload = render_template(args.template, read_settings(args.env_file))
            config = payload["config"]
            print(f"Template valid: {payload['name']}")
            print(f"Publication: {config['publication.name']}; slot: {config['slot.name']}")
            print(f"Tables: {config['table.include.list']}")
            print("Password was read from .env and was not printed or written.")
        elif args.action == "register":
            register(args)
        else:
            status(args)
    except (CdcError, OSError, KeyError, ValueError) as exc:
        print(f"CDC {args.action} failed: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())