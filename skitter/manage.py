"""CLI subcommands for managing apps and sessions.

Thin wrappers that format a runtime API query and send it to the
coordinator via A2A request.
"""

import argparse
import json

from skitter.run import run_prompt


def _query(text: str) -> None:
    run_prompt("skitter", text)


def create_app(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="skitter create-app",
        description="Create a composed multi-agent app.",
    )
    parser.add_argument("name", help="App name")
    parser.add_argument("instructions", help="What the app should do (plain English)")
    parser.add_argument(
        "--agents", required=True, help="Comma-separated list of agent IDs"
    )
    parser.add_argument("--id", default="", help="App ID (auto-generated if omitted)")
    parser.add_argument("--description", default="", help="App description")
    args = parser.parse_args(argv)

    agent_ids = [a.strip() for a in args.agents.split(",") if a.strip()]
    if not agent_ids:
        parser.error("--agents must list at least one agent ID")

    spec: dict = {
        "name": args.name,
        "instructions": args.instructions,
        "agents": agent_ids,
    }
    if args.id:
        spec["id"] = args.id
    if args.description:
        spec["description"] = args.description

    _query(f"create app {json.dumps(spec)}")


def list_apps(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="skitter list-apps", description="List all apps."
    )
    parser.parse_args(argv)
    _query("list apps")


def get_app(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="skitter get-app", description="Get app details."
    )
    parser.add_argument("app_id", help="App ID")
    args = parser.parse_args(argv)
    _query(f"get app {args.app_id}")


def delete_app(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="skitter delete-app", description="Delete an app and all its data."
    )
    parser.add_argument("app_id", help="App ID")
    args = parser.parse_args(argv)
    _query(f"delete app {args.app_id}")


def list_sessions(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="skitter list-sessions", description="List sessions."
    )
    parser.add_argument("app_id", nargs="?", default="", help="Filter by app ID")
    args = parser.parse_args(argv)
    q = f"list sessions {args.app_id}" if args.app_id else "list sessions"
    _query(q)


def get_session(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="skitter get-session", description="Get session details."
    )
    parser.add_argument("session_id", help="Session ID")
    args = parser.parse_args(argv)
    _query(f"get session {args.session_id}")


def cancel_session(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="skitter cancel-session", description="Cancel a running session."
    )
    parser.add_argument("session_id", help="Session ID")
    args = parser.parse_args(argv)
    _query(f"cancel session {args.session_id}")
