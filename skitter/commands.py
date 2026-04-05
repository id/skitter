"""Click-based CLI command tree.

All subcommands are defined here. Global flags (--skitter-home) are
handled once at the group level. Individual command modules supply the
implementation logic; this file handles only argument parsing and dispatch.
"""

import os

import click

from skitter.config import configure_logging


@click.group(invoke_without_command=True)
@click.option(
    "--skitter-home",
    envvar="SKITTER_HOME",
    default=None,
    help="Override config directory (default: ~/.skitter).",
)
@click.pass_context
def cli(ctx, skitter_home):
    """MQTT-based AI agent orchestrator."""
    if skitter_home:
        os.environ["SKITTER_HOME"] = skitter_home
    configure_logging()
    if ctx.invoked_subcommand is None:
        from skitter.coordinator import main

        main()


# --- One-shot request ---


@cli.command()
@click.argument("agent_id")
@click.argument("prompt", nargs=-1, required=True)
@click.option("--context", "context_id", default="", help="Conversation context ID.")
def ask(agent_id, prompt, context_id):
    """Send a one-shot A2A request to an agent."""
    from skitter.request import request_prompt

    request_prompt(agent_id, " ".join(prompt), context_id=context_id)


@cli.command(hidden=True)
@click.argument("agent_id")
@click.argument("prompt", nargs=-1, required=True)
@click.option("--context", "context_id", default="", help="Conversation context ID.")
def request(agent_id, prompt, context_id):
    """Send a one-shot A2A request (alias for ask)."""
    click.echo("Tip: 'skitter ask' is the recommended command.", err=True)
    from skitter.request import request_prompt

    request_prompt(agent_id, " ".join(prompt), context_id=context_id)


# --- Interactive chat ---


@cli.command()
@click.argument("agent_id")
def chat(agent_id):
    """Start an interactive A2A chat session."""
    import asyncio

    from skitter.cli import _run_chat

    try:
        asyncio.run(_run_chat(agent_id))
    except KeyboardInterrupt:
        pass
    finally:
        os._exit(0)


# --- Service management ---


@cli.command()
@click.option("--broker-only", is_flag=True, help="Start only the MQTT broker.")
@click.option("--agent", "agent_id", default="", help="Start a single agent by ID.")
def up(broker_only, agent_id):
    """Start broker, coordinator, and agents."""
    from skitter.services import up as _up

    argv = []
    if broker_only:
        argv.append("--broker-only")
    if agent_id:
        argv.extend(["--agent", agent_id])
    _up(argv)


@cli.command()
@click.option("--agent", "agent_id", default="", help="Stop a single agent by ID.")
def down(agent_id):
    """Stop all skitter containers."""
    from skitter.services import down as _down

    argv = []
    if agent_id:
        argv.extend(["--agent", agent_id])
    _down(argv)


@cli.command()
def status():
    """Show service readiness overview."""
    from skitter.services import status as _status

    _status()


@cli.command()
@click.argument("service")
@click.option("-f", "--follow", is_flag=True, help="Follow log output.")
def logs(service, follow):
    """View logs for a service (emqx, coordinator, agent-ID)."""
    from skitter.services import logs as _logs

    argv = [service]
    if follow:
        argv.append("--follow")
    _logs(argv)


# --- Setup and diagnostics ---


@cli.command()
@click.option("--non-interactive", is_flag=True, help="Use defaults without prompting.")
@click.option("--standalone", is_flag=True, help="Skip coordinator/broker setup.")
def setup(non_interactive, standalone):
    """Interactive setup wizard."""
    from skitter.setup import main as _setup

    argv = []
    if non_interactive:
        argv.append("--non-interactive")
    if standalone:
        argv.append("--standalone")
    _setup(argv)


@cli.command()
def doctor():
    """Run diagnostic health checks."""
    from skitter.doctor import main as _doctor

    _doctor()


# --- Agent runner ---


@cli.command("agent-runner")
@click.argument("agent_path")
def agent_runner(agent_path):
    """Run a standalone A2A agent process."""
    import asyncio

    from skitter.agent_runner import run

    asyncio.run(run(agent_path))


# --- Agent scaffolding ---


@cli.command("create-agent")
@click.argument("name")
@click.argument("prompt")
@click.option("--runtime", type=click.Choice(["claude", "codex"]), default="claude")
@click.option("--model", default="", help="Model name.")
@click.option(
    "--skill",
    "skills",
    multiple=True,
    help="Skill in name:description format (repeatable).",
)
@click.option("--dry-run", is_flag=True, help="Print without writing files.")
@click.option("--edit", is_flag=True, help="Open editor before saving.")
@click.option("--force", is_flag=True, help="Overwrite existing definition.")
def create_agent(name, prompt, runtime, model, skills, dry_run, edit, force):
    """Generate an agent definition via LLM."""
    from skitter.create_agent import run as _run

    argv = [name, prompt, "--runtime", runtime]
    if model:
        argv.extend(["--model", model])
    for s in skills:
        argv.extend(["--skill", s])
    if dry_run:
        argv.append("--dry-run")
    if edit:
        argv.append("--edit")
    if force:
        argv.append("--force")
    _run(argv)


# --- Manage commands (agents, apps, sessions) ---


@cli.command("list-agents")
def list_agents():
    """List agents discovered from broker."""
    from skitter.manage import list_agents as _list_agents

    _list_agents([])


@cli.command("get-agent")
@click.argument("agent_id")
def get_agent(agent_id):
    """Get agent discovery card (JSON)."""
    from skitter.manage import get_agent as _get_agent

    _get_agent([agent_id])


@cli.command("create-app")
@click.argument("name")
@click.argument("instructions")
@click.option("--agents", required=True, help="Comma-separated agent IDs.")
@click.option("--id", "app_id", default="", help="App ID (auto-generated if omitted).")
@click.option("--description", default="", help="App description.")
def create_app(name, instructions, agents, app_id, description):
    """Create a composed multi-agent app."""
    from skitter.manage import create_app as _create_app

    argv = [name, instructions, "--agents", agents]
    if app_id:
        argv.extend(["--id", app_id])
    if description:
        argv.extend(["--description", description])
    _create_app(argv)


@cli.command("list-apps")
def list_apps():
    """List all apps."""
    from skitter.manage import list_apps as _list_apps

    _list_apps([])


@cli.command("get-app")
@click.argument("app_id")
def get_app(app_id):
    """Get app details (JSON)."""
    from skitter.manage import get_app as _get_app

    _get_app([app_id])


@cli.command("delete-app")
@click.argument("app_id")
def delete_app(app_id):
    """Delete an app and all its data."""
    from skitter.manage import delete_app as _delete_app

    _delete_app([app_id])


@cli.command("list-sessions")
@click.argument("app_id", required=False, default="")
def list_sessions(app_id):
    """List sessions (optionally filter by app ID)."""
    from skitter.manage import list_sessions as _list_sessions

    argv = [app_id] if app_id else []
    _list_sessions(argv)


@cli.command("get-session")
@click.argument("session_id")
def get_session(session_id):
    """Get session details (JSON)."""
    from skitter.manage import get_session as _get_session

    _get_session([session_id])


@cli.command("cancel-session")
@click.argument("session_id")
def cancel_session(session_id):
    """Cancel a running session."""
    from skitter.manage import cancel_session as _cancel_session

    _cancel_session([session_id])
