import os
import sys

from skitter.config import configure_logging

_MANAGE_CMDS = {
    "list-agents",
    "get-agent",
    "create-app",
    "list-apps",
    "get-app",
    "delete-app",
    "list-sessions",
    "get-session",
    "cancel-session",
}


def _parse_global_flags() -> None:
    """Parse and consume global flags before subcommand dispatch."""
    if "--skitter-home" in sys.argv:
        idx = sys.argv.index("--skitter-home")
        if idx + 1 >= len(sys.argv):
            print("--skitter-home requires a path", file=sys.stderr)
            sys.exit(1)
        os.environ["SKITTER_HOME"] = sys.argv[idx + 1]
        sys.argv = sys.argv[:idx] + sys.argv[idx + 2 :]


def _parse_request_args(args: list[str]) -> tuple[str, str, str]:
    """Parse agent_id, prompt, and --context from request/ask args."""
    context_id = ""
    if "--context" in args:
        idx = args.index("--context")
        if idx + 1 >= len(args):
            print("--context requires a value", file=sys.stderr)
            sys.exit(1)
        context_id = args[idx + 1]
        args = args[:idx] + args[idx + 2 :]
    if len(args) < 2:
        return "", "", ""
    return args[0], " ".join(args[1:]), context_id


# Dispatch table: subcommand -> (module, function, passes_argv)
# Entries with passes_argv=True call func(sys.argv[2:]).
_DISPATCH: dict[str, tuple[str, str, bool]] = {
    "setup": ("skitter.setup", "main", False),
    "up": ("skitter.services", "up", True),
    "down": ("skitter.services", "down", True),
    "status": ("skitter.services", "status", True),
    "logs": ("skitter.services", "logs", True),
    "doctor": ("skitter.doctor", "main", False),
    "chat": ("skitter.cli", "main", False),
    "agent-runner": ("skitter.agent_runner", "main", False),
    "create-agent": ("skitter.create_agent", "main", False),
}


def dispatch() -> None:
    """Entry point for the ``skitter`` CLI.

    Routes to the appropriate sub-module based on the first positional arg:

        skitter                              -> coordinator (default)
        skitter setup                        -> interactive setup wizard
        skitter ask <agent> '<prompt>'       -> one-shot A2A request (primary)
        skitter chat <agent_id>              -> interactive A2A session
        skitter request <agent_id> '<prompt>' -> one-shot A2A request
        skitter up [--broker-only] [--agent] -> start services
        skitter down [--agent <id>]          -> stop services
        skitter status                       -> show readiness overview
        skitter logs <service>               -> view service logs
        skitter doctor                       -> diagnostic health checks
        skitter agent-runner <name|file>     -> standalone A2A agent process
        skitter create-agent <name> <prompt> -> generate agent definition via LLM
        skitter create-app <name> <instr>    -> create a composed multi-agent app
        skitter list-apps                    -> list all apps
        skitter get-app <id>                 -> get app details
        skitter delete-app <id>              -> delete an app
        skitter list-sessions [app_id]       -> list sessions
        skitter get-session <id>             -> get session details
        skitter cancel-session <id>          -> cancel a running session
        skitter list-agents                  -> list agents discovered from broker
        skitter get-agent <agent_id>         -> get agent discovery card (JSON)

    Global flags (before subcommand):
        --skitter-home <path>               -> override SKITTER_HOME
    """
    _parse_global_flags()
    configure_logging()
    subcmd = sys.argv[1] if len(sys.argv) > 1 else ""

    # Table-driven dispatch
    if subcmd in _DISPATCH:
        module_path, func_name, passes_argv = _DISPATCH[subcmd]
        import importlib

        mod = importlib.import_module(module_path)
        func = getattr(mod, func_name)
        if passes_argv:
            func(sys.argv[2:])
        else:
            func()
    elif subcmd in _MANAGE_CMDS:
        from skitter import manage

        getattr(manage, subcmd.replace("-", "_"))(sys.argv[2:])
    elif subcmd == "run":
        print(
            "'skitter run' has been renamed to 'skitter request'.\n"
            "Usage: skitter request <agent_id> '<prompt>'",
            file=sys.stderr,
        )
        sys.exit(1)
    elif subcmd in ("ask", "request"):
        agent_id, prompt, context_id = _parse_request_args(sys.argv[2:])
        if not agent_id or not prompt:
            print(
                f"Usage: skitter {subcmd} <agent_id> '<prompt>' [--context <id>]",
                file=sys.stderr,
            )
            sys.exit(1)
        if subcmd == "request":
            print(
                "Tip: 'skitter ask' is the recommended command.",
                file=sys.stderr,
            )
        from skitter.request import request_prompt

        request_prompt(agent_id, prompt, context_id=context_id)
    else:
        from skitter.coordinator import main

        main()


if __name__ == "__main__":
    dispatch()
