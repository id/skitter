import sys

from skitter.manage import (
    cancel_session,
    create_app,
    delete_app,
    get_agent,
    get_app,
    get_session,
    list_agents,
    list_apps,
    list_sessions,
)

_MANAGE_CMDS = {
    "list-agents": list_agents,
    "get-agent": get_agent,
    "create-app": create_app,
    "list-apps": list_apps,
    "get-app": get_app,
    "delete-app": delete_app,
    "list-sessions": list_sessions,
    "get-session": get_session,
    "cancel-session": cancel_session,
}


def dispatch() -> None:
    """Entry point for the ``skitter`` CLI.

    Routes to the appropriate sub-module based on the first positional arg:

        skitter                              → coordinator (default)
        skitter chat <agent_id>              → interactive A2A session
        skitter request <agent_id> '<prompt>' → one-shot A2A request
        skitter agent-runner <file>          → standalone A2A agent process
        skitter create-agent <name> <prompt> → generate agent definition via LLM
        skitter create-app <name> <instr>    → create a composed multi-agent app
        skitter list-apps                    → list all apps
        skitter get-app <id>                 → get app details
        skitter delete-app <id>              → delete an app
        skitter list-sessions [app_id]       → list sessions
        skitter get-session <id>             → get session details
        skitter cancel-session <id>          → cancel a running session
        skitter list-agents                   → list agents discovered from broker
        skitter get-agent <agent_id>          → get agent discovery card (JSON)
    """
    subcmd = sys.argv[1] if len(sys.argv) > 1 else ""

    if subcmd == "chat":
        from skitter.cli import main

        main()
    elif subcmd == "agent-runner":
        from skitter.agent_runner import main as runner_main

        runner_main()
    elif subcmd == "create-agent":
        from skitter.create_agent import main as create_agent_main

        create_agent_main()
    elif subcmd in _MANAGE_CMDS:
        _MANAGE_CMDS[subcmd](sys.argv[2:])
    elif subcmd == "run":
        print(
            "'skitter run' has been renamed to 'skitter request'.\n"
            "Usage: skitter request <agent_id> '<prompt>'",
            file=sys.stderr,
        )
        sys.exit(1)
    elif subcmd == "request":
        args = sys.argv[2:]
        if len(args) < 2:
            print("Usage: skitter request <agent_id> '<prompt>'", file=sys.stderr)
            sys.exit(1)
        agent_id, prompt = args[0], " ".join(args[1:])
        from skitter.request import request_prompt

        request_prompt(agent_id, prompt)
    else:
        from skitter.coordinator import main

        main()


if __name__ == "__main__":
    dispatch()
