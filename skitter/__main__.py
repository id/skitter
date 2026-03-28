import sys

from skitter.manage import (
    cancel_session,
    create_app,
    delete_app,
    get_app,
    get_session,
    list_apps,
    list_sessions,
)

_MANAGE_CMDS = {
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
        skitter chat [...]                   → interactive MQTT chat client
        skitter run  [agent_id] '<prompt>'   → one-shot A2A request
        skitter agent-runner <file>          → standalone A2A agent process
        skitter create-agent <name> <prompt> → generate agent definition via LLM
        skitter create-app <name> <instr>    → create a composed multi-agent app
        skitter list-apps                    → list all apps
        skitter get-app <id>                 → get app details
        skitter delete-app <id>              → delete an app
        skitter list-sessions [app_id]       → list sessions
        skitter get-session <id>             → get session details
        skitter cancel-session <id>          → cancel a running session
        skitter pull [target_dir]            → pull agent cards from broker
    """
    subcmd = sys.argv[1] if len(sys.argv) > 1 else ""

    if subcmd == "chat":
        from skitter.cli import main

        main()
    elif subcmd == "agent-runner":
        from skitter.agent_runner import main as runner_main

        runner_main()
    elif subcmd == "pull":
        from skitter.pull import main as pull_main

        pull_main()
    elif subcmd == "create-agent":
        from skitter.create_agent import main as create_agent_main

        create_agent_main()
    elif subcmd in _MANAGE_CMDS:
        _MANAGE_CMDS[subcmd](sys.argv[2:])
    elif subcmd == "run":
        args = sys.argv[2:]
        if not args:
            print("Usage: skitter run [agent_id] '<prompt>'", file=sys.stderr)
            sys.exit(1)
        # Two+ args where first doesn't look like prose: treat as agent_id
        if len(args) >= 2 and " " not in args[0]:
            agent_id, prompt = args[0], " ".join(args[1:])
        else:
            agent_id, prompt = "skitter", " ".join(args)
        from skitter.run import run_prompt

        run_prompt(agent_id, prompt)
    else:
        from skitter.coordinator import main

        main()


if __name__ == "__main__":
    dispatch()
