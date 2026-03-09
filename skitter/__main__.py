import sys


def dispatch() -> None:
    """Entry point for the ``skitter`` CLI.

    Routes to the appropriate sub-module based on the first positional arg:

        skitter                → supervisor (default)
        skitter chat  [...]    → interactive MQTT chat client
        skitter agents [...]   → manage predefined agents
        skitter workflow [...] → manage and run workflows
        skitter init           → create ~/.skitter/ with example files
    """
    subcmd = sys.argv[1] if len(sys.argv) > 1 else ""

    if subcmd == "chat":
        from skitter.cli import main

        main()
    elif subcmd == "agents":
        from skitter.agents_cli import main

        main()
    elif subcmd == "workflow":
        from skitter.workflow_cli import main

        main()
    elif subcmd == "docker":
        from skitter.docker_cli import main

        main()
    elif subcmd == "publish":
        from skitter.discovery import main as discovery_main

        discovery_main()
    elif subcmd == "deploy":
        from skitter.deploy_fly import cmd_deploy_fly

        cmd_deploy_fly()
    elif subcmd == "init":
        from skitter.config import write_examples

        agents_written, workflows_written = write_examples()
        if agents_written:
            print(f"Created agents: {', '.join(agents_written)}")
        if workflows_written:
            print(f"Created workflows: {', '.join(workflows_written)}")
        if not agents_written and not workflows_written:
            print("All example files already exist. Nothing to do.")
        else:
            print("Done. Files are in ~/.skitter/")
    elif subcmd == "run":
        # skitter run "prompt" — one-shot default agent
        from skitter.agents_cli import cmd_run

        prompt = " ".join(sys.argv[2:])
        if not prompt:
            print("Usage: skitter run '<prompt>'", file=sys.stderr)
            sys.exit(1)
        cmd_run("skitter", prompt)
    elif subcmd == "supervisor":
        from skitter.supervisor import main

        main()
    elif subcmd and not subcmd.startswith("-"):
        # skitter "prompt" — treat unrecognized subcommand as prompt
        from skitter.agents_cli import cmd_run

        prompt = " ".join(sys.argv[1:])
        cmd_run("skitter", prompt)
    else:
        from skitter.supervisor import main

        main()


if __name__ == "__main__":
    dispatch()
