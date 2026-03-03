import sys


def dispatch() -> None:
    """Entry point for the ``skitter`` CLI.

    Routes to the appropriate sub-module based on the first positional arg:

        skitter                → coordinator (default)
        skitter chat  [...]    → interactive MQTT chat client
        skitter agents [...]   → manage predefined agents
        skitter pipeline [...] → manage and run pipelines
        skitter init           → create ~/.skitter/ with example files
    """
    subcmd = sys.argv[1] if len(sys.argv) > 1 else ""

    if subcmd == "chat":
        from skitter.cli import main

        main()
    elif subcmd == "agents":
        from skitter.agents_cli import main

        main()
    elif subcmd == "pipeline":
        from skitter.pipeline_cli import main

        main()
    elif subcmd == "init":
        from skitter.config import write_examples

        agents_written, pipelines_written = write_examples()
        if agents_written:
            print(f"Created agents: {', '.join(agents_written)}")
        if pipelines_written:
            print(f"Created pipelines: {', '.join(pipelines_written)}")
        if not agents_written and not pipelines_written:
            print("All example files already exist. Nothing to do.")
        else:
            print("Done. Files are in ~/.skitter/")
    else:
        from skitter.coordinator import main

        main()


if __name__ == "__main__":
    dispatch()
