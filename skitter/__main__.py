import sys


def dispatch() -> None:
    """Entry point for the ``skitter`` CLI.

    Routes to the appropriate sub-module based on the first positional arg:

        skitter              → coordinator (default)
        skitter setup [...]  → interactive setup wizard
        skitter chat  [...]  → interactive MQTT chat client
    """
    if len(sys.argv) > 1 and sys.argv[1] == "setup":
        # Strip 'setup' from argv so Typer only sees its own flags.
        sys.argv = [sys.argv[0]] + sys.argv[2:]
        try:
            from skitter.setup import main
        except ImportError as exc:
            print(
                f"Error: Missing setup dependencies: {exc}\n"
                "Fix:  pip install typer rich paho-mqtt\n"
                " or:  pip install -e '.[all]'",
                file=sys.stderr,
            )
            sys.exit(1)
        main()
    elif len(sys.argv) > 1 and sys.argv[1] == "chat":
        from skitter.cli import main

        main()
    else:
        from skitter.coordinator import main

        main()


if __name__ == "__main__":
    dispatch()
