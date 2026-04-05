from skitter.commands import cli


def dispatch() -> None:
    """Entry point for the ``skitter`` CLI."""
    cli(standalone_mode=True)


if __name__ == "__main__":
    dispatch()
