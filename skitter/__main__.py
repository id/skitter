import asyncio
import sys
import uuid


def _run_prompt(agent_id: str, prompt: str) -> None:
    """Send a one-shot A2A request to an agent and print the reply."""
    from rich.console import Console

    from skitter.mqtt import send_and_wait, topic_request
    from skitter.types import (
        A2ARequest,
        REPLY_ERROR,
        REPLY_FAILED,
        REPLY_SUBMITTED,
        REPLY_TERMINAL,
        REPLY_TEXT,
        REPLY_TOOL,
    )

    console = Console()

    def on_reply(kind: str, content: str) -> bool:
        if kind == REPLY_SUBMITTED:
            console.print(f"[dim]Session: {content}[/dim]")
        elif kind == REPLY_TEXT:
            console.print(content, end="")
        elif kind == REPLY_TOOL:
            console.print(f"  [dim][tool] {content}[/dim]")
        elif kind == REPLY_TERMINAL:
            console.print(f"\n\n{content}")
            return True
        elif kind == REPLY_FAILED:
            console.print(f"[red]Failed: {content}[/red]")
            return True
        elif kind == REPLY_ERROR:
            console.print(f"[red]Error: {content}[/red]")
            return True
        elif kind == "timeout":
            console.print("[yellow]Timed out waiting for result[/yellow]")
            return True
        return False

    request_id = f"run-{uuid.uuid4().hex[:8]}"
    req = A2ARequest(text=prompt, request_id=request_id, sender="cli")

    asyncio.run(
        send_and_wait(
            topic_request(agent_id),
            req.to_json(),
            request_id,
            on_reply,
        )
    )


def dispatch() -> None:
    """Entry point for the ``skitter`` CLI.

    Routes to the appropriate sub-module based on the first positional arg:

        skitter                → coordinator (default)
        skitter chat  [...]    → interactive MQTT chat client
        skitter run   [...]    → one-shot A2A request
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
    elif subcmd == "run":
        prompt = " ".join(sys.argv[2:])
        if not prompt:
            print("Usage: skitter run '<prompt>'", file=sys.stderr)
            sys.exit(1)
        _run_prompt("skitter", prompt)
    elif subcmd in ("skitter", "supervisor"):
        from skitter.coordinator import main

        main()
    elif subcmd and not subcmd.startswith("-"):
        prompt = " ".join(sys.argv[1:])
        _run_prompt("skitter", prompt)
    else:
        from skitter.coordinator import main

        main()


if __name__ == "__main__":
    dispatch()
