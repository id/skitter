import asyncio
import sys
import uuid


def _run_prompt(agent_id: str, prompt: str) -> None:
    """Send a one-shot A2A request to an agent and print the reply."""
    from rich.console import Console

    from skitter.a2a import (
        A2ARequest,
        REPLY_ARTIFACT,
        REPLY_ERROR,
        REPLY_FAILED,
        REPLY_INPUT_REQUIRED,
        REPLY_SUBMITTED,
        REPLY_TEXT,
        REPLY_TIMEOUT,
        REPLY_TOOL,
        stream_replies,
        topic_request,
    )

    console = Console()

    async def _stream() -> None:
        request_id = f"run-{uuid.uuid4().hex[:8]}"
        req = A2ARequest(text=prompt, request_id=request_id, sender="cli")

        async for kind, content in stream_replies(
            topic_request(agent_id), req.to_json(), request_id
        ):
            if kind == REPLY_SUBMITTED:
                console.print(f"[dim]Session: {content}[/dim]")
            elif kind == REPLY_TEXT:
                console.print(content, end="")
            elif kind == REPLY_TOOL:
                console.print(f"  [dim][tool] {content}[/dim]")
            elif kind == REPLY_ARTIFACT:
                console.print(f"\n\n{content}")
            elif kind == REPLY_INPUT_REQUIRED:
                console.print(f"[yellow]Input required: {content}[/yellow]")
            elif kind == REPLY_FAILED:
                console.print(f"[red]Failed: {content}[/red]")
            elif kind == REPLY_ERROR:
                console.print(f"[red]Error: {content}[/red]")
            elif kind == REPLY_TIMEOUT:
                console.print("[yellow]Timed out waiting for result[/yellow]")

    asyncio.run(_stream())


def dispatch() -> None:
    """Entry point for the ``skitter`` CLI.

    Routes to the appropriate sub-module based on the first positional arg:

        skitter                              → coordinator (default)
        skitter chat [...]                   → interactive MQTT chat client
        skitter run  [agent_id] '<prompt>'   → one-shot A2A request (default: skitter)
        skitter agent-runner <file>          → standalone A2A agent process
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
        _run_prompt(agent_id, prompt)
    else:
        from skitter.coordinator import main

        main()


if __name__ == "__main__":
    dispatch()
