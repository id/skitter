"""One-shot A2A request: send a prompt to an agent and print the reply."""

import asyncio
import uuid


def run_prompt(agent_id: str, prompt: str) -> None:
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
