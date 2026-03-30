"""One-shot A2A request: send a prompt to an agent and print the reply."""

import asyncio
import uuid


def request_prompt(agent_id: str, prompt: str) -> None:
    """Send a one-shot A2A request to an agent and print the reply."""
    from rich.console import Console

    from skitter.a2a import (
        A2ARequest,
        print_reply,
        stream_replies,
        topic_request,
    )

    console = Console()

    async def _stream() -> None:
        request_id = f"req-{uuid.uuid4().hex[:8]}"
        req = A2ARequest(text=prompt, request_id=request_id, sender="cli")

        async for kind, content in stream_replies(
            topic_request(agent_id), req.to_json(), request_id
        ):
            print_reply(console, kind, content)

    asyncio.run(_stream())
