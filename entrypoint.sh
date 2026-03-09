#!/bin/bash
# Write OAuth credentials from env var if present (avoids baking secrets into image)
# Unset ANTHROPIC_API_KEY so Claude CLI uses OAuth instead of API key
if [ -n "$CLAUDE_CREDENTIALS" ]; then
    mkdir -p ~/.claude
    install -m 600 /dev/null ~/.claude/.credentials.json
    printf '%s' "$CLAUDE_CREDENTIALS" > ~/.claude/.credentials.json
    unset ANTHROPIC_API_KEY
fi

exec "$@"
