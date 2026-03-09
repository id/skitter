#!/bin/bash
set -e

# Write rclone config from env var (set as Fly secret).
# Named RCLONE_CONFIG_DATA to avoid collision with rclone's built-in
# RCLONE_CONFIG env var (which rclone interprets as a file path).
if [ -n "$RCLONE_CONFIG_DATA" ]; then
    mkdir -p ~/.config/rclone
    printf '%s' "$RCLONE_CONFIG_DATA" > ~/.config/rclone/rclone.conf
    chmod 600 ~/.config/rclone/rclone.conf
    unset RCLONE_CONFIG_DATA
fi

exec "$@"
