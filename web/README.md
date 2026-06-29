# Skitter Dashboard

React + TypeScript dashboard for a minimal Skitter A2A-over-MQTT linkage demo.

## What It Does

- Connects to MQTT over WebSocket.
- Reads retained A2A Agent Cards into a compact target list.
- Sends direct `SendMessage` requests to agents and composed apps.
- Drives workflow creation and execution through chat with the `skitter` runtime agent.
- Keeps debug details out of the primary flow.

## Development

```bash
pnpm install
pnpm dev --host 127.0.0.1
```

The Vite dev server listens on port `18084`.

Default broker settings:

- Broker URL: `ws://162.14.117.182:8083/mqtt`
- Organization: `default`
- Unit: `default`

The settings page persists overrides in `localStorage`.

## Checks

```bash
pnpm lint
pnpm build
```
