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
pnpm dev
```

The Vite dev server runs at http://localhost:18084/skitter/. In local development it proxies `/mqtt` to `ws://localhost:8083` and `/api` to `http://localhost:3000`; override these with the `VITE_PROXY_MQTT` and `VITE_PROXY_API` environment variables.

Default connection settings:

- Broker URL: same origin (works behind the dev proxy or a reverse proxy)
- Organization: `default`
- Unit: `default`

The settings dialog persists broker URL / org / unit overrides in `localStorage`.

## Checks

```bash
pnpm lint
pnpm build
```
