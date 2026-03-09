# Deploying Skitter on Fly.io

This guide covers deploying skitter to Fly Machines with EMQX Serverless as the MQTT broker. The supervisor runs as a single always-on machine (~$2/mo); workers are ephemeral machines that auto-destroy after completing their task.

## Prerequisites

- [Fly CLI](https://fly.io/docs/flyctl/install/) installed and authenticated (`fly auth login`)
- [EMQX Serverless](https://www.emqx.com/en/cloud/serverless-mqtt) account (free tier works)
- `mosquitto_pub` / `mosquitto_sub` for testing (`brew install mosquitto` on macOS)

## Architecture

```
EMQX Serverless (managed MQTT broker)
  |
  | Supervisor subscribes to request/+
  |
  v
Fly Machine: supervisor (always-on, ~30MB RAM)
  | Creates sessions, publishes to MQTT
  | Spawns worker machines via Fly API
  | Publishes discovery cards
  v
Fly Machine: worker (ephemeral, 5-60s)
  | Reads session from MQTT
  | Runs claude/codex CLI
  | Publishes retained result + reply to caller
  | Exits -> auto-destroys
```

The supervisor uses almost no CPU — it just subscribes to MQTT topics and creates machines when requests arrive. Workers are billed per-second while running. For sporadic personal use, cost is effectively $0/mo beyond the supervisor.

## 1. Create EMQX Serverless Deployment

1. Go to [EMQX Cloud Console](https://cloud.emqx.com/) and create a **Serverless** deployment
2. Note the connection details:
   - **Host**: something like `z8d812da.ala.eu-central-1.emqxsl.com`
   - **Port**: `8883` (TLS)
3. Under **Authentication**, create a username/password pair

### Verify MQTT connectivity

```bash
# Subscribe (in one terminal)
mosquitto_sub \
  -h z8d812da.ala.eu-central-1.emqxsl.com \
  -p 8883 -u skitter -P yourpass \
  --cafile emqxsl-ca.crt -V 5 \
  -t 'test/hello'

# Publish (in another terminal)
mosquitto_pub \
  -h z8d812da.ala.eu-central-1.emqxsl.com \
  -p 8883 -u skitter -P yourpass \
  --cafile emqxsl-ca.crt -V 5 \
  -t 'test/hello' -m 'it works'
```

## 2. Create Fly App

```bash
fly apps create skitter
```

## 3. Configure Environment

```bash
cp .env.cloud.example .env.cloud
```

Fill in your actual values:

```bash
# .env.cloud
MQTT_HOST=z8d812da.ala.eu-central-1.emqxsl.com
MQTT_PORT=8883
MQTT_TLS=1
MQTT_USER=skitter
MQTT_PASS=yourpass

FLY_API_TOKEN=your-fly-token    # from `fly tokens create deploy`
FLY_APP=skitter
FLY_REGION=iad                  # or your preferred region

# Worker auth — either API key or OAuth token (not both needed)
ANTHROPIC_API_KEY=sk-ant-...           # API credits
CLAUDE_CODE_OAUTH_TOKEN=sk-ant-oat01-...  # or OAuth (uses your Pro/Max subscription)
```

### OAuth vs API key

Workers can authenticate with Claude in two ways:

- **`ANTHROPIC_API_KEY`** — standard API key, billed per-token
- **`CLAUDE_CODE_OAUTH_TOKEN`** — OAuth token, uses your Pro/Max subscription

To get an OAuth token:

```bash
claude setup-token
# Follow the prompts, then copy the token
```

Set it in `.env.cloud` or as a Fly secret:

```bash
fly secrets set CLAUDE_CODE_OAUTH_TOKEN=sk-ant-oat01-... -a skitter
```

Claude CLI reads `CLAUDE_CODE_OAUTH_TOKEN` directly — no credential files needed.

## Persistent Workspaces (Google Drive via rclone)

By default, workers use ephemeral directories that are destroyed after each run. **Persistent workspaces** let workflows read and write files that survive across runs — e.g. a customer list, accumulated reports, or shared datasets.

Files are stored on Google Drive (or any rclone remote). Fly workers sync files down before running and sync back after. Locally, the Google Drive desktop app provides zero-cost filesystem access with no rclone needed.

### How it works per spawn mode

| Mode | Behavior |
|---|---|
| **subprocess** | Uses Google Drive folder directly via filesystem path. No rclone needed — the desktop app handles sync. |
| **docker** | rclone config is mounted as a read-only volume. Workers sync before/after. |
| **fly** | rclone config is written from `RCLONE_CONFIG_DATA` env var at container startup. Workers sync before/after. |

### 1. Configure rclone

```bash
# Install rclone and set up a Google Drive remote
rclone config
# Create a remote named "drive" (type: Google Drive, follow OAuth flow)
```

### 2. Configure skitter

Add a `workspace` section to `~/.skitter/config.yaml`:

```yaml
default_runtime: claude

workspace:
  remote: drive                     # rclone remote name
  local_mount: /Users/you/Library/CloudStorage/GoogleDrive-.../My Drive
  base_path: skitter/workspaces     # base path on remote
```

- **`remote`** — the rclone remote name (required for docker/fly modes)
- **`local_mount`** — path to your local Google Drive mount (subprocess mode uses this directly, skipping rclone entirely)
- **`base_path`** — directory on the remote under which all workspaces live

### 3. Add workspace to a workflow

```yaml
name: Weekly Report
workspace: weekly-report            # slug, relative to base_path
tasks:
  - id: gather
    agent: researcher
    description: "Gather data from files in the workspace"
    next: summarize
  - id: summarize
    agent: writer
    description: "Write a summary report"
    needs: [gather]
    next: output
```

Each task gets its own subdirectory (`weekly-report/gather/`, `weekly-report/summarize/`) for parallel safety. Join tasks read sibling directories via their agent prompt.

### 4. Deploy with rclone config (Fly only)

Export your rclone config as a Fly secret:

```bash
RCLONE_CONFIG_DATA="$(cat ~/.config/rclone/rclone.conf)"
fly secrets set RCLONE_CONFIG_DATA="$RCLONE_CONFIG_DATA" -a skitter
```

Or add it to `.env.cloud` and re-deploy — `skitter deploy` sets it automatically.

### Workspace behavior

- Workflows **without** `workspace:` keep using ephemeral directories. No breaking changes.
- Workers sync down before running, sync up after. Results are published only after sync completes.
- First run on a new workspace creates the remote directory automatically.
- If sync-up fails after the agent runs, the result includes a warning so you know files may not have persisted.
- Keep workspaces under ~500MB. For larger datasets, request more disk via Fly's `guest.disk` option.

## 4. Deploy

```bash
set -a && source .env.cloud && set +a
uv run python -m skitter deploy
```

This:
1. Sets secrets on the Fly app (MQTT creds, auth, Fly token)
2. Builds a Docker image with agent definitions baked in
3. Deploys the supervisor as an always-on machine
4. Sets `FLY_WORKER_IMAGE` secret to the deployed image ref

After making changes to agent definitions (`~/.claude/agents/*.md` or `~/.skitter/agents/*.yaml`), re-run the deploy command. The supervisor machine is updated in-place.

To update discovery cards without redeploying (e.g. after editing workflow YAML locally):

```bash
set -a && source .env.cloud && set +a
uv run python -m skitter publish
```

## 5. Test End-to-End

The supervisor is already running and listening. Publish a request:

```bash
set -a && source .env.cloud && set +a

mosquitto_pub \
  -h "$MQTT_HOST" -p "$MQTT_PORT" -u "$MQTT_USER" -P "$MQTT_PASS" \
  --cafile emqxsl-ca.crt -V 5 \
  -t '$a2a/v1/request/skitter/default/skitter' \
  -D publish response-topic '$a2a/v1/reply/skitter/default/test' \
  -D publish correlation-data 'test-001' \
  -m '{"jsonrpc":"2.0","method":"tasks/send","id":"test-001","params":{"message":{"role":"user","parts":[{"type":"text","text":"Say hello! Reply in one sentence."}]}}}'
```

Watch replies:

```bash
mosquitto_sub \
  -h "$MQTT_HOST" -p "$MQTT_PORT" -u "$MQTT_USER" -P "$MQTT_PASS" \
  --cafile emqxsl-ca.crt -V 5 \
  -t '$a2a/v1/reply/skitter/default/test' -v
```

Watch all traffic:

```bash
mosquitto_sub \
  -h "$MQTT_HOST" -p "$MQTT_PORT" -u "$MQTT_USER" -P "$MQTT_PASS" \
  --cafile emqxsl-ca.crt -V 5 \
  -t '$a2a/v1/request/#' -t '$a2a/v1/reply/#' -t 'skitter/#' -v
```

## How It Works

1. Client publishes a JSON-RPC request to `$a2a/v1/request/{org}/{unit}/{agent_id}`
2. Supervisor (always listening) creates a session, publishes it as retained MQTT, spawns worker machines via Fly API
3. Worker machine starts (~10s cold boot), reads the session, runs `claude --agent <name>`, publishes the result, exits
4. Worker machine auto-destroys after exit

For workflows with multiple tasks, the supervisor spawns all workers upfront. Workers with dependencies wait for upstream chain results (retained MQTT messages) before starting their work.

### Crash recovery

Worker machines use `restart.policy: on-failure` with `max_retries: 1`. If a worker crashes, Fly restarts it automatically — the supervisor does not handle dead events on Fly (LWT fires on normal exit too due to `auto_destroy`, which would cause an infinite respawn loop). The restarted worker reads the retained session from MQTT and picks up where it left off.

## Cost

- **Supervisor**: always-on `shared-cpu-1x` / 256MB ~ $1.94/mo
- **Workers**: billed per-second while running (`shared-cpu-1x` / 1024MB ~ $0.0000066/sec)
- No persistent volumes (agent definitions baked into image)
- EMQX Serverless free tier: 1M session minutes/month, 1GB traffic
- For sporadic personal use, total cost is ~$2/mo

## Troubleshooting

### Check Fly logs

```bash
fly logs -a skitter
```

### Check machine status

```bash
fly machines list -a skitter
```

### Common issues

| Symptom | Cause | Fix |
|---|---|---|
| No response to requests | Supervisor not running | `fly machines list -a skitter` — should show one running machine |
| Worker OOM killed | Not enough memory | Increase `memory_mb` in `skitter/fly.py` (default: 1024MB) |
| `MANIFEST_UNKNOWN` on worker create | Stale image tag | Re-run `skitter deploy` |
| Worker exits with "Credit balance too low" | Anthropic API quota | Add credits, or switch to OAuth (`CLAUDE_CODE_OAUTH_TOKEN`) |
| Result contains "workspace sync to remote failed" | rclone config missing or expired | Re-set `RCLONE_CONFIG_DATA` secret with a fresh `rclone.conf` |
| `sync_down failed` error in worker logs | rclone remote unreachable | Check rclone config, network, and Google Drive OAuth token expiry |
