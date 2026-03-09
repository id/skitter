# Deploying Skitter on Fly.io

This guide covers deploying skitter to Fly Machines with EMQX Serverless as the MQTT broker. The result is a fully serverless setup: nothing runs when idle, machines spin up on demand per request, and auto-destroy after completing their work.

## Prerequisites

- [Fly CLI](https://fly.io/docs/flyctl/install/) installed and authenticated (`fly auth login`)
- [EMQX Serverless](https://www.emqx.com/en/cloud/serverless-mqtt) account (free tier works)
- Claude Code logged in locally (`claude` CLI must work)
- `mosquitto_pub` / `mosquitto_sub` (for testing; `brew install mosquitto` on macOS)

## Architecture

```
EMQX Serverless (always-on managed service)
  |
  | Rule engine intercepts $a2a/v1/request/+/+/+
  |
  +-- Action 1: Republish payload (retained) to pending topic
  +-- Action 2: POST to Fly Machines API -> create supervisor
  |
  v
Fly Machine: supervisor (ephemeral, ~3s)
  | Reads pending request from MQTT
  | Creates session, publishes to MQTT
  | Spawns worker machines via Fly API
  | Exits -> auto-destroys
  v
Fly Machine: worker (ephemeral, 5-60s)
  | Reads session from MQTT
  | Runs claude/codex CLI
  | Publishes result to MQTT
  | Exits -> auto-destroys
```

All machines use `auto_destroy: true` and `restart.policy: no`. When idle, nothing is running and there is zero cost.

## 1. Create EMQX Serverless Deployment

1. Go to [EMQX Cloud Console](https://cloud.emqx.com/) and create a **Serverless** deployment
2. Note the connection details:
   - **Host**: something like `z8d812da.ala.eu-central-1.emqxsl.com`
   - **Port**: `8883` (TLS)
3. Download the **CA certificate** (you'll need it for local testing with `mosquitto`)
4. Under **Authentication**, create a username/password pair

### Get EMQX API credentials

1. In the EMQX Cloud Console, go to your deployment
2. Navigate to **API Key** (left sidebar)
3. Create a new API key — note the key and secret

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

A single Fly app hosts both supervisor and worker machines. They share the same Docker image but run with different entrypoints and env vars.

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

EMQX_API_URL=https://z8d812da.ala.eu-central-1.emqxsl.com:8443/api/v5
EMQX_API_KEY=your-api-key
EMQX_API_SECRET=your-api-secret

FLY_API_TOKEN=your-fly-token    # from `fly tokens create deploy`
FLY_APP=skitter
FLY_REGION=iad                  # or your preferred region

ANTHROPIC_API_KEY=sk-ant-...
```

## 4. Deploy

```bash
set -a && source .env.cloud && set +a
uv run python -m skitter deploy --target fly
```

This:
1. Sets secrets on the Fly app (MQTT creds, API keys, Fly token)
2. Builds a Docker image with your agent definitions baked in
3. Pushes the image to Fly's registry
4. Cleans up the deploy-created machine (Fly's `deploy` creates a running machine as a side effect)
5. Publishes discovery cards via EMQX REST API

After making changes to agent definitions (`~/.claude/agents/*.md` or `~/.skitter/agents/*.yaml`), re-run the deploy command to bake the updated definitions into the image.

## 5. Configure EMQX Rule Engine

This is the key piece that connects everything. The EMQX rule engine intercepts MQTT requests and triggers Fly machine creation.

In the EMQX Cloud Console, go to **Data Integration > Rules > Create**.

### Rule SQL

```sql
SELECT
  topic,
  payload,
  json_decode(payload).id as session_id
FROM "$a2a/v1/request/+/+/+"
WHERE topic <> '$a2a/v1/request/skitter/default/supervisor'
```

This fires on any agent request and extracts the JSON-RPC `id` field as the session ID. The WHERE clause excludes supervisor-internal topics.

### Action 1: Republish (stage the request)

This stores the request payload as a retained MQTT message so the supervisor can read it on startup.

- **Type**: Republish
- **Topic**: `$a2a/v1/event/skitter/default/supervisor/pending/${session_id}`
- **Payload**: `${payload}`
- **QoS**: 1
- **Retain**: true

### Action 2: HTTP Server (create supervisor machine)

This calls the Fly Machines API to create an ephemeral supervisor.

- **Type**: HTTP Server
- **URL**: `https://api.machines.dev/v1/apps/skitter/machines`
- **Method**: POST
- **Headers**:
  - `Authorization`: `Bearer <your-fly-api-token>`
  - `Content-Type`: `application/json`
- **Body**:

```json
{
  "config": {
    "image": "registry.fly.io/skitter:latest",
    "auto_destroy": true,
    "restart": {"policy": "no"},
    "env": {
      "SESSION_ID": "${session_id}",
      "REQUEST_TOPIC": "${topic}",
      "SKITTER_SPAWN_MODE": "fly"
    },
    "init": {
      "entrypoint": ["python", "-m", "skitter.supervisor", "--ephemeral"]
    },
    "guest": {"cpu_kind": "shared", "cpus": 1, "memory_mb": 512}
  },
  "region": "iad"
}
```

Replace `iad` with your preferred Fly region, and the Fly API token in the Authorization header.

## 6. Test End-to-End

Once the rule engine is configured, any MQTT v5 client can trigger the full flow. Publish a request to an agent's topic:

```bash
set -a && source .env.cloud && set +a

mosquitto_pub \
  -h "$MQTT_HOST" -p "$MQTT_PORT" -u "$MQTT_USER" -P "$MQTT_PASS" \
  --cafile emqxsl-ca.crt -V 5 \
  -t '$a2a/v1/request/skitter/default/skitter' \
  -D publish response-topic '$a2a/v1/reply/skitter/default/test' \
  -D publish correlation-data 'test-001' \
  -m '{"jsonrpc":"2.0","method":"message/send","id":"test-001","params":{"message":{"role":"user","parts":[{"kind":"text","text":"Say hello! Reply in one sentence."}]}}}'
```

Then subscribe to watch results:

```bash
mosquitto_sub \
  -h "$MQTT_HOST" -p "$MQTT_PORT" -u "$MQTT_USER" -P "$MQTT_PASS" \
  --cafile emqxsl-ca.crt -V 5 \
  -t '$a2a/v1/reply/skitter/default/test' -v
```

You can also watch all traffic:

```bash
mosquitto_sub \
  -h "$MQTT_HOST" -p "$MQTT_PORT" -u "$MQTT_USER" -P "$MQTT_PASS" \
  --cafile emqxsl-ca.crt -V 5 \
  -t '$a2a/v1/request/#' -t '$a2a/v1/reply/#' -t '$a2a/v1/event/#' -v
```

## How It Works

1. Client publishes a JSON-RPC request to `$a2a/v1/request/{org}/{unit}/{agent_id}`
2. EMQX rule engine fires, republishes the payload as a retained message to a pending topic, and POSTs to Fly Machines API
3. Fly creates an ephemeral supervisor machine (~3s cold boot)
4. Supervisor connects to EMQX, reads the pending message, creates a session (published as retained MQTT), spawns worker machines via Fly API, exits
5. Worker machine starts (~5s cold boot), reads the session from MQTT, runs `claude --agent <name>`, publishes the result, exits
6. Both machines auto-destroy after exit

For workflows with multiple tasks, the supervisor spawns all workers upfront. Workers with dependencies wait for upstream chain results (retained MQTT messages) before starting their work.

## Cost

- Machines are billed per-second while running (shared-cpu-1x/512MB ~ $0.0000044/sec)
- Nothing runs when idle — zero cost
- No persistent volumes (agent definitions baked into image)
- 100GB/mo free outbound bandwidth
- For sporadic personal use, cost is effectively $0/mo
- EMQX Serverless free tier: 1M session minutes/month, 1GB traffic

## Troubleshooting

### Check Fly logs

```bash
fly logs -a skitter --no-tail
```

### Check machine status

```bash
fly machines list -a skitter --json
```

### Common issues

| Symptom | Cause | Fix |
|---|---|---|
| Supervisor times out reading pending | Topic mismatch (org/unit) | Ensure `SKITTER_A2A_ORG`/`SKITTER_A2A_UNIT` match between rule engine topics and Fly secrets |
| Worker OOM killed | Not enough memory | Increase `memory_mb` in rule engine webhook body and in `skitter/fly.py` |
| `MANIFEST_UNKNOWN` on machine create | Stale image tag | Re-run `skitter deploy --target fly`; the deploy sets `FLY_WORKER_IMAGE` secret automatically |
| Worker exits with "Credit balance too low" | Anthropic API quota | Add credits to your Anthropic account |
| `args.fly` AttributeError | Stale deployed image | Re-deploy; the code was updated to use `--ephemeral` |
