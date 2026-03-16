# Deploying Skitter Coordinator on Fly.io

The coordinator is a pure A2A orchestrator. It listens for requests on
MQTT, manages sessions in a local SQLite DB, and dispatches tasks to
agents. It does not run CLI tools or manage agent processes.

Agent runners are deployed separately (on Fly, bare metal, or wherever
the user's CLI tools live).

## Prerequisites

- [Fly CLI](https://fly.io/docs/flyctl/install/) installed and authenticated (`fly auth login`)
- An MQTT v5 broker (e.g. [EMQX Serverless](https://www.emqx.com/en/cloud/serverless-mqtt))

## 1. Create the Fly app

```bash
fly apps create skitter
```

## 2. Set MQTT secrets

```bash
fly secrets set -a skitter \
  MQTT_HOST=your-broker.emqxsl.com \
  MQTT_PORT=8883 \
  MQTT_TLS=1 \
  MQTT_USER=skitter \
  MQTT_PASS=yourpass
```

## 3. Deploy

```bash
fly deploy -a skitter --ha=false
```

This builds the Docker image and creates a single always-on machine
running `python -m skitter coordinator`.

To redeploy after code changes, run `fly deploy` again.

## 4. Verify

Check logs:

```bash
fly logs -a skitter
```

You should see `Coordinator ready`.

Send a test request (requires an agent running on the same broker):

```bash
mosquitto_pub \
  -h "$MQTT_HOST" -p 8883 -u "$MQTT_USER" -P "$MQTT_PASS" \
  --cafile emqxsl-ca.crt -V 5 \
  -t '$a2a/v1/request/skitter/default/skitter' \
  -D publish response-topic '$a2a/v1/reply/skitter/default/test' \
  -D publish correlation-data 'test-001' \
  -m '{"jsonrpc":"2.0","method":"tasks/send","id":"test-001","params":{"message":{"role":"user","parts":[{"type":"text","text":"Say hello"}]}}}'
```

## Cost

- **Coordinator**: `shared-cpu-1x` / 256MB ~ $2/mo
- EMQX Serverless free tier: 1M session minutes/month
