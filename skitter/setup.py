"""Interactive setup wizard: skitter setup.

Collects runtime, LLM, and broker configuration, verifies connectivity,
and writes ~/.skitter/config.yaml.
"""

import asyncio
import getpass
import logging
import os
import sys
import uuid

import yaml

from skitter.config import config_file, detect_runtimes, skitter_home

log = logging.getLogger("skitter.setup")


def _prompt(msg: str, default: str = "", *, secret: bool = False) -> str:
    if secret:
        hint = " [****]" if default else ""
    else:
        hint = f" [{default}]" if default else ""
    try:
        if secret:
            value = getpass.getpass(f"{msg}{hint}: ").strip()
        else:
            value = input(f"{msg}{hint}: ").strip()
    except (EOFError, KeyboardInterrupt):
        print("\nAborted.", file=sys.stderr)
        sys.exit(1)
    if value == "-":
        return ""
    return value or default


def _prompt_choice(msg: str, choices: list[str], default: str = "") -> str:
    for i, c in enumerate(choices, 1):
        print(f"  {i}. {c}")
    while True:
        raw = _prompt(msg, default)
        if raw in choices:
            return raw
        try:
            idx = int(raw)
            if 1 <= idx <= len(choices):
                return choices[idx - 1]
        except ValueError:
            pass
        print(f"Please enter 1-{len(choices)} or one of: {', '.join(choices)}")


# ---------------------------------------------------------------------------
# Collect: runtimes
# ---------------------------------------------------------------------------


def _collect_runtimes(non_interactive: bool) -> dict[str, str | None]:
    print("\n--- Runtimes ---\n")
    runtimes = detect_runtimes()
    for name, path in runtimes.items():
        if path:
            print(f"  [found] {name}: {path}")
        else:
            print(f"  [not found] {name}")

    found = [n for n, p in runtimes.items() if p]
    if not found:
        print(
            "\nNo agent runtimes found. Install Claude Code or Codex CLI to run agents."
        )
    else:
        print(f"\nAvailable runtimes: {', '.join(found)}")
    return runtimes


# ---------------------------------------------------------------------------
# Collect: LLM provider (optional)
# ---------------------------------------------------------------------------


def _collect_llm(
    non_interactive: bool, standalone: bool, existing: dict
) -> dict | None:
    """Configure LLM provider for the coordinator. Returns None to skip."""
    print("\n--- Coordinator LLM ---\n")

    if standalone:
        print("Standalone mode: skipping coordinator LLM configuration.")
        return None

    default_models = {
        "anthropic": "claude-sonnet-4-6",
        "openai": "gpt-5.4-mini",
        "openai-completions": "gpt-5.4-mini",
    }

    if non_interactive:
        api = os.environ.get("SKITTER_LLM_API", "anthropic")
        model = os.environ.get(
            "SKITTER_LLM_MODEL", default_models.get(api, "claude-sonnet-4-6")
        )
        if not os.environ.get("SKITTER_LLM_API_KEY"):
            print("No LLM API key found; skipping coordinator configuration.")
            return None
        return {"model": model, "api": api}

    print("The coordinator uses an LLM for graph generation in composed apps.")
    print("If you only plan to use standalone agents, you can skip this.\n")

    if _prompt("Configure LLM provider? (y/n)", "y") != "y":
        return None

    print("\nWhich LLM API?")
    print("  (openai-completions: for 3rd-party OpenAI-compatible providers)")
    api = _prompt_choice(
        "API",
        ["anthropic", "openai", "openai-completions"],
        default=existing.get("api", "anthropic"),
    )

    model = _prompt("Model", existing.get("model", default_models.get(api, "")))

    api_key = os.environ.get("SKITTER_LLM_API_KEY", "") or existing.get("api_key", "")
    if not api_key:
        api_key = _prompt("API key", secret=True)
    if api_key:
        os.environ["SKITTER_LLM_API_KEY"] = api_key

    base_url = _prompt(
        "Base URL (optional, for custom endpoints)", existing.get("base_url", "")
    )

    result: dict = {"model": model, "api": api}
    if api_key:
        result["api_key"] = api_key
    if base_url:
        result["base_url"] = base_url
    return result


# ---------------------------------------------------------------------------
# Collect: broker
# ---------------------------------------------------------------------------


def _collect_broker(non_interactive: bool, existing: dict) -> dict:
    print("\n--- MQTT Broker ---\n")

    if non_interactive:
        return {"tier": "docker", "url": "mqtt://localhost:1883"}

    print("How do you want to run the MQTT broker?")
    existing_tier = existing.get("tier", "docker")
    tier = _prompt_choice(
        "Broker tier",
        ["docker", "public", "serverless", "custom"],
        default=existing_tier,
    )

    if tier == "docker":
        return {"tier": "docker", "url": "mqtt://localhost:1883"}

    if tier == "public":
        org_prefix = existing.get("org_prefix") or f"skitter-{uuid.uuid4().hex[:8]}"
        print("\nUsing public broker: mqtt://broker.emqx.io:1883")
        print(f"Org prefix: {org_prefix}")
        print(
            "WARNING: public broker has no authentication. Do not use for sensitive data."
        )
        return {
            "tier": "public",
            "url": "mqtt://broker.emqx.io:1883",
            "org_prefix": org_prefix,
        }

    if tier == "serverless":
        url = _prompt(
            "Broker URL (e.g. mqtts://xxx.emqxsl.com:8883)",
            existing.get("url", ""),
        )
        username = _prompt("Username", existing.get("username", ""))
        password = _prompt("Password", existing.get("password", ""), secret=True)
        ca_cert = _prompt("CA cert path (optional)", existing.get("ca_cert", ""))
        return {
            "tier": "serverless",
            "url": url,
            "username": username,
            "password": password,
            "ca_cert": ca_cert,
        }

    # custom
    url = _prompt(
        "Broker URL (e.g. mqtt://192.168.1.100:1883)",
        existing.get("url", ""),
    )
    if any(h in url for h in ("localhost", "127.0.0.1")):
        print(
            "WARNING: localhost is not reachable from Docker containers. "
            "Consider the 'docker' tier or use an externally routable address."
        )
    username = _prompt("Username (optional)", existing.get("username", ""))
    password = _prompt("Password (optional)", existing.get("password", ""), secret=True)
    return {
        "tier": "custom",
        "url": url,
        "username": username,
        "password": password,
    }


# ---------------------------------------------------------------------------
# Verify + write
# ---------------------------------------------------------------------------


def _verify(
    llm_cfg: dict | None, broker_cfg: dict, *, non_interactive: bool = False
) -> None:
    """Verify broker and LLM connectivity. Non-fatal: warns and offers to continue."""
    print("\n--- Verification ---\n")

    url = broker_cfg.get("url", "mqtt://localhost:1883")
    tier = broker_cfg.get("tier", "docker")

    if tier == "docker":
        print("Broker verification will happen when you run 'skitter up'.")
    else:
        print(f"Verifying broker connectivity ({url})...")
        try:
            from skitter.config import BrokerConfig
            from skitter.mqtt import mqtt_roundtrip

            broker = BrokerConfig(
                tier=tier,
                url=url,
                username=broker_cfg.get("username", ""),
                password=broker_cfg.get("password", ""),
                ca_cert=broker_cfg.get("ca_cert", ""),
            )
            asyncio.run(mqtt_roundtrip(broker=broker))
            print("Broker OK (publish/subscribe round-trip verified).")
        except Exception as e:
            print(f"Warning: broker verification failed: {e}", file=sys.stderr)
            if non_interactive:
                print("Continuing (non-interactive mode).")
            elif _prompt("Continue anyway? (y/n)", "n") != "y":
                sys.exit(1)

    if llm_cfg:
        from skitter.config import LLMConfig

        cfg = LLMConfig(
            model=llm_cfg.get("model", ""),
            api=llm_cfg.get("api", "anthropic"),
            base_url=llm_cfg.get("base_url", ""),
            api_key=llm_cfg.get("api_key", ""),
        )
        print(f"Validating LLM connectivity (model={cfg.model})...")
        try:
            from skitter.llm import check

            asyncio.run(check(cfg))
            print("LLM OK.")
        except Exception as e:
            print(f"Warning: LLM validation failed: {e}", file=sys.stderr)
            if non_interactive:
                print("Continuing (non-interactive mode).")
            elif _prompt("Continue anyway? (y/n)", "n") != "y":
                sys.exit(1)


def _write_config(llm_cfg: dict | None, broker_cfg: dict) -> None:
    home = skitter_home()
    home.mkdir(parents=True, exist_ok=True)
    (home / "agents").mkdir(exist_ok=True)

    config: dict = {
        "broker": {k: v for k, v in broker_cfg.items() if v},
    }
    if llm_cfg:
        config["llm"] = {k: v for k, v in llm_cfg.items() if v}

    if broker_cfg.get("org_prefix"):
        config["org"] = broker_cfg["org_prefix"]

    cfg_path = config_file()
    cfg_path.write_text(yaml.dump(config, default_flow_style=False, sort_keys=False))
    print(f"\nConfig written to {cfg_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> None:
    """Entry point for 'skitter setup'."""
    from skitter.config import load_raw_config

    argv = argv or sys.argv[2:]
    non_interactive = "--non-interactive" in argv
    standalone = "--standalone" in argv

    print("Welcome to skitter!")
    print("This wizard will set up your configuration.\n")

    existing = load_raw_config()

    if config_file().is_file() and not non_interactive:
        if _prompt(f"{config_file()} already exists. Overwrite? (y/n)", "n") != "y":
            print("Keeping existing config.")
            return

    runtimes = _collect_runtimes(non_interactive)
    llm_cfg = _collect_llm(non_interactive, standalone, existing.get("llm", {}) or {})
    broker_cfg = _collect_broker(non_interactive, existing.get("broker", {}) or {})

    while True:
        # Summary
        print("\n--- Summary ---\n")
        found = [n for n, p in runtimes.items() if p]
        print(f"  Runtimes:    {', '.join(found) if found else '(none found)'}")
        if llm_cfg:
            print(f"  LLM model:   {llm_cfg.get('model', '(not set)')}")
            print(f"  API:         {llm_cfg.get('api', 'anthropic')}")
        else:
            print("  Coordinator: skipped (standalone only)")
        print(f"  Broker tier: {broker_cfg.get('tier', 'docker')}")
        print(f"  Broker URL:  {broker_cfg.get('url', '')}")
        print(f"  Config path: {config_file()}")

        if non_interactive:
            break

        choice = _prompt("\nProceed? (y)es / (e)dit / (d)iscard", "y")
        if choice.lower().startswith("y"):
            break
        if choice.lower().startswith("e"):
            # Re-run collection with current values as defaults
            llm_existing = llm_cfg or {}
            broker_existing = broker_cfg or {}
            llm_cfg = _collect_llm(False, standalone, llm_existing)
            broker_cfg = _collect_broker(False, broker_existing)
            continue
        print("Discarded.")
        return

    _verify(llm_cfg, broker_cfg, non_interactive=non_interactive)
    _write_config(llm_cfg, broker_cfg)

    # Next steps
    print("\nSetup complete! Next steps:")
    if not found:
        print("  1. Install Claude Code or Codex CLI")
        print("  2. skitter create-agent <name> '<description>'")
    else:
        print("  1. skitter create-agent <name> '<description>'")
    print("  2. skitter up")
    print("  3. skitter ask <agent> 'hello'")
