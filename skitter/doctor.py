"""Diagnostic health checks: skitter doctor.

Checks config, LLM, broker, agents, coordinator, and database.
"""

import asyncio
import logging
import sys

from rich.console import Console

from skitter.config import SkitterConfig, config_file, load_config, load_raw_config

log = logging.getLogger("skitter.doctor")


class _Doctor:
    def __init__(self) -> None:
        self.console = Console()
        self.ok = 0
        self.warn = 0
        self.fail = 0
        self.cfg: SkitterConfig | None = None

    def _ok(self, msg: str) -> None:
        self.console.print(f"  [green][OK][/green] {msg}")
        self.ok += 1

    def _warn(self, msg: str) -> None:
        self.console.print(f"  [yellow][WARN][/yellow] {msg}")
        self.warn += 1

    def _fail(self, msg: str) -> None:
        self.console.print(f"  [red][FAIL][/red] {msg}")
        self.fail += 1

    def check_config(self) -> bool:
        cfg_path = config_file()
        try:
            load_raw_config(strict=True)
        except FileNotFoundError:
            self._fail(f"Config: {cfg_path} not found. Run 'skitter setup' first.")
            return False
        except Exception as e:
            self._fail(f"Config: {cfg_path} parse error: {e}")
            return False
        self.cfg = load_config()
        self._ok(f"Config: {cfg_path} found and valid")
        return True

    def check_llm(self) -> None:
        if not self.cfg.llm.model:
            self._warn("LLM: no model configured")
            return

        try:
            from skitter.llm import check

            asyncio.run(check())
            self._ok(f"LLM: {self.cfg.llm.model} responds")
        except Exception as e:
            self._fail(f"LLM: {self.cfg.llm.model} failed: {e}")

    def check_broker(self) -> bool:
        from skitter.services import _verify_broker_connectivity

        url = self.cfg.broker.url

        try:
            _verify_broker_connectivity(url)
            self._ok(f"Broker: {url} reachable")
        except ConnectionError as e:
            self._fail(f"Broker: {url} unreachable: {e}")
            return False

        try:
            from skitter.mqtt import mqtt_roundtrip

            asyncio.run(mqtt_roundtrip())
            self._ok("Broker: publish/subscribe round-trip OK")
            return True
        except Exception as e:
            self._fail(f"Broker: pub/sub round-trip failed: {e}")
            return False

    def check_services(self) -> None:
        from skitter.services import check_running

        docker_ok, coord_running, agent_status = check_running()

        if not docker_ok:
            self._warn("Docker: not available (skipping container checks)")
            return

        if coord_running:
            self._ok("Coordinator: running")
        else:
            self._warn("Coordinator: not running. Run 'skitter up' to start services.")

        if not agent_status:
            self._warn("Agents: no definitions found in ~/.skitter/agents/")
            return

        for agent_id, running in agent_status.items():
            if running:
                self._ok(f"Agent: {agent_id} running")
            else:
                self._warn(
                    f"Agent: {agent_id} defined but not running. "
                    f"Run 'skitter up --agent {agent_id}' to start."
                )

    def check_database(self) -> None:
        db_path = self.cfg.db.sqlite_path
        if db_path and db_path != ":memory:":
            from pathlib import Path

            if Path(db_path).exists():
                self._ok(f"Database: {db_path} accessible")
            else:
                self._warn(
                    f"Database: {db_path} not found (will be created on first use)"
                )
        else:
            self._ok("Database: configured")

    def summary(self) -> int:
        total = self.ok + self.warn + self.fail
        parts = []
        if self.fail:
            parts.append(f"{self.fail} failed")
        if self.warn:
            parts.append(f"{self.warn} warning{'s' if self.warn > 1 else ''}")
        parts.append(f"{self.ok} passed")

        self.console.print()
        if self.fail:
            self.console.print(f"[red]{total} checks: {', '.join(parts)}.[/red]")
            return 1
        elif self.warn:
            self.console.print(
                f"[yellow]All checks passed ({', '.join(parts)}).[/yellow]"
            )
            return 0
        else:
            self.console.print(f"[green]All {total} checks passed.[/green]")
            return 0


def main(argv: list[str] | None = None) -> None:
    """Entry point for 'skitter doctor'."""
    doc = _Doctor()
    doc.console.print("\nChecking skitter health...\n")

    config_ok = doc.check_config()
    if config_ok:
        doc.check_llm()
        doc.check_broker()
        doc.check_services()
        doc.check_database()

    rc = doc.summary()
    sys.exit(rc)
