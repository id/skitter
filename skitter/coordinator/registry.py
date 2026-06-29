"""In-memory registry of discovered A2A agent cards."""

import logging

from skitter.discovery import is_app_card

log = logging.getLogger("skitter.coordinator")


class DiscoveryRegistry:
    """In-memory registry of discovered A2A agent cards."""

    def __init__(self) -> None:
        self._cards: dict[str, dict] = {}  # agent_id -> card dict
        self._statuses: dict[str, str] = {}  # agent_id -> online/offline/unknown

    def update(self, agent_id: str, card: dict, status: str = "unknown") -> None:
        self._cards[agent_id] = card
        self._statuses[agent_id] = status
        log.info("Registry: updated card for %s", agent_id)

    def remove(self, agent_id: str) -> None:
        removed = self._cards.pop(agent_id, None)
        self._statuses.pop(agent_id, None)
        if removed is not None:
            log.info("Registry: removed card for %s", agent_id)

    def get(self, agent_id: str) -> dict | None:
        return self._cards.get(agent_id)

    def status(self, agent_id: str) -> str:
        return self._statuses.get(agent_id, "unknown")

    def list_agents(self) -> list[str]:
        return [
            aid
            for aid, card in self._cards.items()
            if not is_app_card(card) and self.status(aid) != "offline"
        ]

    def list_apps(self) -> list[str]:
        return [aid for aid, card in self._cards.items() if is_app_card(card)]
