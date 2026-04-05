"""In-memory registry of discovered A2A agent cards."""

import logging

from skitter.discovery import is_app_card

log = logging.getLogger("skitter.coordinator")


class DiscoveryRegistry:
    """In-memory registry of discovered A2A agent cards."""

    def __init__(self) -> None:
        self._cards: dict[str, dict] = {}  # agent_id -> card dict

    def update(self, agent_id: str, card: dict) -> None:
        self._cards[agent_id] = card
        log.info("Registry: updated card for %s", agent_id)

    def remove(self, agent_id: str) -> None:
        if agent_id in self._cards:
            del self._cards[agent_id]
            log.info("Registry: removed card for %s", agent_id)

    def get(self, agent_id: str) -> dict | None:
        return self._cards.get(agent_id)

    def list_agents(self) -> list[str]:
        return [aid for aid, card in self._cards.items() if not is_app_card(card)]

    def list_apps(self) -> list[str]:
        return [aid for aid, card in self._cards.items() if is_app_card(card)]
