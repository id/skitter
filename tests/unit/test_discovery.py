"""Discovery card unit tests."""

import json


from skitter.a2a import topic_discovery_wildcard
from skitter.config import AgentDef


# --- Discovery cards ---


class TestBuildCard:
    def test_agent_card_schema(self):
        from skitter.discovery import build_card

        agent = AgentDef(
            id="researcher",
            name="Researcher",
            description="Deep research with citations",
        )
        card = build_card(agent)
        assert card["name"] == "Researcher"
        assert card["description"] == "Deep research with citations"
        assert card["version"] == "0.1.0"
        assert "url" not in card
        # URL only lives inside supportedInterfaces per registry schema
        ifaces = card["supportedInterfaces"]
        assert len(ifaces) == 1
        assert ifaces[0]["protocolVersion"] == "1.0.0"
        assert ifaces[0]["protocolBinding"] == "MQTTv5+JSONRPCv2"
        assert "url" in ifaces[0]
        assert card["capabilities"]["streaming"] is True
        assert card["capabilities"]["pushNotifications"] is False
        assert card["defaultInputModes"] == ["text/plain"]
        assert card["defaultOutputModes"] == ["text/plain"]
        assert card["skills"][0]["id"] == "default"
        assert card["skills"][0]["tags"] == ["researcher"]
        assert "metadata" not in card

    def test_agent_card_custom_capabilities(self):
        from skitter.discovery import build_card

        agent = AgentDef(
            id="coder",
            name="Coder",
            description="Writes code",
            capabilities={"streaming": False},
            input_modes=["text/plain", "application/json"],
        )
        card = build_card(agent)
        assert card["capabilities"]["streaming"] is False
        assert card["capabilities"]["pushNotifications"] is False
        assert card["defaultInputModes"] == ["text/plain", "application/json"]

    def test_composed_app_card_has_app_extension(self):
        from skitter.discovery import APP_EXTENSION_URI, build_card

        agent = AgentDef(id="my-app", name="My App", description="A composed app")
        metadata = {
            "variables": ["topic"],
            "tasks": [
                {
                    "id": "step1",
                    "agent": "researcher",
                    "description": "Research {topic}",
                },
            ],
        }
        card = build_card(agent, metadata=metadata)
        assert "metadata" not in card
        exts = card["capabilities"]["extensions"]
        wf = next(e for e in exts if e["uri"] == APP_EXTENSION_URI)
        assert wf["params"]["variables"] == ["topic"]
        assert len(wf["params"]["tasks"]) == 1
        assert wf["params"]["tasks"][0]["id"] == "step1"

    def test_card_has_url(self):
        from skitter.discovery import build_card

        agent = AgentDef(id="test", name="Test")
        card = build_card(agent, url="mqtt://custom:1883")
        assert "url" not in card
        assert card["supportedInterfaces"][0]["url"] == "mqtt://custom:1883"

    def test_card_skills_have_tags(self):
        from skitter.discovery import build_card

        agent = AgentDef(
            id="coder",
            name="Coder",
            description="Writes code",
            tags=["code", "python"],
        )
        card = build_card(agent)
        assert card["skills"][0]["tags"] == ["code", "python"]

    def test_card_skills_default_tags(self):
        from skitter.discovery import build_card

        agent = AgentDef(id="writer", name="Writer", description="Writes")
        card = build_card(agent)
        assert card["skills"][0]["tags"] == ["writer"]


class TestParseCard:
    def test_parse_card(self):
        from skitter.discovery import parse_card

        raw = json.dumps({"name": "Test", "version": "0.1.0"}).encode()
        card = parse_card(raw)
        assert card["name"] == "Test"

    def test_is_app_card(self):
        from skitter.discovery import is_app_card

        assert not is_app_card({"name": "Agent"})
        assert not is_app_card({"capabilities": {}})
        assert not is_app_card({"capabilities": {"extensions": []}})
        assert is_app_card(
            {
                "capabilities": {
                    "extensions": [
                        {
                            "uri": "urn:skitter:app",
                            "params": {"tasks": [{"id": "step1"}]},
                        }
                    ]
                }
            }
        )


class TestDiscoveryWildcard:
    def test_default_org_unit(self):
        t = topic_discovery_wildcard()
        assert "/discovery/" in t
        assert t.endswith("/+")

    def test_custom_org_unit(self):
        t = topic_discovery_wildcard("myorg", "myunit")
        assert "myorg/myunit/+" in t
