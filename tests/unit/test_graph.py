"""Graph generation and validation unit tests."""

import json
from unittest.mock import AsyncMock, patch

import pytest


# --- Graph generation and validation ---


class TestGraphValidation:
    def test_valid_graph(self):
        from skitter.graph_gen import validate_graph

        graph = {
            "tasks": [
                {
                    "id": "read",
                    "agent": "reader",
                    "description": "Read data",
                    "needs": [],
                },
                {
                    "id": "analyze",
                    "agent": "analyzer",
                    "description": "Analyze data",
                    "needs": ["read"],
                    "terminal": True,
                },
            ]
        }
        validate_graph(graph, {"reader", "analyzer"})  # should not raise

    def test_empty_tasks(self):
        from skitter.graph_gen import GraphValidationError, validate_graph

        with pytest.raises(GraphValidationError, match="non-empty"):
            validate_graph({"tasks": []}, {"a"})

    def test_unknown_agent(self):
        from skitter.graph_gen import GraphValidationError, validate_graph

        graph = {
            "tasks": [{"id": "t1", "agent": "unknown", "needs": [], "terminal": True}]
        }
        with pytest.raises(GraphValidationError, match="unknown agent"):
            validate_graph(graph, {"reader"})

    def test_duplicate_task_id(self):
        from skitter.graph_gen import GraphValidationError, validate_graph

        graph = {
            "tasks": [
                {"id": "t1", "agent": "a", "needs": [], "terminal": True},
                {"id": "t1", "agent": "a", "needs": [], "terminal": True},
            ]
        }
        with pytest.raises(GraphValidationError, match="Duplicate"):
            validate_graph(graph, {"a"})

    def test_cycle_detected(self):
        from skitter.graph_gen import GraphValidationError, validate_graph

        graph = {
            "tasks": [
                {"id": "a", "agent": "x", "needs": ["b"]},
                {"id": "b", "agent": "y", "needs": ["a"], "terminal": True},
            ]
        }
        with pytest.raises(GraphValidationError, match="Cycle"):
            validate_graph(graph, {"x", "y"})

    def test_no_terminal_caught(self):
        from skitter.graph_gen import GraphValidationError, validate_graph

        graph = {
            "tasks": [
                {"id": "t1", "agent": "a", "needs": []},
                {"id": "t2", "agent": "b", "needs": ["t1"]},
            ]
        }
        with pytest.raises(GraphValidationError, match="No terminal"):
            validate_graph(graph, {"a", "b"})

    def test_unknown_need(self):
        from skitter.graph_gen import GraphValidationError, validate_graph

        graph = {
            "tasks": [
                {"id": "t1", "agent": "a", "needs": ["nonexistent"], "terminal": True},
            ]
        }
        with pytest.raises(GraphValidationError, match="unknown task"):
            validate_graph(graph, {"a"})

    def test_non_list_needs(self):
        from skitter.graph_gen import GraphValidationError, validate_graph

        # LLM sometimes emits a bare string instead of a list; iterating it would
        # otherwise produce misleading per-character "unknown task" errors.
        graph = {
            "tasks": [
                {"id": "t1", "agent": "a", "needs": "t0", "terminal": True},
            ]
        }
        with pytest.raises(GraphValidationError, match="non-list"):
            validate_graph(graph, {"a"})

    def test_terminal_has_dependents(self):
        from skitter.graph_gen import GraphValidationError, validate_graph

        graph = {
            "tasks": [
                {"id": "t1", "agent": "a", "needs": [], "terminal": True},
                {"id": "t2", "agent": "b", "needs": ["t1"], "terminal": True},
            ]
        }
        with pytest.raises(GraphValidationError, match="must not have dependents"):
            validate_graph(graph, {"a", "b"})


class TestGraphGeneration:
    def _make_cards(self):
        # Agent IDs intentionally differ from skill IDs to catch code
        # that confuses the two (the bug that motivated this change).
        return {
            "agent-reader-001": {
                "name": "Reader",
                "description": "Reads sensor data",
                "skills": [{"id": "read-skill", "name": "Reader"}],
            },
            "agent-analyzer-002": {
                "name": "Analyzer",
                "description": "Analyzes data",
                "skills": [{"id": "analyze-skill", "name": "Analyzer"}],
            },
        }

    @pytest.mark.asyncio
    async def test_generate_valid_graph(self):

        from skitter.graph_gen import generate_graph

        valid_graph = json.dumps(
            {
                "tasks": [
                    {
                        "id": "read",
                        "agent": "agent-reader-001",
                        "description": "Read sensor data",
                        "needs": [],
                    },
                    {
                        "id": "analyze",
                        "agent": "agent-analyzer-002",
                        "description": "Analyze the data",
                        "needs": ["read"],
                        "terminal": True,
                    },
                ]
            }
        )

        with patch("skitter.graph_gen.complete", new_callable=AsyncMock) as mock_llm:
            mock_llm.return_value = valid_graph
            graph = await generate_graph(
                "Read and analyze sensors", self._make_cards(), model="test"
            )

        assert len(graph["tasks"]) == 2
        assert graph["tasks"][0]["agent"] == "agent-reader-001"

    @pytest.mark.asyncio
    async def test_generate_strips_markdown_fences(self):

        from skitter.graph_gen import generate_graph

        fenced = '```json\n{"tasks": [{"id": "t1", "agent": "agent-reader-001", "description": "do it", "needs": [], "terminal": true}]}\n```'

        with patch("skitter.graph_gen.complete", new_callable=AsyncMock) as mock_llm:
            mock_llm.return_value = fenced
            graph = await generate_graph("Do it", self._make_cards(), model="test")

        assert len(graph["tasks"]) == 1

    @pytest.mark.asyncio
    async def test_generate_retries_on_validation_error(self):

        from skitter.graph_gen import generate_graph

        bad_graph = json.dumps(
            {
                "tasks": [
                    {
                        "id": "t1",
                        "agent": "nonexistent",
                        "needs": [],
                        "terminal": True,
                    }
                ]
            }
        )
        good_graph = json.dumps(
            {
                "tasks": [
                    {
                        "id": "t1",
                        "agent": "agent-reader-001",
                        "description": "Read",
                        "needs": [],
                        "terminal": True,
                    }
                ]
            }
        )

        with patch("skitter.graph_gen.complete", new_callable=AsyncMock) as mock_llm:
            mock_llm.side_effect = [bad_graph, good_graph]
            graph = await generate_graph("Read", self._make_cards(), model="test")

        assert mock_llm.call_count == 2
        assert graph["tasks"][0]["agent"] == "agent-reader-001"

    @pytest.mark.asyncio
    async def test_generate_fails_after_retries(self):

        from skitter.graph_gen import GraphValidationError, generate_graph

        bad_graph = json.dumps(
            {
                "tasks": [
                    {
                        "id": "t1",
                        "agent": "nonexistent",
                        "needs": [],
                        "terminal": True,
                    }
                ]
            }
        )

        with patch("skitter.graph_gen.complete", new_callable=AsyncMock) as mock_llm:
            mock_llm.return_value = bad_graph
            with pytest.raises(GraphValidationError, match="unknown agent"):
                await generate_graph("Read", self._make_cards(), model="test")

        assert mock_llm.call_count == 2
