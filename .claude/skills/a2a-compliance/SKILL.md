---
name: a2a-compliance
description: Validate A2A and A2A-over-MQTT protocol compliance. Use after changing protocol-facing code.
allowed-tools: Read, Grep, Glob, WebFetch, WebSearch
---

# A2A Protocol Compliance Check

Validate that skitter's protocol layer conforms to the A2A v1.0.0 spec and the A2A-over-MQTT v0.1 binding.

## Authoritative Sources

- **A2A proto** (data structures): https://github.com/a2aproject/A2A/blob/main/specification/a2a.proto
- **A2A-over-MQTT spec** (MQTT transport binding): https://github.com/emqx/mqtt-for-ai/blob/main/a2a-over-mqtt/specification/0.1/basic/mqtt_transport.md

Fetch both specs via `WebFetch` (raw GitHub URLs) before checking. Do not rely on memory; the specs evolve.

## Procedure

Work through each section of the fetched specs in order. For every MUST/REQUIRED constraint in the spec:

1. **Find the implementation.** Search the codebase for the code that implements or should implement the requirement. This is not limited to specific files; follow imports and call chains wherever they lead.
2. **Verify correctness.** Read the relevant code and confirm it satisfies the spec requirement. Check field names, types, presence/absence, allowed values, error codes, and behavioral rules (ordering, dedup, retry, etc.).
3. **Verify test coverage.** Search `tests/` for tests that enforce this requirement. A requirement without a test is a compliance gap even if the code is correct today.
4. **Record the result.** PASS, FAIL (with file, line, and what the spec requires), or WARN (SHOULD-level recommendations we intentionally skip).

For SHOULD-level recommendations: verify and report, but WARN is acceptable if intentionally skipped.

Skip sections that are clearly out of scope (broker internals, features listed in CLAUDE.md Limitations as not implemented).

## Output

Report:
- **PASS**: requirement satisfied in code and covered by tests
- **FAIL**: code violation, missing implementation, or missing test coverage (with file, line, and what the spec requires)
- **WARN**: SHOULD-level gaps or intentional deviations
