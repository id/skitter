---
name: a2a-compliance
description: Validate A2A and A2A-over-MQTT protocol compliance. Use after changing protocol-facing code.
allowed-tools: Read, Grep, Glob
---

# A2A Protocol Compliance Check

Validate that skitter's protocol layer conforms to the A2A v1.0.0 spec and the A2A-over-MQTT v0.1 binding.

## Authoritative Sources (local)

Read all four files before checking:

- **A2A proto** (data structures): `docs/spec/a2a.proto`
- **A2A spec** (JSON serialization rules, examples): `docs/spec/a2a-specification.md`
- **A2A-over-MQTT transport** (MQTT transport binding): `docs/spec/a2a-over-mqtt-transport.md`
- **A2A-over-MQTT architecture** (design rationale): `docs/spec/a2a-over-mqtt-architecture.md`

## Procedure

### Phase 1: Data Object Wire Format Verification

The proto is the canonical schema. The A2A spec (docs/specification.md) defines JSON serialization rules. Both must be checked. For every data object skitter produces or consumes on the wire:

1. **Enum values.** Check the spec's JSON serialization section for enum representation rules. Then grep skitter for every enum value written to JSON and verify it matches the required format.
2. **Field names.** Check the spec's field naming convention. Verify every JSON field name skitter produces matches.
3. **Proto oneof mapping.** For every proto `oneof`, check how the spec requires it to be represented in JSON. Compare against what skitter actually produces. Pay special attention to discriminator patterns (type fields, wrapper keys).
4. **Required fields.** For every proto message with `REQUIRED` annotations, verify skitter includes all required fields with correct types.
5. **Extra fields.** Check for fields skitter includes that are NOT in the proto. Note as WARN.

Do this by reading each `make_*` / `to_json` function and comparing its output structure, field by field, against the proto message definition and the spec's JSON rules. Do not assume correctness from field names alone; verify values and structure.

### Phase 2: Transport Layer Verification

Work through the A2A-over-MQTT spec sections in order. For every MUST/REQUIRED constraint:

1. **Find the implementation.** Search the codebase for the code that implements or should implement the requirement. Follow imports and call chains wherever they lead.
2. **Verify correctness.** Read the relevant code and confirm it satisfies the spec requirement.
3. **Verify test coverage.** Search `tests/` for tests that enforce this requirement. A requirement without a test is a compliance gap even if the code is correct today.
4. **Record the result.**

For SHOULD-level recommendations: verify and report, but WARN is acceptable if intentionally skipped.

Skip sections that are clearly out of scope (broker internals, features listed in CLAUDE.md Limitations as not implemented).

## Output

Report:
- **PASS**: requirement satisfied in code and covered by tests
- **FAIL**: code violation, missing implementation, or missing test coverage (with file, line, and what the spec requires)
- **WARN**: SHOULD-level gaps or intentional deviations
