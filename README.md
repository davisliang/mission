# Mission MCP: storage foundation

Long-running assistant work should not disappear when a chat ends or a model changes. This first
project slice establishes a durable SQLite record for the agent identity and the mission it owns.
Models and providers are deliberately absent from that identity: a later executor can resume the
same durable agent and mission.

## Included in this snapshot

- Durable agent onboarding, lookup, and listing
- Non-secret account references and scopes
- Mission creation, lookup, and listing with exactly one owner
- Validated, ordered action-policy rules inherited from onboarding
- Atomic SQLite transactions guarded for multi-threaded callers
- Idempotency keys bound to the exact mutation request
- Append-only audit events for every successful mutation

This is the domain-store foundation only. It does **not** yet include an MCP transport, board work
items, memory, approvals, wake scheduling, external-action permits, or a model host. Those layers
can build on these storage guarantees without making a model session the source of truth.

## Core rules

An agent is a durable identity and may eventually own many missions. Each mission has one owning
agent and records its goal, constraints, acceptance criteria, action policy, lifecycle status,
and optimistic version.

Mutating methods require an `idempotency_key`. Repeating the same request with the same key returns
the original response; reusing that key for different arguments raises `IDEMPOTENCY_CONFLICT`.
Current rows are operational state, while immutable events retain the history of successful
changes.

Action policy allows by default and supports ordered exact-name or glob rules with three effects:
`allow`, `require_approval`, and `deny`. This snapshot validates and stores policy; enforcement
arrives with the external-action layer.

Account references are identifiers, not credentials. Metadata with secret-shaped fields such as
passwords, API keys, tokens, private keys, or cookies is rejected. Do not place credential values
in account identifiers, mission text, or other SQLite fields.

## Install and use

The runtime has no third-party dependencies. Python 3.12 or newer and
[uv](https://docs.astral.sh/uv/) are expected for development:

```bash
uv sync
```

```python
from mission_mcp import MissionStore

with MissionStore("mission.db") as store:
    agent = store.agent_onboard(
        name="Avery",
        role="Executive assistant",
        idempotency_key="onboard-avery",
    )["agent"]
    mission = store.mission_create(
        owner_agent_id=agent["id"],
        title="Plan the retreat",
        goal="Deliver a verified retreat plan",
        constraints=["Stay on budget"],
        acceptance_criteria=["Venue and agenda are confirmed"],
        idempotency_key="create-retreat-mission",
    )["mission"]
```

Use a durable filesystem path in real use. The default `:memory:` database is intentionally
ephemeral and is useful only for tests or experiments.

## Store API

- Agents: `agent_onboard`, `agent_get`, `agent_list`
- Account references: `account_reference_add`, `account_reference_list`
- Missions: `mission_create`, `mission_get`, `mission_list`
- Lifecycle: `close`, plus context-manager support

The store assumes one trusted workspace. Authentication, caller-specific tool surfaces, and tenant
isolation are outside this snapshot.

## Checks

```bash
uv lock --check
uv run python -m unittest discover -s tests -p 'test*.py' -v
uv run --with ruff ruff check mission_mcp tests
uv run --with ruff ruff format --check mission_mcp tests
uv run python -m compileall -q mission_mcp tests
git diff --check
```
