# Mission MCP: durable board, governance, and action foundation

Long-running assistant work should not disappear when a chat ends or a model changes. This
snapshot keeps the durable agent, mission, and work board in SQLite so another model execution can
resume the same plan. Current board rows drive execution; append-only events retain what changed.

## Included in this snapshot

- Durable agents, non-secret account references, and one-owner missions
- A five-state work board with one assigned writer per card
- Same-mission, acyclic dependencies with automatic unblock and recursive reblock
- Evidence-backed completion and explicit, evidence-backed mission closure
- Reasoned archiving that preserves history and protects active dependents
- Durable leases for actionable work and timed or conditional wakeups
- Human-approved changes to mission goals, constraints, criteria, and policy
- Policy-gated, exact-payload permits for one-time external actions
- Atomic transactions, optimistic versions, and request-bound idempotency
- An append-only audit trail for board, governance, and action decisions

This remains a transport-independent domain store. It does **not** yet include an MCP server,
conversation or memory management, procedural memory, external connectors, or a model host.

## Mission and board model

An agent is a durable identity that may own many missions. Each mission has one owner. The owner
creates, delegates, and archives work; the assigned agent is the card's single writer. Every card
mutation supplies `expected_version`, so a stale execution cannot silently overwrite newer state.

The five states belong to work items, not missions:

| State | Meaning |
| --- | --- |
| `doing` | Actionable now and eligible for an execution lease. |
| `waiting` | Check again at `next_check_at`, optionally for a named condition. |
| `blocked` | One or more work-item dependencies are still open. |
| `needs_input` | A concrete human question must be answered. |
| `done` | Complete, with retained verification evidence. |

Dependencies must stay within one mission and form a directed acyclic graph. Finishing the last
open prerequisite moves a blocked card to `doing`. Reopening a completed prerequisite recursively
reblocks every active downstream card, including cards that were already `done`; their historical
completion evidence remains in the event stream.

Archiving is cancellation from the active plan, not a sixth state. It requires a reason, never
deletes the card, and is rejected while another active card depends on it. The owner explicitly
closes a mission only after every non-archived card is `done`, no wakeup remains live, and the
overall goal and acceptance criteria have concrete closure evidence. Pending mission changes and
unredeemed pending or approved actions must also be resolved first.

Every mutation also requires an `idempotency_key`. Repeating the exact request returns the original
response; reusing that key for different arguments raises `IDEMPOTENCY_CONFLICT`.

## Governance and external actions

Mission title, goal, constraints, acceptance criteria, and action policy cannot be edited directly.
The owner proposes an exact patch against the current mission version; a trusted human caller then
approves or rejects it. Approval fails if the mission changed while the proposal was waiting. Any
approved mission-specification change supersedes every unredeemed pending or approved action
permit, ensuring old authority cannot survive a changed objective or policy.

Action policy has a default effect and ordered exact-name or glob rules. The first matching rule
wins. `action_prepare` is owner-only and stores the exact payload, its hash, and the policy hash:

- `allow` creates an approved permit.
- `require_approval` creates a pending request for a trusted human decision.
- `deny` records a denied request and creates no usable permit.

The owner may cancel a pending or approved request with a retained reason. A trusted runtime gateway
can redeem an approved permit exactly once, with optimistic-version and current-policy checks, and
receives only the exact stored payload. Preparing or redeeming a permit never executes a connector.
`board_snapshot` exposes pending mission changes and live action requests alongside operational work.

## Host scheduling boundary

SQLite can preserve durable scheduling intent, but it cannot start a model or keep a worker
process alive. A host or scheduler should:

1. Call `ready_work_claim` to lease `doing` cards and resume their assigned agents.
2. Call `wakeup_claim_due` to lease due `waiting` checks.
3. Start the model with the authoritative mission and `board_snapshot` state.
4. Release the execution lease after the model turn, or resolve the wake as ready or rescheduled.
5. Send an approved `action_redeem` payload to the appropriate connector gateway exactly once.

Leases prevent duplicate dispatch while allowing another worker to recover after expiry. This
store never sends email, watches an inbox, or executes another connector itself.

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
    store.work_item_create(
        mission_id=mission["id"],
        actor_agent_id=agent["id"],
        title="Book the venue",
        idempotency_key="create-venue-work",
    )
```

Use a durable filesystem path in real use. The default `:memory:` database is intentionally
ephemeral and is useful only for tests or experiments.

## Store API

- Agents: `agent_onboard`, `agent_get`, `agent_list`
- Account references: `account_reference_add`, `account_reference_list`
- Missions: `mission_create`, `mission_get`, `mission_list`, participant reads, `mission_close`
- Governance: `mission_change_propose`, `mission_change_decide`
- External actions: `action_prepare`, `action_decide`, `action_cancel`, `action_redeem`
- Work: create, get, list, update, assign, add/remove dependency, transition, and archive
- Board and audit: `board_snapshot`, `audit_list`
- Runtime: `ready_work_claim`, `ready_work_release`, `wakeup_claim_due`, `wakeup_resolve`
- Lifecycle: `close`, plus context-manager support

The store assumes one trusted workspace. Authentication, caller-specific tool surfaces, and tenant
isolation are outside this snapshot. A host must expose human-decision methods only through an
authenticated human surface and `action_redeem` only to a trusted connector gateway. Account
references are identifiers, not credentials; secret-shaped metadata fields are rejected.

## Checks

```bash
uv lock --check
uv run python -m unittest discover -s tests -p 'test*.py' -v
uv run --with ruff ruff check mission_mcp tests
uv run --with ruff ruff format --check mission_mcp tests
uv run python -m compileall -q mission_mcp tests
git diff --check
```
