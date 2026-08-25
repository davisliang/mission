# Mission MCP: durable mission, memory, and handoff foundation

Long-running assistant work should not disappear when a chat ends or a model changes. This
snapshot keeps the durable agent, mission, work board, memory, and raw conversation in SQLite so
another model execution can resume the same plan. Current rows drive execution; immutable history
retains what changed and why.

## Included in this snapshot

- Durable agents, non-secret account references, and one-owner missions
- A five-state work board with one assigned writer per card
- Same-mission, acyclic dependencies with automatic unblock and recursive reblock
- Evidence-backed completion and explicit, evidence-backed mission closure
- Reasoned archiving that preserves history and protects active dependents
- Durable leases for actionable work and timed or conditional wakeups
- Human-approved changes to mission goals, constraints, criteria, and policy
- Policy-gated, exact-payload execution claims for external actions
- Versioned semantic facts, immutable episodes, and human-approved procedures
- Raw mission and general-agent conversation with non-destructive compaction checkpoints
- Bounded, recipient-scoped handoff context for model-independent resumption
- Atomic transactions, optimistic versions, and request-bound idempotency
- An append-only audit trail for board, governance, action, memory, and conversation events

This remains a transport-independent domain store. It does **not** yet include an MCP server,
external connectors, embeddings or semantic ranking, or a model host.

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
pending, approved, or executing actions must also be resolved first.

Every mutation also requires an `idempotency_key`. Repeating the exact request returns the original
response; reusing that key for different arguments raises `IDEMPOTENCY_CONFLICT`.

## Governance and external actions

Mission title, goal, constraints, acceptance criteria, and action policy cannot be edited directly.
The owner proposes an exact patch against the current mission version; a trusted human caller then
approves or rejects it. Approval fails if the mission changed while the proposal was waiting. Any
approved mission-specification change supersedes every pending or approved action request, ensuring
old authority cannot survive a changed objective or policy. A change cannot be approved while a
connector action is already in flight.

Action policy has a default effect and ordered exact-name or glob rules. The first matching rule
wins. `action_prepare` is owner-only and stores the exact payload, its hash, and the policy hash:

- `allow` creates an approved permit.
- `require_approval` creates a pending request for a trusted human decision.
- `deny` records a denied request and creates no usable permit.

The owner may cancel a pending or approved request with a retained reason. `action_redeem` gives a
trusted gateway a time-limited execution claim and the exact stored payload. Its stable
`execution_key` must be used as the downstream connector's idempotency key. The gateway then calls
`action_resolve` with concrete success or failure evidence. An expired claim may be recovered, but
always uses the same execution key. Mission changes and closure wait for in-flight actions, and a
resolved, superseded, or closed action can never replay its payload. Preparing or claiming an action
never executes a connector itself. `board_snapshot` exposes pending mission changes and live action
requests alongside operational work.

## Memory, conversation, and handoff

Semantic memory keeps one current fact per agent, optional mission scope, and key. Writes require
provenance and an optimistic version; every complete version remains in immutable history and the
audit stream. Expiry hides a current fact from search without erasing its history. Episodic memory
is append-only evidence of what happened. Agent-wide procedures do not change immediately: an
agent proposes an exact versioned change, optionally linked to a mission, and a trusted human
approves or rejects it through the same governance path as mission changes.

Conversation has two explicit scopes. Mission conversation is shared by participating agents;
general conversation belongs to one durable agent. Every message is immutable. A compaction is an
additive summary checkpoint through a validated message cursor, never deletion or replacement of
the raw history. A user reply may carry `metadata.work_item_id` only for active `needs_input` work
in the same mission.

`mission_handoff` builds bounded resumption context for a named participating recipient. It keeps
the recipient identity distinct from the mission owner and combines the authoritative board,
recipient-only memory and general chat, the newest shared conversation after the latest compaction,
and the newest shared audit events. Tail truncation flags tell the host when it must page history.
Another agent's private semantic memory and general chat are never included in the recipient's
handoff.

## Host scheduling boundary

SQLite can preserve durable scheduling intent, but it cannot start a model or keep a worker
process alive. A host or scheduler should:

1. Call `ready_work_claim` to lease `doing` cards and resume their assigned agents.
2. Call `wakeup_claim_due` to lease due `waiting` checks.
3. Start the model with the recipient-scoped `mission_handoff` context.
4. Release the execution lease after the model turn, or resolve the wake as ready or rescheduled.
5. Claim an approved action, execute it with its `execution_key`, and record the outcome with
   `action_resolve`.

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
- External actions: `action_prepare`, `action_decide`, `action_cancel`, `action_redeem`,
  `action_resolve`
- Memory: `semantic_memory_put`, `semantic_memory_history`, `episodic_memory_append`,
  `procedural_memory_change_propose`, `memory_search`
- Conversation: mission/general append and history, `conversation_compact`
- Handoff: `mission_handoff`
- Work: create, get, list, update, assign, add/remove dependency, transition, and archive
- Board and audit: `board_snapshot`, `audit_list`
- Runtime: `ready_work_claim`, `ready_work_release`, `wakeup_claim_due`, `wakeup_resolve`
- Lifecycle: `close`, plus context-manager support

The store assumes one trusted workspace. Authentication, caller-specific tool surfaces, and tenant
isolation are outside this snapshot. A host must expose human-decision methods only through an
authenticated human surface and action claim/resolution only to a trusted connector gateway.
references are identifiers, not credentials; secret-shaped metadata fields are rejected. Hosts
must bind agent-scoped methods to the authenticated durable agent and pass participant identity for
mission-scoped conversation access.

## Checks

```bash
uv lock --check
uv run python -m unittest discover -s tests -p 'test*.py' -v
uv run --with ruff ruff check mission_mcp tests
uv run --with ruff ruff format --check mission_mcp tests
uv run python -m compileall -q mission_mcp tests
git diff --check
```
