# Mission MCP

Mission MCP gives a long-running assistant a durable place to work. Model calls, chat sessions,
and model providers are temporary; a real mission may last for days, pause for a reply, require
approval, involve delegated work, and resume after a restart. Without shared durable state, an
assistant can lose its plan, repeat an action, miss a follow-up, or declare success without checking
the original goal.

This project keeps the assistant's identity and work outside any particular model execution. A new
model can resume the same mission with the same board, memory, conversation, permissions, pending
wakes, and evidence. The model is the current worker; it is not the durable agent.

Mission MCP is a compact Python 3.12 server built with the official `mcp==2.1.0` SDK and the
standard-library SQLite driver. It is designed as a practical foundation for a personal executive
assistant while keeping its domain rules independent of any model, scheduler, or connector.

## Core model

```text
durable agent -> mission -> work items -> temporary model executions
```

An agent is a persistent identity and may own many missions. Each mission has exactly one owner.
The owner may delegate individual work items to other durable agents but remains accountable for
the mission as a whole. Optimistic versions give every card one current writer and reject stale
updates instead of silently overwriting newer work.

The board is the execution source of truth. Its current rows decide what is actionable and what
should wake next; an append-only event history records how and why that state changed.

## Mission lifecycle

1. A trusted human-facing host onboards the durable agent and creates an approved mission with a
   goal, constraints, acceptance criteria, and action policy.
2. The owner turns the mission into work items, orders them with dependencies, and delegates when
   useful.
3. Models execute actionable work, retain memory and evidence, and move cards through the five
   states below. A later model can resume from a handoff without becoming a new agent.
4. Timed or conditional work records its next check. Human questions and protected external
   actions wait for the appropriate reply or approval. The host wakes the relevant agent.
5. Every completed card retains verification evidence. The owner checks the overall goal and
   acceptance criteria, then explicitly closes the mission with mission-level evidence.

A mission itself is simply open or closed. These five states apply only to work items:

| State | Meaning |
| --- | --- |
| `doing` | Work is actionable and eligible for an execution lease. |
| `waiting` | Check again at `next_check_at`, optionally for a named condition. |
| `blocked` | One or more work-item dependencies are still open. |
| `needs_input` | A concrete human question must be answered. |
| `done` | Work is complete and includes verification evidence. |

Completing the last dependency makes a blocked card actionable. Reopening a prerequisite
recursively reblocks its downstream dependents. Archiving is an explicit, reasoned cancellation
from the active plan, not a sixth state. It preserves the card and its evidence.

Every mutation has an `idempotency_key`; retrying the same request cannot duplicate its effect.
Writes to versioned records also carry `expected_version` so competing executions fail clearly.

## Components and trust boundaries

All surfaces use the same store but expose different authority:

| Component | Intended caller | Responsibility |
| --- | --- | --- |
| SQLite store | All server surfaces | Holds the board, memory, conversation, approvals, leases, and audit history. |
| `agent` surface | One session bound to one durable agent ID | Reads and advances that agent's visible missions without impersonating another agent. |
| `control` surface | Authenticated human-facing application | Onboards agents, creates missions, records chat, and decides approvals. |
| `runtime` surface | Scheduler, worker host, or connector gateway | Leases ready work and claims and resolves connector execution. |
| `all` surface | Local development or trusted administration | Combines every surface and must not be exposed to an ordinary model. |

MCP supplies durable tools and lifecycle enforcement; it is not the agent host. The server can say
what is ready or due and build resumption context, but it cannot start a model, keep a process
alive, watch an inbox, or send an email by itself. A production host owns that loop and the actual
connectors.

## Durable context

SQLite holds complementary forms of state:

- Current mission and board rows drive execution, scheduling, and snapshots.
- The append-only audit stream records state changes and decisions.
- Semantic memory is versioned, provenanced, optionally expiring, and retains every version.
- Episodic memory is immutable evidence of what happened.
- Procedural-memory changes require trusted-human approval and remain private agent state.
- Raw mission and general-agent conversation is immutable and cursor-readable.
- Owner-only compaction adds summary checkpoints without deleting or rewriting raw messages.

Memory search is paged independently by semantic, episodic, and procedural category. Pending
procedure proposals do not appear on the shared board or block mission closure; their owning agent
can recover them after a restart or after a related mission closes.

Conversation actors have durable namespaces: `user` uses `human:*`, `system` uses `system:*`, and
`tool` uses `tool:*`; an `assistant` actor must exactly match the scoped durable agent. A human reply
may reference `metadata.work_item_id` only for active `needs_input` work in the same mission.

`mission_handoff` builds bounded resumption context for one participating recipient. It combines
that durable agent's identity, the shared mission and board, recipient-only memory and general chat,
the newest mission conversation after the latest compaction, its own pending procedure proposals,
and a privacy-filtered audit tail. Conversation and memory truncation metadata tell the host exactly
where to page more context. Delegates never receive another agent's private memory, general chat,
procedure proposals, or procedure audit events.

Memory is context, not a credential store. Store account references only—never passwords, tokens,
API keys, private keys, session cookies, or connector credentials.

## Governance and external actions

Mission title, goal, constraints, acceptance criteria, and action policy cannot be edited directly.
The owner proposes an exact patch against the current mission version; a trusted human approves or
rejects it. Any approved mission change supersedes pending or approved action requests. A change
cannot be approved while a connector action is executing.

Actions are allowed by default. Ordered policy rules can require approval or deny selected action
name globs; the first match wins:

```json
{
  "default": "allow",
  "rules": [
    {"action": "email.*", "effect": "require_approval"},
    {"action": "purchase", "effect": "deny"}
  ]
}
```

External actions cross a deliberately explicit boundary:

1. The owner calls `action_prepare`, which stores the exact payload plus payload and policy hashes.
2. The control surface records human approval when policy requires it.
3. A trusted gateway calls `action_redeem` to claim the payload under a time-limited execution
   lease.
4. The gateway executes the connector using the returned stable `execution_key` as its downstream
   idempotency key.
5. The gateway calls `action_resolve` with concrete success or failure evidence.

An expired claim may be recovered, always with the same execution key. Mission changes and closure
wait for executing actions. Once a claim is resolved, superseded, or invalidated, an idempotent
retry cannot replay its payload. Preparing or claiming an action never executes a connector.

## Install and run

Install the locked environment with [uv](https://docs.astral.sh/uv/):

```bash
uv sync
```

Start the trusted control surface, onboard an agent, then bind the ordinary agent surface to the
returned durable identity:

```bash
uv run mission-mcp --surface control --db ./mission.db
uv run mission-mcp --surface agent --agent-id AGENT_ID --db ./mission.db
```

The equivalent environment configuration is useful in MCP client definitions:

```bash
MISSION_MCP_SURFACE=agent MISSION_MCP_AGENT_ID=AGENT_ID \
MISSION_MCP_DB=./mission.db uv run mission-mcp
```

Other useful invocations:

```bash
uv run mission-mcp --surface runtime --db ./mission.db
uv run mission-mcp --surface all --db :memory:
python -m mission_mcp --surface control --db ./mission.db
```

Command-line flags override `MISSION_MCP_SURFACE`, `MISSION_MCP_AGENT_ID`, and `MISSION_MCP_DB`.
The secure default is the `agent` surface, which refuses to start without a valid identity binding.
Use a filesystem database in real use; `:memory:` is intentionally ephemeral.

## MCP surfaces

MCP discovery is authoritative. The current capability split is summarized here.

### `agent`

- Identity and mission: `agent_get`, `mission_get`, `mission_list`, `mission_handoff`,
  `mission_close`, `mission_change_propose`
- External-action planning: `action_prepare`, `action_cancel`
- Board: `board_snapshot`, work-item get/list/create/update/assign, dependency add/remove,
  transition, and archive
- Memory: semantic put/history, episodic append, procedural change proposal/pending list, and
  paged `memory_search`
- Conversation: agent/mission history and `conversation_compact`

It also exposes read-only `mission://{mission_id}/board` and
`mission://{mission_id}/handoff` resources. Mission and target-agent reads are bound to the
authenticated durable agent ID.

### `control`

- `agent_onboard`, `agent_list`
- `account_reference_add`, `account_reference_list`
- `mission_create`, `mission_change_decide`
- `action_decide`
- `procedural_memory_change_list_pending`
- `agent_conversation_append`, `agent_conversation_history`
- `conversation_append`, `audit_list`

Mission creation, human decisions, authenticated conversation writes, and the complete audit stream
remain behind the trusted control boundary.

### `runtime`

- `ready_work_claim`, `ready_work_release`
- `wakeup_claim_due`, `wakeup_resolve`
- `action_redeem`, `action_resolve`

Work, wakeup, and connector claims use leases so another runtime can recover after a crash.

### `all`

The union of all surfaces, intended only for local development or trusted administration.

## Wakeups require a host

A stdio MCP server is passive. Recording `next_check_at` preserves durable intent but does not wake
an agent by itself. A production scheduler should:

1. Call `ready_work_claim` for actionable `doing` cards and resume their assigned agents.
2. Call `wakeup_claim_due` for timed or conditional `waiting` checks.
3. Start the model with recipient-scoped `mission_handoff` context.
4. Release the ready-work lease after the model turn, or resolve the wake as ready or rescheduled.

An incoming human reply normally wakes the chat host directly. Put the relevant `work_item_id` in
`conversation_append.metadata` to correlate a reply with a `needs_input` card.

## Security

- Expose only an identity-bound `agent` surface to a model session.
- Keep `control` behind authenticated human-facing infrastructure.
- Restrict `runtime` to schedulers, workers, and connector gateways.
- Do not expose `all` outside trusted development or administration.
- Keep connector credentials out of persisted mission text, action payloads, memory, conversation,
  evidence, metadata, and account references. Gateways should obtain short-lived credentials from
  their own secret manager.
- Treat the SQLite file as privileged because it contains full history and authoritative state.
- Use separate databases or operating-system isolation when tenants do not share trust.

## Project layout

- `mission_mcp/store.py` — transport-independent domain model and SQLite persistence
- `mission_mcp/server.py` — caller-specific MCP tools, resources, and stdio entrypoint
- `tests/test_mission.py` — domain, privacy, lease, protocol, and real-transport coverage

## Tests and checks

```bash
uv lock --check
uv run python -m unittest discover -s tests -p 'test*.py' -v
uv run --with ruff ruff check mission_mcp tests
uv run --with ruff ruff format --check mission_mcp tests
uv run python -m compileall -q mission_mcp tests
uv run mission-mcp --help
git diff --check
```
