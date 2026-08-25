"""MCP transport for the durable mission store.

The surfaces are capability boundaries, not presentation choices:

* ``agent`` is bound to one durable agent identity.
* ``control`` is for an authenticated human-facing host.
* ``runtime`` is for schedulers, workers, and connector gateways.
* ``all`` is for local development and trusted administration only.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any, Literal

from mcp.server import MCPServer
from mcp.server.mcpserver.exceptions import ResourceError, ToolError
from mcp.types import ToolAnnotations

from .store import DomainError, MissionStore

Surface = Literal["agent", "control", "runtime", "all"]
SURFACES = {"agent", "control", "runtime", "all"}
READ = ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True)
WRITE = ToolAnnotations(readOnlyHint=False, destructiveHint=False, idempotentHint=True)


def _call(method: Any, /, **arguments: Any) -> Any:
    """Turn an anticipated domain failure into a model-readable tool error."""
    try:
        return method(**arguments)
    except DomainError as error:
        raise ToolError(json.dumps(error.as_dict(), separators=(",", ":"))) from error


def _resource_call(method: Any, /, **arguments: Any) -> Any:
    """Turn an anticipated domain failure into an MCP resource error."""
    try:
        return method(**arguments)
    except DomainError as error:
        raise ResourceError(json.dumps(error.as_dict(), separators=(",", ":"))) from error


def build_server(
    store: MissionStore,
    surface: Surface = "agent",
    bound_agent_id: str | None = None,
) -> MCPServer:
    """Build one caller-specific MCP surface over a shared durable store."""
    if surface not in SURFACES:
        raise ValueError(f"Unknown surface: {surface}")
    if surface == "agent" and not bound_agent_id:
        raise ValueError("The agent surface requires a bound_agent_id")
    if surface == "agent":
        try:
            store.agent_get(bound_agent_id)
        except DomainError as error:
            raise ValueError("The bound agent does not exist in this database") from error

    def authorize_actor(actor_id: str) -> None:
        if surface == "agent" and actor_id != bound_agent_id:
            raise ToolError(
                json.dumps(
                    {
                        "code": "FORBIDDEN",
                        "message": "Actor identity does not match this MCP agent session",
                        "details": {"bound_agent_id": bound_agent_id},
                    },
                    separators=(",", ":"),
                )
            )

    def authorize_mission(mission_id: str) -> None:
        if surface == "agent":
            _call(
                store.mission_get_for_agent,
                mission_id=mission_id,
                agent_id=bound_agent_id,
            )

    def authorize_mission_resource(mission_id: str) -> None:
        if surface == "agent":
            _resource_call(
                store.mission_get_for_agent,
                mission_id=mission_id,
                agent_id=bound_agent_id,
            )

    def authorize_work(work_item_id: str) -> None:
        if surface == "agent":
            item = _call(store.work_item_get, work_item_id=work_item_id)
            authorize_mission(item["mission_id"])

    mcp = MCPServer(
        "Mission",
        version="0.1.0",
        description="Durable, model-independent mission state for an executive-assistant agent.",
        instructions=(
            "Treat the board as authoritative and only Doing work as actionable. Every mutation "
            "needs a fresh expected_version where offered and a request-specific idempotency_key. "
            "Waiting needs next_check_at; Blocked needs work-item dependencies; Done needs "
            "verification evidence. Propose protected changes for human approval. Claim external "
            "actions under a runtime lease, execute them with the stable execution_key, and resolve "
            "them with outcome evidence. Close a mission only after checking its overall goal."
        ),
    )

    if surface in {"control", "all"}:

        @mcp.tool(annotations=WRITE)
        def agent_onboard(
            name: str,
            role: str,
            idempotency_key: str,
            manager: str | None = None,
            biography: str = "",
            default_policy: dict[str, Any] | None = None,
            agent_id: str | None = None,
        ) -> dict[str, Any]:
            """Create a durable identity; model and provider names are not identity fields."""
            return _call(
                store.agent_onboard,
                name=name,
                role=role,
                idempotency_key=idempotency_key,
                manager=manager,
                biography=biography,
                default_policy=default_policy,
                agent_id=agent_id,
            )

        @mcp.tool(annotations=READ)
        def agent_list() -> list[dict[str, Any]]:
            """List durable agents at the trusted control boundary."""
            return _call(store.agent_list)

        @mcp.tool(annotations=WRITE)
        def account_reference_add(
            agent_id: str,
            service: str,
            account_ref: str,
            idempotency_key: str,
            scopes: list[str] | None = None,
            metadata: dict[str, Any] | None = None,
        ) -> dict[str, Any]:
            """Attach a non-secret account identifier and scopes during onboarding."""
            return _call(
                store.account_reference_add,
                agent_id=agent_id,
                service=service,
                account_ref=account_ref,
                idempotency_key=idempotency_key,
                scopes=scopes,
                metadata=metadata,
            )

        @mcp.tool(annotations=READ)
        def account_reference_list(agent_id: str) -> list[dict[str, Any]]:
            """List account identifiers and scopes; credentials are never stored here."""
            return _call(store.account_reference_list, agent_id=agent_id)

        @mcp.tool(annotations=WRITE)
        def mission_create(
            owner_agent_id: str,
            title: str,
            goal: str,
            acceptance_criteria: list[str],
            idempotency_key: str,
            constraints: list[str] | None = None,
            action_policy: dict[str, Any] | None = None,
            mission_id: str | None = None,
        ) -> dict[str, Any]:
            """Create an approved mission with exactly one accountable owner."""
            return _call(
                store.mission_create,
                owner_agent_id=owner_agent_id,
                title=title,
                goal=goal,
                constraints=constraints or [],
                acceptance_criteria=acceptance_criteria,
                action_policy=action_policy,
                mission_id=mission_id,
                idempotency_key=idempotency_key,
            )

        @mcp.tool(annotations=WRITE)
        def mission_change_decide(
            change_id: str,
            human_actor: str,
            approve: bool,
            decision_reason: str,
            idempotency_key: str,
        ) -> dict[str, Any]:
            """Approve or reject an exact pending mission or procedure change as a human."""
            return _call(
                store.mission_change_decide,
                change_id=change_id,
                human_actor=human_actor,
                approve=approve,
                decision_reason=decision_reason,
                idempotency_key=idempotency_key,
            )

        @mcp.tool(annotations=WRITE)
        def action_decide(
            action_request_id: str,
            human_actor: str,
            approve: bool,
            decision_reason: str,
            expected_version: int,
            idempotency_key: str,
        ) -> dict[str, Any]:
            """Approve or deny one exact external-action payload as a trusted human."""
            return _call(
                store.action_decide,
                action_request_id=action_request_id,
                human_actor=human_actor,
                approve=approve,
                decision_reason=decision_reason,
                expected_version=expected_version,
                idempotency_key=idempotency_key,
            )

        @mcp.tool(annotations=WRITE)
        def conversation_append(
            mission_id: str,
            role: Literal["user", "assistant", "tool", "system"],
            actor_id: str,
            content: str,
            idempotency_key: str,
            metadata: dict[str, Any] | None = None,
            agent_id: str | None = None,
        ) -> dict[str, Any]:
            """Append an authenticated, immutable mission-conversation message."""
            return _call(
                store.conversation_append,
                mission_id=mission_id,
                role=role,
                actor_id=actor_id,
                content=content,
                idempotency_key=idempotency_key,
                metadata=metadata,
                agent_id=agent_id,
            )

        @mcp.tool(annotations=WRITE)
        def agent_conversation_append(
            agent_id: str,
            role: Literal["user", "assistant", "tool", "system"],
            actor_id: str,
            content: str,
            idempotency_key: str,
            metadata: dict[str, Any] | None = None,
        ) -> dict[str, Any]:
            """Append authenticated raw EA chat outside a particular mission."""
            return _call(
                store.agent_conversation_append,
                agent_id=agent_id,
                role=role,
                actor_id=actor_id,
                content=content,
                idempotency_key=idempotency_key,
                metadata=metadata,
            )

        @mcp.tool(annotations=READ)
        def audit_list(
            mission_id: str | None = None,
            after_seq: int = 0,
            limit: int = 100,
        ) -> list[dict[str, Any]]:
            """Read the append-only audit stream at the trusted control boundary."""
            return _call(store.audit_list, mission_id=mission_id, after_seq=after_seq, limit=limit)

    if surface == "control":

        @mcp.tool(annotations=READ)
        def mission_list(owner_agent_id: str | None = None) -> list[dict[str, Any]]:
            """List missions so the human-facing host can recover its review queues."""
            return _call(store.mission_list, owner_agent_id=owner_agent_id)

        @mcp.tool(annotations=READ)
        def mission_get(mission_id: str) -> dict[str, Any]:
            """Read an exact mission specification at the trusted control boundary."""
            return _call(store.mission_get, mission_id=mission_id)

        @mcp.tool(annotations=READ)
        def board_snapshot(mission_id: str) -> dict[str, Any]:
            """Recover pending changes and exact action payloads for human review."""
            return _call(store.board_snapshot, mission_id=mission_id)

    if surface in {"agent", "all"}:

        @mcp.tool(annotations=READ)
        def agent_get(agent_id: str) -> dict[str, Any]:
            """Read one durable agent identity and charter."""
            authorize_actor(agent_id)
            return _call(store.agent_get, agent_id=agent_id)

        @mcp.tool(annotations=READ)
        def mission_list(owner_agent_id: str | None = None) -> list[dict[str, Any]]:
            """List missions visible to this durable agent."""
            if surface == "agent":
                missions = _call(store.mission_list_for_agent, agent_id=bound_agent_id)
                if owner_agent_id is not None:
                    authorize_actor(owner_agent_id)
                    return [
                        mission
                        for mission in missions
                        if mission["owner_agent_id"] == owner_agent_id
                    ]
                return missions
            return _call(store.mission_list, owner_agent_id=owner_agent_id)

        @mcp.tool(annotations=READ)
        def mission_get(mission_id: str) -> dict[str, Any]:
            """Read one visible mission specification and lifecycle."""
            if surface == "agent":
                return _call(
                    store.mission_get_for_agent,
                    mission_id=mission_id,
                    agent_id=bound_agent_id,
                )
            return _call(store.mission_get, mission_id=mission_id)

        @mcp.tool(annotations=WRITE)
        def mission_change_propose(
            mission_id: str,
            proposed_by: str,
            expected_version: int,
            patch: dict[str, Any],
            reason: str,
            idempotency_key: str,
        ) -> dict[str, Any]:
            """Propose a protected mission change for trusted-human approval."""
            authorize_actor(proposed_by)
            return _call(
                store.mission_change_propose,
                mission_id=mission_id,
                proposed_by=proposed_by,
                expected_version=expected_version,
                patch=patch,
                reason=reason,
                idempotency_key=idempotency_key,
            )

        @mcp.tool(annotations=WRITE)
        def mission_close(
            mission_id: str,
            owner_agent_id: str,
            expected_version: int,
            closure_evidence: list[dict[str, Any]],
            idempotency_key: str,
        ) -> dict[str, Any]:
            """Close a mission after its owner verifies all work and the overall goal."""
            authorize_actor(owner_agent_id)
            return _call(
                store.mission_close,
                mission_id=mission_id,
                owner_agent_id=owner_agent_id,
                expected_version=expected_version,
                closure_evidence=closure_evidence,
                idempotency_key=idempotency_key,
            )

        @mcp.tool(annotations=WRITE)
        def action_prepare(
            mission_id: str,
            agent_id: str,
            action_type: str,
            payload: dict[str, Any],
            idempotency_key: str,
        ) -> dict[str, Any]:
            """Evaluate policy and bind authority to one exact external-action payload."""
            authorize_actor(agent_id)
            return _call(
                store.action_prepare,
                mission_id=mission_id,
                agent_id=agent_id,
                action_type=action_type,
                payload=payload,
                idempotency_key=idempotency_key,
            )

        @mcp.tool(annotations=WRITE)
        def action_cancel(
            action_request_id: str,
            owner_agent_id: str,
            expected_version: int,
            reason: str,
            idempotency_key: str,
        ) -> dict[str, Any]:
            """Abandon a pending or approved external action with a retained reason."""
            authorize_actor(owner_agent_id)
            return _call(
                store.action_cancel,
                action_request_id=action_request_id,
                owner_agent_id=owner_agent_id,
                expected_version=expected_version,
                reason=reason,
                idempotency_key=idempotency_key,
            )

        @mcp.tool(annotations=READ)
        def board_snapshot(mission_id: str) -> dict[str, Any]:
            """Read the authoritative board and its actionable runtime projection."""
            authorize_mission(mission_id)
            return _call(store.board_snapshot, mission_id=mission_id)

        @mcp.tool(annotations=READ)
        def work_item_get(work_item_id: str) -> dict[str, Any]:
            """Read one visible work item with dependencies and optimistic version."""
            authorize_work(work_item_id)
            return _call(store.work_item_get, work_item_id=work_item_id)

        @mcp.tool(annotations=READ)
        def work_item_list(mission_id: str, state: str | None = None) -> list[dict[str, Any]]:
            """List visible mission work, optionally filtered by one work state."""
            authorize_mission(mission_id)
            return _call(store.work_item_list, mission_id=mission_id, state=state)

        @mcp.tool(annotations=WRITE)
        def work_item_create(
            mission_id: str,
            actor_agent_id: str,
            title: str,
            idempotency_key: str,
            description: str = "",
            state: Literal["doing", "waiting", "blocked", "needs_input"] = "doing",
            assignee_agent_id: str | None = None,
            dependency_ids: list[str] | None = None,
            next_check_at: str | None = None,
            wait_condition: str | None = None,
            input_request: str | None = None,
        ) -> dict[str, Any]:
            """Create work while enforcing state-specific data and single-writer ownership."""
            authorize_actor(actor_agent_id)
            return _call(
                store.work_item_create,
                mission_id=mission_id,
                actor_agent_id=actor_agent_id,
                title=title,
                description=description,
                idempotency_key=idempotency_key,
                state=state,
                assignee_agent_id=assignee_agent_id,
                dependency_ids=dependency_ids,
                next_check_at=next_check_at,
                wait_condition=wait_condition,
                input_request=input_request,
            )

        @mcp.tool(annotations=WRITE)
        def work_item_update(
            work_item_id: str,
            actor_agent_id: str,
            expected_version: int,
            idempotency_key: str,
            title: str | None = None,
            description: str | None = None,
        ) -> dict[str, Any]:
            """Update planning metadata as the assigned optimistic writer."""
            authorize_actor(actor_agent_id)
            return _call(
                store.work_item_update,
                work_item_id=work_item_id,
                actor_agent_id=actor_agent_id,
                expected_version=expected_version,
                idempotency_key=idempotency_key,
                title=title,
                description=description,
            )

        @mcp.tool(annotations=WRITE)
        def work_item_assign(
            work_item_id: str,
            owner_agent_id: str,
            assignee_agent_id: str,
            expected_version: int,
            idempotency_key: str,
        ) -> dict[str, Any]:
            """Transfer a card's single-writer assignment while its owner stays accountable."""
            authorize_actor(owner_agent_id)
            return _call(
                store.work_item_assign,
                work_item_id=work_item_id,
                owner_agent_id=owner_agent_id,
                assignee_agent_id=assignee_agent_id,
                expected_version=expected_version,
                idempotency_key=idempotency_key,
            )

        @mcp.tool(annotations=WRITE)
        def work_item_add_dependency(
            work_item_id: str,
            dependency_id: str,
            actor_agent_id: str,
            expected_version: int,
            idempotency_key: str,
        ) -> dict[str, Any]:
            """Add an acyclic, same-mission dependency and block on open work."""
            authorize_actor(actor_agent_id)
            return _call(
                store.work_item_add_dependency,
                work_item_id=work_item_id,
                dependency_id=dependency_id,
                actor_agent_id=actor_agent_id,
                expected_version=expected_version,
                idempotency_key=idempotency_key,
            )

        @mcp.tool(annotations=WRITE)
        def work_item_remove_dependency(
            work_item_id: str,
            dependency_id: str,
            actor_agent_id: str,
            expected_version: int,
            idempotency_key: str,
        ) -> dict[str, Any]:
            """Remove an obsolete dependency and resume a card with no open blockers."""
            authorize_actor(actor_agent_id)
            return _call(
                store.work_item_remove_dependency,
                work_item_id=work_item_id,
                dependency_id=dependency_id,
                actor_agent_id=actor_agent_id,
                expected_version=expected_version,
                idempotency_key=idempotency_key,
            )

        @mcp.tool(annotations=WRITE)
        def work_item_transition(
            work_item_id: str,
            actor_agent_id: str,
            expected_version: int,
            new_state: Literal["doing", "waiting", "blocked", "needs_input", "done"],
            idempotency_key: str,
            next_check_at: str | None = None,
            wait_condition: str | None = None,
            input_request: str | None = None,
            verification_evidence: list[dict[str, Any]] | None = None,
        ) -> dict[str, Any]:
            """Move work through the five states while preserving their invariants."""
            authorize_actor(actor_agent_id)
            return _call(
                store.work_item_transition,
                work_item_id=work_item_id,
                actor_agent_id=actor_agent_id,
                expected_version=expected_version,
                new_state=new_state,
                idempotency_key=idempotency_key,
                next_check_at=next_check_at,
                wait_condition=wait_condition,
                input_request=input_request,
                verification_evidence=verification_evidence,
            )

        @mcp.tool(annotations=WRITE)
        def work_item_archive(
            work_item_id: str,
            owner_agent_id: str,
            expected_version: int,
            reason: str,
            idempotency_key: str,
        ) -> dict[str, Any]:
            """Cancel work from the active board without deleting its history."""
            authorize_actor(owner_agent_id)
            return _call(
                store.work_item_archive,
                work_item_id=work_item_id,
                owner_agent_id=owner_agent_id,
                expected_version=expected_version,
                reason=reason,
                idempotency_key=idempotency_key,
            )

        @mcp.tool(annotations=WRITE)
        def semantic_memory_put(
            agent_id: str,
            actor_agent_id: str,
            key: str,
            content: str,
            provenance: str,
            idempotency_key: str,
            mission_id: str | None = None,
            expected_version: int = 0,
            expires_at: str | None = None,
        ) -> dict[str, Any]:
            """Create or version one semantic fact with provenance and optional expiry."""
            authorize_actor(agent_id)
            authorize_actor(actor_agent_id)
            if mission_id:
                authorize_mission(mission_id)
            return _call(
                store.semantic_memory_put,
                agent_id=agent_id,
                actor_agent_id=actor_agent_id,
                key=key,
                content=content,
                provenance=provenance,
                idempotency_key=idempotency_key,
                mission_id=mission_id,
                expected_version=expected_version,
                expires_at=expires_at,
            )

        @mcp.tool(annotations=READ)
        def semantic_memory_history(
            agent_id: str,
            key: str,
            mission_id: str | None = None,
        ) -> list[dict[str, Any]]:
            """Read every retained version of one semantic-memory key."""
            authorize_actor(agent_id)
            if mission_id:
                authorize_mission(mission_id)
            return _call(
                store.semantic_memory_history,
                agent_id=agent_id,
                key=key,
                mission_id=mission_id,
            )

        @mcp.tool(annotations=WRITE)
        def episodic_memory_append(
            agent_id: str,
            actor_agent_id: str,
            summary: str,
            idempotency_key: str,
            mission_id: str | None = None,
            evidence: list[dict[str, Any]] | None = None,
            tags: list[str] | None = None,
            occurred_at: str | None = None,
        ) -> dict[str, Any]:
            """Append an immutable episode describing what happened and what proves it."""
            authorize_actor(agent_id)
            authorize_actor(actor_agent_id)
            if mission_id:
                authorize_mission(mission_id)
            return _call(
                store.episodic_memory_append,
                agent_id=agent_id,
                actor_agent_id=actor_agent_id,
                summary=summary,
                idempotency_key=idempotency_key,
                mission_id=mission_id,
                evidence=evidence,
                tags=tags,
                occurred_at=occurred_at,
            )

        @mcp.tool(annotations=WRITE)
        def procedural_memory_change_propose(
            agent_id: str,
            proposed_by: str,
            name: str,
            content: str,
            expected_version: int,
            reason: str,
            idempotency_key: str,
            mission_id: str | None = None,
        ) -> dict[str, Any]:
            """Propose a procedure update; a trusted human must decide it."""
            authorize_actor(agent_id)
            authorize_actor(proposed_by)
            if mission_id:
                authorize_mission(mission_id)
            return _call(
                store.procedural_memory_change_propose,
                agent_id=agent_id,
                proposed_by=proposed_by,
                name=name,
                content=content,
                expected_version=expected_version,
                reason=reason,
                idempotency_key=idempotency_key,
                mission_id=mission_id,
            )

        @mcp.tool(annotations=READ)
        def memory_search(
            agent_id: str,
            query: str,
            mission_id: str | None = None,
            limit: int = 20,
            offset: int = 0,
        ) -> dict[str, Any]:
            """Page through current semantic, episodic, and approved procedural memory."""
            authorize_actor(agent_id)
            if mission_id:
                authorize_mission(mission_id)
            return _call(
                store.memory_search,
                agent_id=agent_id,
                query=query,
                mission_id=mission_id,
                limit=limit,
                offset=offset,
            )

        @mcp.tool(annotations=READ)
        def conversation_history(
            mission_id: str,
            after_seq: int = 0,
            limit: int = 100,
        ) -> list[dict[str, Any]]:
            """Read immutable mission conversation using a durable cursor."""
            authorize_mission(mission_id)
            return _call(
                store.conversation_history,
                mission_id=mission_id,
                agent_id=bound_agent_id if surface == "agent" else None,
                after_seq=after_seq,
                limit=limit,
            )

        @mcp.tool(annotations=WRITE)
        def conversation_compact(
            mission_id: str,
            owner_agent_id: str,
            through_seq: int,
            summary: str,
            idempotency_key: str,
        ) -> dict[str, Any]:
            """Add an owner-authored checkpoint without changing retained raw messages."""
            authorize_actor(owner_agent_id)
            authorize_mission(mission_id)
            return _call(
                store.conversation_compact,
                mission_id=mission_id,
                owner_agent_id=owner_agent_id,
                through_seq=through_seq,
                summary=summary,
                idempotency_key=idempotency_key,
            )

        @mcp.tool(annotations=READ)
        def mission_handoff(
            mission_id: str,
            conversation_tail: int = 50,
            memory_limit: int = 20,
        ) -> dict[str, Any]:
            """Build bounded resumption context for this participating durable agent."""
            authorize_mission(mission_id)
            return _call(
                store.mission_handoff,
                mission_id=mission_id,
                agent_id=bound_agent_id if surface == "agent" else None,
                conversation_tail=conversation_tail,
                memory_limit=memory_limit,
            )

        @mcp.resource("mission://{mission_id}/board", mime_type="application/json")
        def board_resource(mission_id: str) -> str:
            """Authoritative current board projection."""
            authorize_mission_resource(mission_id)
            return json.dumps(
                _resource_call(store.board_snapshot, mission_id=mission_id),
                separators=(",", ":"),
            )

        @mcp.resource("mission://{mission_id}/handoff", mime_type="application/json")
        def handoff_resource(mission_id: str) -> str:
            """Recipient-scoped, model-independent mission handoff package."""
            authorize_mission_resource(mission_id)
            return json.dumps(
                _resource_call(
                    store.mission_handoff,
                    mission_id=mission_id,
                    agent_id=bound_agent_id if surface == "agent" else None,
                ),
                separators=(",", ":"),
            )

    if surface in {"agent", "control", "all"}:

        @mcp.tool(annotations=READ)
        def procedural_memory_change_list_pending(
            agent_id: str,
            mission_id: str | None = None,
        ) -> list[dict[str, Any]]:
            """List one agent's private pending procedure proposals for recovery or review."""
            if surface == "agent":
                authorize_actor(agent_id)
                if mission_id:
                    authorize_mission(mission_id)
            return _call(
                store.procedural_memory_change_list_pending,
                agent_id=agent_id,
                mission_id=mission_id,
            )

        @mcp.tool(annotations=READ)
        def agent_conversation_history(
            agent_id: str,
            after_seq: int = 0,
            limit: int = 100,
        ) -> list[dict[str, Any]]:
            """Read immutable non-mission conversation for one durable agent."""
            if surface == "agent":
                authorize_actor(agent_id)
            return _call(
                store.agent_conversation_history,
                agent_id=agent_id,
                after_seq=after_seq,
                limit=limit,
            )

    if surface in {"runtime", "all"}:

        @mcp.tool(annotations=WRITE)
        def ready_work_claim(
            worker_id: str,
            idempotency_key: str,
            limit: int = 10,
            lease_seconds: int = 300,
        ) -> dict[str, Any]:
            """Lease Doing cards so one runtime execution wakes each card."""
            return _call(
                store.ready_work_claim,
                worker_id=worker_id,
                idempotency_key=idempotency_key,
                limit=limit,
                lease_seconds=lease_seconds,
            )

        @mcp.tool(annotations=WRITE)
        def ready_work_release(
            claim_id: str,
            worker_id: str,
            expected_version: int,
            idempotency_key: str,
        ) -> dict[str, Any]:
            """Release a ready-work lease after its model turn or handoff."""
            return _call(
                store.ready_work_release,
                claim_id=claim_id,
                worker_id=worker_id,
                expected_version=expected_version,
                idempotency_key=idempotency_key,
            )

        @mcp.tool(annotations=WRITE)
        def wakeup_claim_due(
            worker_id: str,
            idempotency_key: str,
            limit: int = 10,
            lease_seconds: int = 300,
        ) -> dict[str, Any]:
            """Claim due or expired wakeups under a bounded scheduler lease."""
            return _call(
                store.wakeup_claim_due,
                worker_id=worker_id,
                idempotency_key=idempotency_key,
                limit=limit,
                lease_seconds=lease_seconds,
            )

        @mcp.tool(annotations=WRITE)
        def wakeup_resolve(
            wakeup_id: str,
            worker_id: str,
            expected_version: int,
            ready: bool,
            idempotency_key: str,
            next_check_at: str | None = None,
            condition: str | None = None,
        ) -> dict[str, Any]:
            """Resume ready work or schedule its next condition check."""
            return _call(
                store.wakeup_resolve,
                wakeup_id=wakeup_id,
                worker_id=worker_id,
                expected_version=expected_version,
                ready=ready,
                idempotency_key=idempotency_key,
                next_check_at=next_check_at,
                condition=condition,
            )

        @mcp.tool(annotations=WRITE)
        def action_redeem(
            action_request_id: str,
            gateway_id: str,
            expected_version: int,
            idempotency_key: str,
            lease_seconds: int = 300,
        ) -> dict[str, Any]:
            """Claim an approved exact payload for connector execution under a lease."""
            return _call(
                store.action_redeem,
                action_request_id=action_request_id,
                gateway_id=gateway_id,
                expected_version=expected_version,
                idempotency_key=idempotency_key,
                lease_seconds=lease_seconds,
            )

        @mcp.tool(annotations=WRITE)
        def action_resolve(
            action_request_id: str,
            gateway_id: str,
            expected_version: int,
            success: bool,
            outcome_evidence: list[dict[str, Any]],
            idempotency_key: str,
        ) -> dict[str, Any]:
            """Record an executing connector claim's success or failure with evidence."""
            return _call(
                store.action_resolve,
                action_request_id=action_request_id,
                gateway_id=gateway_id,
                expected_version=expected_version,
                success=success,
                outcome_evidence=outcome_evidence,
                idempotency_key=idempotency_key,
            )

    return mcp


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI flags, with environment variables as deployment-friendly defaults."""
    parser = argparse.ArgumentParser(description="Run Mission MCP over stdio.")
    parser.add_argument(
        "--surface",
        choices=sorted(SURFACES),
        default=os.getenv("MISSION_MCP_SURFACE", "agent"),
    )
    parser.add_argument("--db", default=os.getenv("MISSION_MCP_DB", ".data/mission.db"))
    parser.add_argument("--agent-id", default=os.getenv("MISSION_MCP_AGENT_ID"))
    args = parser.parse_args(argv)
    if args.surface == "agent" and not args.agent_id:
        parser.error("--agent-id (or MISSION_MCP_AGENT_ID) is required for the agent surface")
    return args


def main() -> None:
    """Open the shared store and serve the selected MCP surface over stdio."""
    args = parse_args()
    db_path = args.db
    if db_path != ":memory:":
        db_path = str(Path(db_path).expanduser().resolve())
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    store = MissionStore(db_path)
    try:
        build_server(store, args.surface, args.agent_id).run()
    finally:
        store.close()


if __name__ == "__main__":
    main()
