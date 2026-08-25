from __future__ import annotations

import json
import sys
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path

from mcp import Client, StdioServerParameters

from mission_mcp import DomainError, MissionStore
from mission_mcp.server import build_server


class MutableClock:
    def __init__(self) -> None:
        self.value = datetime(2026, 8, 24, 12, 0, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self.value

    def advance(self, **delta: int) -> None:
        self.value += timedelta(**delta)


class MissionStoreTest(unittest.TestCase):
    def setUp(self) -> None:
        self._keys = 0

    def key(self) -> str:
        self._keys += 1
        return f"test-{self._keys}"

    def context(self):
        self.clock = MutableClock()
        store = MissionStore(clock=self.clock)
        agent = store.agent_onboard(
            name="Avery",
            role="Executive assistant",
            idempotency_key=self.key(),
            agent_id="agent-owner",
        )["agent"]
        mission = store.mission_create(
            owner_agent_id=agent["id"],
            title="Plan retreat",
            goal="Deliver a verified retreat plan",
            constraints=["Stay on budget"],
            acceptance_criteria=["Venue and agenda are confirmed"],
            idempotency_key=self.key(),
            mission_id="mission-retreat",
        )["mission"]
        return store, agent, mission

    def create_work(self, store, agent, mission, title: str = "Book venue", **values):
        arguments = {
            "mission_id": mission["id"],
            "actor_agent_id": agent["id"],
            "title": title,
            "description": "Complete the task",
            "idempotency_key": self.key(),
        }
        arguments.update(values)
        return store.work_item_create(**arguments)["work_item"]

    def assert_domain_error(self, code: str, function, /, **arguments) -> None:
        with self.assertRaises(DomainError) as caught:
            function(**arguments)
        self.assertEqual(caught.exception.code, code)

    def test_agent_and_mission_survive_store_restart(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "mission.db")
            with MissionStore(path, clock=MutableClock()) as first:
                agent = first.agent_onboard(
                    name="Durable agent",
                    role="Executive assistant",
                    idempotency_key="durable-agent",
                )["agent"]
                mission = first.mission_create(
                    owner_agent_id=agent["id"],
                    title="Durable mission",
                    goal="Outlive this process",
                    constraints=[],
                    acceptance_criteria=["State reloads"],
                    idempotency_key="durable-mission",
                )["mission"]

            with MissionStore(path, clock=MutableClock()) as reopened:
                self.assertEqual(reopened.agent_get(agent["id"])["id"], agent["id"])
                loaded = reopened.mission_get(mission["id"])
                self.assertEqual(loaded["owner_agent_id"], agent["id"])
                self.assertEqual(loaded["goal"], "Outlive this process")

    def test_account_references_accept_identifiers_but_reject_secrets(self) -> None:
        with MissionStore(clock=MutableClock()) as store:
            agent = store.agent_onboard(
                name="Avery",
                role="Executive assistant",
                idempotency_key="agent",
            )["agent"]
            reference = store.account_reference_add(
                agent_id=agent["id"],
                service="gmail",
                account_ref="davis@example.com",
                scopes=["mail.send"],
                metadata={"workspace": "personal"},
                idempotency_key="safe-account",
            )["account_reference"]
            self.assertEqual(reference["account_ref"], "davis@example.com")
            self.assertEqual(reference["scopes"], ["mail.send"])

            unsafe_metadata = [
                {"oauth": {"api_token": "must-not-be-stored"}},
                {"secret_key": "must-not-be-stored"},
                {"aws_secret_access_key": "must-not-be-stored"},
                {"access_key_id": "must-not-be-stored"},
            ]
            for index, metadata in enumerate(unsafe_metadata):
                with self.subTest(metadata=metadata), self.assertRaises(DomainError) as caught:
                    store.account_reference_add(
                        agent_id=agent["id"],
                        service="gmail",
                        account_ref=f"unsafe-reference-{index}",
                        metadata=metadata,
                        idempotency_key=f"unsafe-account-{index}",
                    )
                self.assertEqual(caught.exception.code, "SECRET_REJECTED")
            self.assertEqual(len(store.account_reference_list(agent["id"])), 1)

    def test_mission_inherits_a_validated_onboarding_action_policy(self) -> None:
        policy = {
            "default": "allow",
            "rules": [{"action": "email.*", "effect": "require_approval"}],
        }
        with MissionStore(clock=MutableClock()) as store:
            agent = store.agent_onboard(
                name="Protected agent",
                role="Executive assistant",
                default_policy=policy,
                idempotency_key="protected-agent",
            )["agent"]
            mission = store.mission_create(
                owner_agent_id=agent["id"],
                title="Inherited policy",
                goal="Use the onboarding policy",
                constraints=[],
                acceptance_criteria=["Policy is inherited"],
                idempotency_key="policy-mission",
            )["mission"]
            self.assertEqual(mission["action_policy"], policy)

            with self.assertRaises(DomainError) as caught:
                store.agent_onboard(
                    name="Invalid policy",
                    role="Executive assistant",
                    default_policy={},
                    idempotency_key="invalid-policy",
                )
            self.assertEqual(caught.exception.code, "VALIDATION_ERROR")

    def test_stale_writes_assignment_and_participant_reads(self) -> None:
        store, agent, mission = self.context()
        with store:
            item = self.create_work(store, agent, mission)
            updated = store.work_item_update(
                work_item_id=item["id"],
                actor_agent_id=agent["id"],
                expected_version=item["version"],
                title="Book an accessible venue",
                idempotency_key=self.key(),
            )["work_item"]
            self.assertEqual(updated["version"], 2)
            self.assert_domain_error(
                "VERSION_CONFLICT",
                store.work_item_update,
                work_item_id=item["id"],
                actor_agent_id=agent["id"],
                expected_version=item["version"],
                description="Stale overwrite",
                idempotency_key=self.key(),
            )

            delegate = store.agent_onboard(
                name="Jordan",
                role="Venue specialist",
                idempotency_key=self.key(),
                agent_id="agent-delegate",
            )["agent"]
            assigned = store.work_item_assign(
                work_item_id=item["id"],
                owner_agent_id=agent["id"],
                assignee_agent_id=delegate["id"],
                expected_version=updated["version"],
                idempotency_key=self.key(),
            )["work_item"]
            participant_view = store.mission_get_for_agent(mission["id"], delegate["id"])
            self.assertEqual(participant_view["id"], mission["id"])
            self.assertEqual(participant_view["owner_agent_id"], agent["id"])
            self.assertEqual(
                [record["id"] for record in store.mission_list_for_agent(delegate["id"])],
                [mission["id"]],
            )
            self.assert_domain_error(
                "WRITER_CONFLICT",
                store.work_item_update,
                work_item_id=item["id"],
                actor_agent_id=agent["id"],
                expected_version=assigned["version"],
                description="Owner is no longer the writer",
                idempotency_key=self.key(),
            )
            delegated_update = store.work_item_update(
                work_item_id=item["id"],
                actor_agent_id=delegate["id"],
                expected_version=assigned["version"],
                description="Delegate owns this write",
                idempotency_key=self.key(),
            )["work_item"]
            self.assertEqual(store.work_item_get(item["id"]), delegated_update)
            self.assertEqual(store.work_item_list(mission["id"]), [delegated_update])

    def test_waiting_and_needs_input_require_state_data_and_wake_resolution(self) -> None:
        store, agent, mission = self.context()
        with store:
            self.assert_domain_error(
                "INVALID_STATE",
                store.work_item_create,
                mission_id=mission["id"],
                actor_agent_id=agent["id"],
                title="Wait for host",
                state="waiting",
                idempotency_key=self.key(),
            )
            self.assert_domain_error(
                "INVALID_STATE",
                store.work_item_create,
                mission_id=mission["id"],
                actor_agent_id=agent["id"],
                title="Ask the user",
                state="needs_input",
                idempotency_key=self.key(),
            )
            needs_input = self.create_work(
                store,
                agent,
                mission,
                "Choose a venue",
                state="needs_input",
                input_request="Which venue do you prefer?",
            )
            self.assertEqual(needs_input["state"], "needs_input")

            due = "2026-08-24T12:05:00Z"
            waiting = store.work_item_create(
                mission_id=mission["id"],
                actor_agent_id=agent["id"],
                title="Wait for host",
                state="waiting",
                next_check_at=due,
                wait_condition="Host replied",
                idempotency_key=self.key(),
            )
            self.assertEqual(waiting["work_item"]["next_check_at"], due)
            self.assertEqual(len(waiting["wake_ids"]), 1)
            self.assertEqual(
                store.wakeup_claim_due(worker_id="scheduler", idempotency_key=self.key())[
                    "wakeups"
                ],
                [],
            )
            self.clock.advance(minutes=6)
            claimed = store.wakeup_claim_due(
                worker_id="scheduler",
                lease_seconds=60,
                idempotency_key=self.key(),
            )["wakeups"][0]
            self.assert_domain_error(
                "LEASE_CONFLICT",
                store.wakeup_resolve,
                wakeup_id=claimed["id"],
                worker_id="other-scheduler",
                expected_version=claimed["version"],
                ready=True,
                idempotency_key=self.key(),
            )
            resolved = store.wakeup_resolve(
                wakeup_id=claimed["id"],
                worker_id="scheduler",
                expected_version=claimed["version"],
                ready=True,
                idempotency_key=self.key(),
            )
            self.assertEqual(resolved["work_item"]["state"], "doing")
            self.assertEqual(store.board_snapshot(mission["id"])["wakeups"], [])

    def test_runtime_leases_doing_work_without_duplicate_dispatch(self) -> None:
        store, agent, mission = self.context()
        with store:
            item = self.create_work(store, agent, mission, "Resume after a restart")
            first = store.ready_work_claim(
                worker_id="runtime-1",
                limit=1,
                lease_seconds=60,
                idempotency_key=self.key(),
            )["claims"][0]
            self.assertEqual(first["work_item"]["id"], item["id"])
            self.assertEqual(
                store.ready_work_claim(
                    worker_id="runtime-2",
                    limit=1,
                    lease_seconds=60,
                    idempotency_key=self.key(),
                )["claims"],
                [],
            )
            released = store.ready_work_release(
                claim_id=first["claim"]["id"],
                worker_id="runtime-1",
                expected_version=first["claim"]["version"],
                idempotency_key=self.key(),
            )["claim"]
            self.assertEqual(released["status"], "released")
            reclaimed = store.ready_work_claim(
                worker_id="runtime-2",
                limit=1,
                lease_seconds=60,
                idempotency_key=self.key(),
            )["claims"][0]
            self.assertEqual(reclaimed["work_item"]["id"], item["id"])

    def test_terminal_state_archive_and_reassignment_revoke_execution_leases(self) -> None:
        store, agent, mission = self.context()
        with store:
            completed_item = self.create_work(store, agent, mission, "Complete claimed work")
            completed_claim = store.ready_work_claim(
                worker_id="runtime",
                limit=1,
                lease_seconds=3600,
                idempotency_key=self.key(),
            )["claims"][0]["claim"]
            store.work_item_transition(
                work_item_id=completed_item["id"],
                actor_agent_id=agent["id"],
                expected_version=completed_item["version"],
                new_state="done",
                verification_evidence=[{"summary": "Claimed work verified"}],
                idempotency_key=self.key(),
            )

            delegate = store.agent_onboard(
                name="Jordan",
                role="Specialist",
                idempotency_key=self.key(),
            )["agent"]
            reassigned_item = self.create_work(store, agent, mission, "Reassign claimed work")
            reassigned_claim = store.ready_work_claim(
                worker_id="runtime",
                limit=1,
                lease_seconds=3600,
                idempotency_key=self.key(),
            )["claims"][0]["claim"]
            reassigned_item = store.work_item_assign(
                work_item_id=reassigned_item["id"],
                owner_agent_id=agent["id"],
                assignee_agent_id=delegate["id"],
                expected_version=reassigned_item["version"],
                idempotency_key=self.key(),
            )["work_item"]
            store.work_item_archive(
                work_item_id=reassigned_item["id"],
                owner_agent_id=agent["id"],
                expected_version=reassigned_item["version"],
                reason="The delegated work is no longer needed",
                idempotency_key=self.key(),
            )

            archived_item = self.create_work(store, agent, mission, "Archive claimed work")
            archived_claim = store.ready_work_claim(
                worker_id="runtime",
                limit=1,
                lease_seconds=3600,
                idempotency_key=self.key(),
            )["claims"][0]["claim"]
            store.work_item_archive(
                work_item_id=archived_item["id"],
                owner_agent_id=agent["id"],
                expected_version=archived_item["version"],
                reason="The user cancelled this work",
                idempotency_key=self.key(),
            )

            for claim in (completed_claim, reassigned_claim, archived_claim):
                status = store.db.execute(
                    "SELECT status FROM execution_claims WHERE id=?", (claim["id"],)
                ).fetchone()["status"]
                self.assertEqual(status, "released")
            self.assertEqual(store.board_snapshot(mission["id"])["execution_claims"], [])

            closed = store.mission_close(
                mission_id=mission["id"],
                owner_agent_id=agent["id"],
                expected_version=mission["version"],
                closure_evidence=[{"summary": "All remaining work was verified or cancelled"}],
                idempotency_key=self.key(),
            )["mission"]
            self.assertEqual(closed["status"], "closed")
            self.assert_domain_error(
                "MISSION_CLOSED",
                store.ready_work_release,
                claim_id=completed_claim["id"],
                worker_id="runtime",
                expected_version=completed_claim["version"],
                idempotency_key=self.key(),
            )

    def test_dependency_scope_initial_state_and_cycle_validation(self) -> None:
        store, agent, mission = self.context()
        with store:
            blocker = self.create_work(store, agent, mission, "Confirm budget")
            self.assert_domain_error(
                "INVALID_STATE",
                store.work_item_create,
                mission_id=mission["id"],
                actor_agent_id=agent["id"],
                title="Incorrectly actionable",
                dependency_ids=[blocker["id"]],
                state="doing",
                idempotency_key=self.key(),
            )
            self.assert_domain_error(
                "INVALID_STATE",
                store.work_item_create,
                mission_id=mission["id"],
                actor_agent_id=agent["id"],
                title="Incorrectly waiting",
                dependency_ids=[blocker["id"]],
                state="waiting",
                next_check_at="2026-08-24T12:05:00Z",
                idempotency_key=self.key(),
            )
            self.assert_domain_error(
                "INVALID_STATE",
                store.work_item_create,
                mission_id=mission["id"],
                actor_agent_id=agent["id"],
                title="Blocked without dependency",
                state="blocked",
                idempotency_key=self.key(),
            )
            dependent = self.create_work(
                store,
                agent,
                mission,
                "Book venue",
                state="blocked",
                dependency_ids=[blocker["id"]],
            )
            self.assert_domain_error(
                "DEPENDENCY_CYCLE",
                store.work_item_add_dependency,
                work_item_id=blocker["id"],
                dependency_id=dependent["id"],
                actor_agent_id=agent["id"],
                expected_version=blocker["version"],
                idempotency_key=self.key(),
            )

            other_mission = store.mission_create(
                owner_agent_id=agent["id"],
                title="Other mission",
                goal="Remain separate",
                constraints=[],
                acceptance_criteria=["No cross-scope edge"],
                idempotency_key=self.key(),
            )["mission"]
            other = store.work_item_create(
                mission_id=other_mission["id"],
                actor_agent_id=agent["id"],
                title="Other work",
                idempotency_key=self.key(),
            )["work_item"]
            self.assert_domain_error(
                "OUT_OF_SCOPE",
                store.work_item_add_dependency,
                work_item_id=dependent["id"],
                dependency_id=other["id"],
                actor_agent_id=agent["id"],
                expected_version=dependent["version"],
                idempotency_key=self.key(),
            )

    def test_dependency_add_remove_and_completion_unblock(self) -> None:
        store, agent, mission = self.context()
        with store:
            blocker = self.create_work(store, agent, mission, "Required approval")
            dependent = self.create_work(store, agent, mission, "Act after approval")
            blocked = store.work_item_add_dependency(
                work_item_id=dependent["id"],
                dependency_id=blocker["id"],
                actor_agent_id=agent["id"],
                expected_version=dependent["version"],
                idempotency_key=self.key(),
            )["work_item"]
            self.assertEqual(blocked["state"], "blocked")
            self.assert_domain_error(
                "PRECONDITION_FAILED",
                store.work_item_transition,
                work_item_id=dependent["id"],
                actor_agent_id=agent["id"],
                expected_version=blocked["version"],
                new_state="done",
                verification_evidence=[{"summary": "Attempted bypass"}],
                idempotency_key=self.key(),
            )
            resumed = store.work_item_remove_dependency(
                work_item_id=dependent["id"],
                dependency_id=blocker["id"],
                actor_agent_id=agent["id"],
                expected_version=blocked["version"],
                idempotency_key=self.key(),
            )
            self.assertTrue(resumed["became_actionable"])
            reblocked = store.work_item_add_dependency(
                work_item_id=dependent["id"],
                dependency_id=blocker["id"],
                actor_agent_id=agent["id"],
                expected_version=resumed["work_item"]["version"],
                idempotency_key=self.key(),
            )["work_item"]
            completed = store.work_item_transition(
                work_item_id=blocker["id"],
                actor_agent_id=agent["id"],
                expected_version=blocker["version"],
                new_state="done",
                verification_evidence=[{"summary": "Approval recorded"}],
                idempotency_key=self.key(),
            )
            self.assertEqual(completed["unblocked_work_item_ids"], [dependent["id"]])
            self.assertEqual(reblocked["state"], "blocked")
            self.assertEqual(store.work_item_get(dependent["id"])["state"], "doing")

    def test_reopening_done_prerequisite_recursively_reblocks_done_dependents(self) -> None:
        store, agent, mission = self.context()
        with store:
            prerequisite = self.create_work(store, agent, mission, "Confirm source")
            middle = self.create_work(
                store,
                agent,
                mission,
                "Use source",
                state="blocked",
                dependency_ids=[prerequisite["id"]],
            )
            final = self.create_work(
                store,
                agent,
                mission,
                "Publish plan",
                state="blocked",
                dependency_ids=[middle["id"]],
            )
            prerequisite = store.work_item_transition(
                work_item_id=prerequisite["id"],
                actor_agent_id=agent["id"],
                expected_version=prerequisite["version"],
                new_state="done",
                verification_evidence=[{"summary": "Source confirmed"}],
                idempotency_key=self.key(),
            )["work_item"]
            middle = store.work_item_get(middle["id"])
            store.work_item_transition(
                work_item_id=middle["id"],
                actor_agent_id=agent["id"],
                expected_version=middle["version"],
                new_state="done",
                verification_evidence=[{"summary": "Source used"}],
                idempotency_key=self.key(),
            )
            final = store.work_item_get(final["id"])
            store.work_item_transition(
                work_item_id=final["id"],
                actor_agent_id=agent["id"],
                expected_version=final["version"],
                new_state="done",
                verification_evidence=[{"summary": "Plan published"}],
                idempotency_key=self.key(),
            )

            reopened = store.work_item_transition(
                work_item_id=prerequisite["id"],
                actor_agent_id=agent["id"],
                expected_version=prerequisite["version"],
                new_state="doing",
                idempotency_key=self.key(),
            )
            self.assertCountEqual(
                reopened["reblocked_work_item_ids"],
                [middle["id"], final["id"]],
            )
            self.assertEqual(store.work_item_get(middle["id"])["state"], "blocked")
            self.assertEqual(store.work_item_get(final["id"])["state"], "blocked")
            self.assert_domain_error(
                "PRECONDITION_FAILED",
                store.work_item_archive,
                work_item_id=prerequisite["id"],
                owner_agent_id=agent["id"],
                expected_version=reopened["work_item"]["version"],
                reason="Attempted dependency bypass",
                idempotency_key=self.key(),
            )

    def test_archive_is_reasoned_cancellation_and_protects_history(self) -> None:
        store, agent, mission = self.context()
        with store:
            item = self.create_work(store, agent, mission, "Optional research")
            self.assert_domain_error(
                "VALIDATION_ERROR",
                store.work_item_archive,
                work_item_id=item["id"],
                owner_agent_id=agent["id"],
                expected_version=item["version"],
                reason="",
                idempotency_key=self.key(),
            )
            archived = store.work_item_archive(
                work_item_id=item["id"],
                owner_agent_id=agent["id"],
                expected_version=item["version"],
                reason="The user removed this option",
                idempotency_key=self.key(),
            )["work_item"]
            self.assertTrue(archived["archived"])
            self.assertEqual(archived["archive_reason"], "The user removed this option")
            self.assert_domain_error(
                "WORK_ARCHIVED",
                store.work_item_update,
                work_item_id=item["id"],
                actor_agent_id=agent["id"],
                expected_version=archived["version"],
                description="Archived work is immutable",
                idempotency_key=self.key(),
            )
            self.assertEqual(store.work_item_get(item["id"]), archived)

    def test_done_and_mission_closure_require_concrete_evidence(self) -> None:
        store, agent, mission = self.context()
        with store:
            item = self.create_work(store, agent, mission)
            self.assert_domain_error(
                "INVALID_STATE",
                store.work_item_transition,
                work_item_id=item["id"],
                actor_agent_id=agent["id"],
                expected_version=item["version"],
                new_state="done",
                verification_evidence=[{}],
                idempotency_key=self.key(),
            )
            self.assert_domain_error(
                "PRECONDITION_FAILED",
                store.mission_close,
                mission_id=mission["id"],
                owner_agent_id=agent["id"],
                expected_version=mission["version"],
                closure_evidence=[{"summary": "Too early"}],
                idempotency_key=self.key(),
            )
            completed = store.work_item_transition(
                work_item_id=item["id"],
                actor_agent_id=agent["id"],
                expected_version=item["version"],
                new_state="done",
                verification_evidence=[{"summary": "Venue confirmed"}],
                idempotency_key=self.key(),
            )["work_item"]
            self.assertEqual(completed["verification_evidence"][0]["summary"], "Venue confirmed")
            self.assert_domain_error(
                "VALIDATION_ERROR",
                store.mission_close,
                mission_id=mission["id"],
                owner_agent_id=agent["id"],
                expected_version=mission["version"],
                closure_evidence=[],
                idempotency_key=self.key(),
            )
            closed = store.mission_close(
                mission_id=mission["id"],
                owner_agent_id=agent["id"],
                expected_version=mission["version"],
                closure_evidence=[{"summary": "Overall goal checked"}],
                idempotency_key=self.key(),
            )["mission"]
            self.assertEqual(closed["status"], "closed")
            self.assertEqual(closed["closure_evidence"][0]["summary"], "Overall goal checked")

    def test_idempotent_retry_does_not_duplicate_work_or_events(self) -> None:
        store, agent, mission = self.context()
        with store:
            arguments = {
                "mission_id": mission["id"],
                "actor_agent_id": agent["id"],
                "title": "Retry-safe task",
                "description": "Create once",
                "idempotency_key": "same-work-request",
            }
            first = store.work_item_create(**arguments)
            self.assertEqual(store.work_item_create(**arguments), first)
            self.assert_domain_error(
                "IDEMPOTENCY_CONFLICT",
                store.work_item_create,
                **{**arguments, "title": "Different request"},
            )
            self.assertEqual(len(store.work_item_list(mission["id"])), 1)
            events = [
                event
                for event in store.audit_list(mission["id"])
                if event["type"] == "work.created"
            ]
            self.assertEqual(len(events), 1)

    def test_mission_change_waits_for_human_and_stale_proposal_conflicts(self) -> None:
        store, agent, mission = self.context()
        with store:
            first = store.mission_change_propose(
                mission_id=mission["id"],
                proposed_by=agent["id"],
                expected_version=mission["version"],
                patch={"goal": "Deliver an approved retreat plan"},
                reason="Approval is now required",
                idempotency_key=self.key(),
            )["change"]
            stale = store.mission_change_propose(
                mission_id=mission["id"],
                proposed_by=agent["id"],
                expected_version=mission["version"],
                patch={"constraints": ["Stay on budget", "Use accessible venues"]},
                reason="Accessibility requirement",
                idempotency_key=self.key(),
            )["change"]
            self.assertEqual(store.mission_get(mission["id"])["goal"], mission["goal"])
            self.assertCountEqual(
                [change["id"] for change in store.board_snapshot(mission["id"])["pending_changes"]],
                [first["id"], stale["id"]],
            )

            approved = store.mission_change_decide(
                change_id=first["id"],
                human_actor="human:davis",
                approve=True,
                decision_reason="Approved",
                idempotency_key=self.key(),
            )
            self.assertEqual(approved["change"]["status"], "approved")
            changed = store.mission_get(mission["id"])
            self.assertEqual(changed["goal"], "Deliver an approved retreat plan")
            self.assertEqual(changed["version"], mission["version"] + 1)
            self.assert_domain_error(
                "VERSION_CONFLICT",
                store.mission_change_decide,
                change_id=stale["id"],
                human_actor="human:davis",
                approve=True,
                decision_reason="Too late",
                idempotency_key=self.key(),
            )

    def test_action_approval_and_idempotent_execution_resolution(self) -> None:
        store, agent, _ = self.context()
        with store:
            protected = store.mission_create(
                owner_agent_id=agent["id"],
                title="Send invitations",
                goal="Invite attendees",
                constraints=[],
                acceptance_criteria=["Invitations sent"],
                action_policy={
                    "default": "allow",
                    "rules": [
                        {"action": "email.*", "effect": "require_approval"},
                        {"action": "email.send", "effect": "deny"},
                    ],
                },
                idempotency_key=self.key(),
            )["mission"]
            payload = {
                "to": "guest@example.com",
                "subject": "Retreat",
                "body": "Please join",
            }
            action = store.action_prepare(
                mission_id=protected["id"],
                agent_id=agent["id"],
                action_type="email.send",
                payload=payload,
                idempotency_key=self.key(),
            )["action_request"]
            self.assertEqual(
                (action["effect"], action["status"]),
                ("require_approval", "pending"),
            )
            self.assert_domain_error(
                "PRECONDITION_FAILED",
                store.action_redeem,
                action_request_id=action["id"],
                gateway_id="gateway:email",
                expected_version=action["version"],
                idempotency_key=self.key(),
            )

            approved = store.action_decide(
                action_request_id=action["id"],
                human_actor="human:davis",
                approve=True,
                decision_reason="Recipient and message verified",
                expected_version=action["version"],
                idempotency_key=self.key(),
            )["action_request"]
            redeem_key = self.key()
            claimed = store.action_redeem(
                action_request_id=action["id"],
                gateway_id="gateway:email",
                expected_version=approved["version"],
                idempotency_key=redeem_key,
            )
            self.assertEqual(claimed["permit"]["payload"], payload)
            self.assertEqual(claimed["permit"]["execution_key"], action["id"])
            self.assertEqual(claimed["action_request"]["status"], "executing")
            self.assertEqual(
                store.action_redeem(
                    action_request_id=action["id"],
                    gateway_id="gateway:email",
                    expected_version=approved["version"],
                    idempotency_key=redeem_key,
                ),
                claimed,
            )
            self.assert_domain_error(
                "PRECONDITION_FAILED",
                store.action_redeem,
                action_request_id=action["id"],
                gateway_id="gateway:email",
                expected_version=approved["version"] + 1,
                idempotency_key=self.key(),
            )
            resolved = store.action_resolve(
                action_request_id=action["id"],
                gateway_id="gateway:email",
                expected_version=claimed["action_request"]["version"],
                success=True,
                outcome_evidence=[{"connector_message_id": "email-123"}],
                idempotency_key=self.key(),
            )["action_request"]
            self.assertEqual(resolved["status"], "completed")
            self.assertEqual(
                resolved["outcome_evidence"],
                [{"connector_message_id": "email-123"}],
            )
            self.assert_domain_error(
                "PRECONDITION_FAILED",
                store.action_redeem,
                action_request_id=action["id"],
                gateway_id="gateway:email",
                expected_version=approved["version"],
                idempotency_key=redeem_key,
            )

    def test_action_replay_is_bound_to_one_lease_generation(self) -> None:
        store, agent, mission = self.context()
        with store:
            action = store.action_prepare(
                mission_id=mission["id"],
                agent_id=agent["id"],
                action_type="email.send",
                payload={"to": "guest@example.com"},
                idempotency_key=self.key(),
            )["action_request"]
            first_key = self.key()
            store.action_redeem(
                action_request_id=action["id"],
                gateway_id="gateway:a",
                expected_version=action["version"],
                lease_seconds=1,
                idempotency_key=first_key,
            )
            self.clock.advance(seconds=2)
            second = store.action_redeem(
                action_request_id=action["id"],
                gateway_id="gateway:b",
                expected_version=action["version"] + 1,
                lease_seconds=1,
                idempotency_key=self.key(),
            )
            self.clock.advance(seconds=2)
            current_key = self.key()
            current = store.action_redeem(
                action_request_id=action["id"],
                gateway_id="gateway:a",
                expected_version=second["action_request"]["version"],
                lease_seconds=60,
                idempotency_key=current_key,
            )
            self.assertEqual(
                store.action_redeem(
                    action_request_id=action["id"],
                    gateway_id="gateway:a",
                    expected_version=second["action_request"]["version"],
                    lease_seconds=60,
                    idempotency_key=current_key,
                ),
                current,
            )
            self.assert_domain_error(
                "LEASE_CONFLICT",
                store.action_redeem,
                action_request_id=action["id"],
                gateway_id="gateway:a",
                expected_version=action["version"],
                lease_seconds=1,
                idempotency_key=first_key,
            )

    def test_owner_cancels_pending_and_approved_actions_before_closure(self) -> None:
        store, agent, _ = self.context()
        with store:
            protected = store.mission_create(
                owner_agent_id=agent["id"],
                title="Optional invitations",
                goal="Decide whether to invite additional attendees",
                constraints=[],
                acceptance_criteria=["The invitation decision is recorded"],
                action_policy={"default": "require_approval", "rules": []},
                idempotency_key=self.key(),
            )["mission"]
            pending = store.action_prepare(
                mission_id=protected["id"],
                agent_id=agent["id"],
                action_type="email.send",
                payload={"to": "first@example.com"},
                idempotency_key=self.key(),
            )["action_request"]
            to_approve = store.action_prepare(
                mission_id=protected["id"],
                agent_id=agent["id"],
                action_type="email.send",
                payload={"to": "second@example.com"},
                idempotency_key=self.key(),
            )["action_request"]
            approved = store.action_decide(
                action_request_id=to_approve["id"],
                human_actor="human:davis",
                approve=True,
                decision_reason="Initially approved",
                expected_version=to_approve["version"],
                idempotency_key=self.key(),
            )["action_request"]
            self.assert_domain_error(
                "PRECONDITION_FAILED",
                store.mission_close,
                mission_id=protected["id"],
                owner_agent_id=agent["id"],
                expected_version=protected["version"],
                closure_evidence=[{"summary": "Decision verified"}],
                idempotency_key=self.key(),
            )

            pending_reason = "The additional invitation is no longer needed"
            approved_reason = "The attendee list changed before sending"
            cancelled_pending = store.action_cancel(
                action_request_id=pending["id"],
                owner_agent_id=agent["id"],
                expected_version=pending["version"],
                reason=pending_reason,
                idempotency_key=self.key(),
            )["action_request"]
            cancelled_approved = store.action_cancel(
                action_request_id=approved["id"],
                owner_agent_id=agent["id"],
                expected_version=approved["version"],
                reason=approved_reason,
                idempotency_key=self.key(),
            )["action_request"]
            self.assertEqual(cancelled_pending["status"], "cancelled")
            self.assertEqual(cancelled_pending["decision_reason"], pending_reason)
            self.assertEqual(cancelled_approved["status"], "cancelled")
            self.assertEqual(cancelled_approved["decision_reason"], approved_reason)
            self.assertEqual(store.board_snapshot(protected["id"])["live_actions"], [])
            closed = store.mission_close(
                mission_id=protected["id"],
                owner_agent_id=agent["id"],
                expected_version=protected["version"],
                closure_evidence=[{"summary": "Invitation decision verified"}],
                idempotency_key=self.key(),
            )["mission"]
            self.assertEqual(closed["status"], "closed")

    def test_policy_change_supersedes_unredeemed_action_permits(self) -> None:
        store, agent, mission = self.context()
        with store:
            action = store.action_prepare(
                mission_id=mission["id"],
                agent_id=agent["id"],
                action_type="email.send",
                payload={"to": "guest@example.com", "body": "Hello"},
                idempotency_key=self.key(),
            )["action_request"]
            self.assertEqual(action["status"], "approved")
            change = store.mission_change_propose(
                mission_id=mission["id"],
                proposed_by=agent["id"],
                expected_version=mission["version"],
                patch={
                    "action_policy": {
                        "default": "allow",
                        "rules": [{"action": "email.*", "effect": "deny"}],
                    }
                },
                reason="Pause outbound email",
                idempotency_key=self.key(),
            )["change"]
            store.mission_change_decide(
                change_id=change["id"],
                human_actor="human:davis",
                approve=True,
                decision_reason="Pause approved",
                idempotency_key=self.key(),
            )
            self.assert_domain_error(
                "VERSION_CONFLICT",
                store.action_redeem,
                action_request_id=action["id"],
                gateway_id="gateway:email",
                expected_version=action["version"],
                idempotency_key=self.key(),
            )
            approval_events = [
                event
                for event in store.audit_list(mission["id"])
                if event["type"] == "mission.change_approved"
            ]
            self.assertIn(
                action["id"],
                approval_events[-1]["payload"]["superseded_action_request_ids"],
            )
            snapshot = store.board_snapshot(mission["id"])
            self.assertEqual(snapshot["pending_changes"], [])
            self.assertEqual(snapshot["live_actions"], [])

    def test_in_flight_action_blocks_change_and_closure_without_replay(self) -> None:
        store, agent, mission = self.context()
        with store:
            action = store.action_prepare(
                mission_id=mission["id"],
                agent_id=agent["id"],
                action_type="email.send",
                payload={"to": "guest@example.com", "body": "Hello"},
                idempotency_key=self.key(),
            )["action_request"]
            redeem_key = self.key()
            claimed = store.action_redeem(
                action_request_id=action["id"],
                gateway_id="gateway:email",
                expected_version=action["version"],
                idempotency_key=redeem_key,
            )
            change = store.mission_change_propose(
                mission_id=mission["id"],
                proposed_by=agent["id"],
                expected_version=mission["version"],
                patch={
                    "action_policy": {
                        "default": "allow",
                        "rules": [{"action": "email.*", "effect": "deny"}],
                    }
                },
                reason="Pause outbound email",
                idempotency_key=self.key(),
            )["change"]
            self.assert_domain_error(
                "PRECONDITION_FAILED",
                store.mission_change_decide,
                change_id=change["id"],
                human_actor="human:davis",
                approve=True,
                decision_reason="Pause approved",
                idempotency_key=self.key(),
            )
            self.assert_domain_error(
                "PRECONDITION_FAILED",
                store.mission_close,
                mission_id=mission["id"],
                owner_agent_id=agent["id"],
                expected_version=mission["version"],
                closure_evidence=[{"summary": "Mission checked"}],
                idempotency_key=self.key(),
            )

            failed = store.action_resolve(
                action_request_id=action["id"],
                gateway_id="gateway:email",
                expected_version=claimed["action_request"]["version"],
                success=False,
                outcome_evidence=[{"summary": "Connector rejected the request"}],
                idempotency_key=self.key(),
            )["action_request"]
            self.assertEqual(failed["status"], "failed")
            changed = store.mission_change_decide(
                change_id=change["id"],
                human_actor="human:davis",
                approve=True,
                decision_reason="Pause approved",
                idempotency_key=self.key(),
            )["mission"]
            closed = store.mission_close(
                mission_id=mission["id"],
                owner_agent_id=agent["id"],
                expected_version=changed["version"],
                closure_evidence=[{"summary": "Mission checked"}],
                idempotency_key=self.key(),
            )["mission"]
            self.assertEqual(closed["status"], "closed")
            self.assert_domain_error(
                "MISSION_CLOSED",
                store.action_redeem,
                action_request_id=action["id"],
                gateway_id="gateway:email",
                expected_version=action["version"],
                idempotency_key=redeem_key,
            )

    def test_semantic_memory_retains_versions_provenance_and_expiry(self) -> None:
        store, agent, mission = self.context()
        with store:
            created = store.semantic_memory_put(
                agent_id=agent["id"],
                actor_agent_id=agent["id"],
                mission_id=mission["id"],
                key="venue.preference",
                content="Natural light",
                provenance="User said this in chat",
                expected_version=0,
                idempotency_key=self.key(),
            )["memory"]
            updated = store.semantic_memory_put(
                agent_id=agent["id"],
                actor_agent_id=agent["id"],
                mission_id=mission["id"],
                key="venue.preference",
                content="Natural light and step-free access",
                provenance="User clarified in chat",
                expected_version=created["version"],
                idempotency_key=self.key(),
            )["memory"]
            self.assertEqual((created["version"], updated["version"]), (1, 2))
            self.assertEqual(updated["provenance"], "User clarified in chat")
            self.assert_domain_error(
                "VERSION_CONFLICT",
                store.semantic_memory_put,
                agent_id=agent["id"],
                actor_agent_id=agent["id"],
                mission_id=mission["id"],
                key="venue.preference",
                content="Stale overwrite",
                provenance="Stale source",
                expected_version=created["version"],
                idempotency_key=self.key(),
            )
            versions = store.semantic_memory_history(
                agent_id=agent["id"],
                mission_id=mission["id"],
                key="venue.preference",
            )
            self.assertEqual(
                [(memory["version"], memory["content"]) for memory in versions],
                [
                    (1, "Natural light"),
                    (2, "Natural light and step-free access"),
                ],
            )
            audit_versions = [
                event["payload"]["memory"]
                for event in store.audit_list(mission["id"])
                if event["type"] == "memory.semantic_put"
                and event["payload"]["memory"]["key"] == "venue.preference"
            ]
            self.assertEqual(audit_versions, versions)

            expiry = (self.clock.value + timedelta(hours=1)).isoformat()
            expiring = store.semantic_memory_put(
                agent_id=agent["id"],
                actor_agent_id=agent["id"],
                mission_id=mission["id"],
                key="temporary.hold",
                content="Hold this venue for one hour",
                provenance="Vendor quote",
                expires_at=expiry,
                idempotency_key=self.key(),
            )["memory"]
            self.assertEqual(
                [
                    memory["id"]
                    for memory in store.memory_search(
                        agent_id=agent["id"], query="Hold this venue", mission_id=mission["id"]
                    )["semantic"]
                ],
                [expiring["id"]],
            )
            self.clock.advance(hours=2)
            self.assertEqual(
                store.memory_search(
                    agent_id=agent["id"], query="Hold this venue", mission_id=mission["id"]
                )["semantic"],
                [],
            )
            self.assertEqual(
                len(
                    store.semantic_memory_history(
                        agent_id=agent["id"],
                        mission_id=mission["id"],
                        key="temporary.hold",
                    )
                ),
                1,
            )

    def test_procedural_memory_requires_human_approval(self) -> None:
        store, agent, mission = self.context()
        with store:
            change = store.procedural_memory_change_propose(
                agent_id=agent["id"],
                proposed_by=agent["id"],
                mission_id=mission["id"],
                name="vendor_followup",
                content="Follow up after two business days",
                expected_version=0,
                reason="Observed reliable practice",
                idempotency_key=self.key(),
            )["change"]
            self.assertEqual(
                store.memory_search(agent_id=agent["id"], query="vendor")["procedural"],
                [],
            )
            decision = store.mission_change_decide(
                change_id=change["id"],
                human_actor="human:davis",
                approve=True,
                decision_reason="Useful procedure",
                idempotency_key=self.key(),
            )
            self.assertEqual(decision["change"]["status"], "approved")
            self.assertEqual(decision["procedure"]["version"], 1)
            procedures = store.memory_search(agent_id=agent["id"], query="vendor")["procedural"]
            self.assertEqual(procedures[0]["content"], "Follow up after two business days")
            self.assert_domain_error(
                "VERSION_CONFLICT",
                store.procedural_memory_change_propose,
                agent_id=agent["id"],
                proposed_by=agent["id"],
                mission_id=mission["id"],
                name="vendor_followup",
                content="Stale procedure",
                expected_version=0,
                reason="Stale proposal",
                idempotency_key=self.key(),
            )
            general_change = store.procedural_memory_change_propose(
                agent_id=agent["id"],
                proposed_by=agent["id"],
                name="briefing_format",
                content="Lead with decisions and open questions",
                expected_version=0,
                reason="Improve general briefings",
                idempotency_key=self.key(),
            )["change"]
            store.mission_change_decide(
                change_id=general_change["id"],
                human_actor="human:davis",
                approve=True,
                decision_reason="Useful across missions",
                idempotency_key=self.key(),
            )
            self.assertEqual(
                store.memory_search(agent_id=agent["id"], query="briefing")["procedural"][0][
                    "name"
                ],
                "briefing_format",
            )

    def test_outsider_cannot_inject_scoped_memory_or_block_closure(self) -> None:
        store, agent, mission = self.context()
        with store:
            outsider = store.agent_onboard(
                name="Outside agent",
                role="Unassigned specialist",
                idempotency_key=self.key(),
                agent_id="agent-outsider",
            )["agent"]
            self.assert_domain_error(
                "FORBIDDEN",
                store.semantic_memory_put,
                agent_id=outsider["id"],
                actor_agent_id=outsider["id"],
                mission_id=mission["id"],
                key="foreign.context",
                content="Must not attach to this mission",
                provenance="Out-of-scope attempt",
                idempotency_key=self.key(),
            )
            self.assert_domain_error(
                "FORBIDDEN",
                store.episodic_memory_append,
                agent_id=outsider["id"],
                actor_agent_id=outsider["id"],
                mission_id=mission["id"],
                summary="Must not become a mission episode",
                idempotency_key=self.key(),
            )
            self.assert_domain_error(
                "FORBIDDEN",
                store.procedural_memory_change_propose,
                agent_id=outsider["id"],
                proposed_by=outsider["id"],
                mission_id=mission["id"],
                name="foreign_procedure",
                content="Must not become a pending change",
                expected_version=0,
                reason="Out-of-scope attempt",
                idempotency_key=self.key(),
            )
            self.assert_domain_error(
                "FORBIDDEN",
                store.memory_search,
                agent_id=outsider["id"],
                mission_id=mission["id"],
                query="",
            )
            self.assertEqual(store.board_snapshot(mission["id"])["pending_changes"], [])
            closed = store.mission_close(
                mission_id=mission["id"],
                owner_agent_id=agent["id"],
                expected_version=mission["version"],
                closure_evidence=[{"summary": "No foreign proposal blocked closure"}],
                idempotency_key=self.key(),
            )["mission"]
            self.assertEqual(closed["status"], "closed")

    def test_compaction_retains_every_raw_conversation_message(self) -> None:
        store, agent, mission = self.context()
        with store:
            first = store.conversation_append(
                mission_id=mission["id"],
                role="user",
                actor_id="human:davis",
                content="Find an accessible venue",
                idempotency_key=self.key(),
            )["message"]
            second = store.conversation_append(
                mission_id=mission["id"],
                role="assistant",
                actor_id=agent["id"],
                content="I will compare three options",
                idempotency_key=self.key(),
            )["message"]
            compacted = store.conversation_compact(
                mission_id=mission["id"],
                owner_agent_id=agent["id"],
                through_seq=second["seq"],
                summary="The user requested accessible venue options.",
                idempotency_key=self.key(),
            )["compaction"]
            history = store.conversation_history(mission["id"])
            self.assertEqual(
                [message["id"] for message in history],
                [first["id"], second["id"]],
            )
            self.assertEqual(
                store.mission_handoff(mission["id"])["latest_compaction"]["id"],
                compacted["id"],
            )
            self.assert_domain_error(
                "VALIDATION_ERROR",
                store.conversation_compact,
                mission_id=mission["id"],
                owner_agent_id=agent["id"],
                through_seq=second["seq"],
                summary="This cursor does not advance",
                idempotency_key=self.key(),
            )
            self.assert_domain_error(
                "VALIDATION_ERROR",
                store.conversation_compact,
                mission_id=mission["id"],
                owner_agent_id=agent["id"],
                through_seq=second["seq"] + 100,
                summary="This cursor passes retained history",
                idempotency_key=self.key(),
            )

    def test_general_agent_conversation_is_retained_and_handed_off(self) -> None:
        store, agent, mission = self.context()
        with store:
            message = store.agent_conversation_append(
                agent_id=agent["id"],
                role="user",
                actor_id="human:davis",
                content="Remember this before we discuss a particular mission.",
                idempotency_key=self.key(),
            )["message"]
            history = store.agent_conversation_history(agent["id"])
            self.assertEqual([entry["id"] for entry in history], [message["id"]])
            handoff = store.mission_handoff(mission["id"])
            self.assertEqual(
                [entry["id"] for entry in handoff["agent_conversation_tail"]],
                [message["id"]],
            )

    def test_delegate_handoff_excludes_owner_private_context(self) -> None:
        store, agent, mission = self.context()
        with store:
            delegate = store.agent_onboard(
                name="Jordan",
                role="Travel specialist",
                idempotency_key=self.key(),
                agent_id="agent-delegate",
            )["agent"]
            item = self.create_work(store, agent, mission, "Compare flights")
            store.work_item_assign(
                work_item_id=item["id"],
                owner_agent_id=agent["id"],
                assignee_agent_id=delegate["id"],
                expected_version=item["version"],
                idempotency_key=self.key(),
            )
            store.semantic_memory_put(
                agent_id=agent["id"],
                actor_agent_id=agent["id"],
                mission_id=mission["id"],
                key="owner.private",
                content="Owner-only memory",
                provenance="Owner conversation",
                idempotency_key=self.key(),
            )
            store.semantic_memory_put(
                agent_id=delegate["id"],
                actor_agent_id=delegate["id"],
                mission_id=mission["id"],
                key="delegate.context",
                content="Delegate memory",
                provenance="Delegation briefing",
                idempotency_key=self.key(),
            )
            store.agent_conversation_append(
                agent_id=agent["id"],
                role="user",
                actor_id="human:davis",
                content="Private owner chat",
                idempotency_key=self.key(),
            )
            delegate_message = store.agent_conversation_append(
                agent_id=delegate["id"],
                role="user",
                actor_id="human:davis",
                content="Delegate briefing",
                idempotency_key=self.key(),
            )["message"]
            store.procedural_memory_change_propose(
                agent_id=agent["id"],
                proposed_by=agent["id"],
                mission_id=mission["id"],
                name="owner_private_process",
                content="Owner-only procedure",
                expected_version=0,
                reason="Private workflow improvement",
                idempotency_key=self.key(),
            )

            handoff = store.mission_handoff(mission["id"], agent_id=delegate["id"])
            self.assertEqual(handoff["agent"]["id"], delegate["id"])
            self.assertEqual(handoff["recipient_agent_id"], delegate["id"])
            self.assertEqual(handoff["mission_owner_agent_id"], agent["id"])
            self.assertEqual(
                [memory["key"] for memory in handoff["memory"]["semantic"]],
                ["delegate.context"],
            )
            self.assertEqual(
                [message["id"] for message in handoff["agent_conversation_tail"]],
                [delegate_message["id"]],
            )
            self.assertNotIn("Owner-only memory", str(handoff))
            self.assertNotIn("Owner-only procedure", str(handoff))
            self.assertNotIn("Private owner chat", str(handoff))
            self.assertEqual(handoff["board"]["pending_changes"], [])
            self.assertEqual(handoff["pending_procedure_changes"], [])
            self.assertNotIn("hidden_agent_private_pending_change_count", handoff["board"])

    def test_handoff_uses_newest_post_compaction_and_audit_tails(self) -> None:
        store, agent, mission = self.context()
        with store:
            first = store.conversation_append(
                mission_id=mission["id"],
                role="user",
                actor_id="human:davis",
                content="Start the mission",
                idempotency_key=self.key(),
            )["message"]
            store.conversation_compact(
                mission_id=mission["id"],
                owner_agent_id=agent["id"],
                through_seq=first["seq"],
                summary="The mission started.",
                idempotency_key=self.key(),
            )
            messages = []
            for index in range(60):
                messages.append(
                    store.conversation_append(
                        mission_id=mission["id"],
                        role="assistant",
                        actor_id=agent["id"],
                        content=f"Update {index}",
                        idempotency_key=self.key(),
                    )["message"]
                )
            handoff = store.mission_handoff(mission["id"], conversation_tail=5)
            self.assertEqual(
                [message["id"] for message in handoff["conversation_tail"]],
                [message["id"] for message in messages[-5:]],
            )
            self.assertTrue(handoff["conversation_tail_truncated"])
            self.assertEqual(
                handoff["audit_tail"][-1]["payload"]["message_id"],
                messages[-1]["id"],
            )
            self.assertTrue(handoff["audit_tail_truncated"])

    def test_human_reply_correlates_only_to_same_mission_needs_input_work(self) -> None:
        store, agent, mission = self.context()
        with store:
            item = self.create_work(
                store,
                agent,
                mission,
                "Choose a venue",
                state="needs_input",
                input_request="Which venue do you prefer?",
            )
            reply = store.conversation_append(
                mission_id=mission["id"],
                role="user",
                actor_id="human:davis",
                content="Choose the waterfront venue.",
                metadata={"work_item_id": item["id"]},
                idempotency_key=self.key(),
            )["message"]
            self.assertEqual(reply["metadata"]["work_item_id"], item["id"])

            other_mission = store.mission_create(
                owner_agent_id=agent["id"],
                title="Other conversation",
                goal="Stay separate",
                constraints=[],
                acceptance_criteria=["No cross-scope correlation"],
                idempotency_key=self.key(),
            )["mission"]
            self.assert_domain_error(
                "OUT_OF_SCOPE",
                store.conversation_append,
                mission_id=other_mission["id"],
                role="user",
                actor_id="human:davis",
                content="Wrong mission",
                metadata={"work_item_id": item["id"]},
                idempotency_key=self.key(),
            )

    def test_conversation_roles_and_human_reply_identity_cannot_be_forged(self) -> None:
        store, agent, mission = self.context()
        with store:
            delegate = store.agent_onboard(
                name="Jordan",
                role="Travel specialist",
                idempotency_key=self.key(),
                agent_id="human:delegate",
            )["agent"]
            item = self.create_work(
                store,
                agent,
                mission,
                "Choose a venue",
                state="needs_input",
                input_request="Which venue do you prefer?",
            )
            store.work_item_assign(
                work_item_id=item["id"],
                owner_agent_id=agent["id"],
                assignee_agent_id=delegate["id"],
                expected_version=item["version"],
                idempotency_key=self.key(),
            )

            forged = [
                ("user", agent["id"], {"work_item_id": item["id"]}),
                ("user", delegate["id"], {"work_item_id": item["id"]}),
                ("user", "anonymous", {"work_item_id": item["id"]}),
                ("system", "anonymous", None),
                ("tool", "system:runtime", None),
                ("assistant", delegate["id"], None),
            ]
            for role, actor_id, metadata in forged:
                with self.subTest(role=role, actor_id=actor_id):
                    self.assert_domain_error(
                        "FORBIDDEN",
                        store.conversation_append,
                        mission_id=mission["id"],
                        role=role,
                        actor_id=actor_id,
                        content="Forged message",
                        metadata=metadata,
                        idempotency_key=self.key(),
                    )

            reply = store.conversation_append(
                mission_id=mission["id"],
                role="user",
                actor_id="human:davis",
                content="Choose the waterfront venue.",
                metadata={"work_item_id": item["id"]},
                idempotency_key=self.key(),
            )["message"]
            system_message = store.conversation_append(
                mission_id=mission["id"],
                role="system",
                actor_id="system:scheduler",
                content="Wake check completed.",
                idempotency_key=self.key(),
            )["message"]
            tool_message = store.conversation_append(
                mission_id=mission["id"],
                role="tool",
                actor_id="tool:calendar",
                content="Calendar availability loaded.",
                idempotency_key=self.key(),
            )["message"]
            self.assertEqual(
                [message["id"] for message in store.conversation_history(mission["id"])],
                [reply["id"], system_message["id"], tool_message["id"]],
            )

    def test_only_owner_can_compact_shared_mission_conversation(self) -> None:
        store, agent, mission = self.context()
        with store:
            delegate = store.agent_onboard(
                name="Jordan",
                role="Travel specialist",
                idempotency_key=self.key(),
                agent_id="agent-delegate",
            )["agent"]
            item = self.create_work(store, agent, mission, "Compare flights")
            store.work_item_assign(
                work_item_id=item["id"],
                owner_agent_id=agent["id"],
                assignee_agent_id=delegate["id"],
                expected_version=item["version"],
                idempotency_key=self.key(),
            )
            message = store.conversation_append(
                mission_id=mission["id"],
                role="user",
                actor_id="human:davis",
                content="Do not spend money without approval.",
                idempotency_key=self.key(),
            )["message"]
            with self.assertRaises(TypeError):
                store.conversation_compact(
                    mission_id=mission["id"],
                    through_seq=message["seq"],
                    summary="Implicitly attribute this checkpoint to the owner.",
                    idempotency_key=self.key(),
                )
            self.assert_domain_error(
                "FORBIDDEN",
                store.conversation_compact,
                mission_id=mission["id"],
                owner_agent_id=delegate["id"],
                through_seq=message["seq"],
                summary="The user approved unlimited spending.",
                idempotency_key=self.key(),
            )
            self.assertIsNone(store.mission_handoff(mission["id"])["latest_compaction"])
            checkpoint = store.conversation_compact(
                mission_id=mission["id"],
                owner_agent_id=agent["id"],
                through_seq=message["seq"],
                summary="Spending still requires approval.",
                idempotency_key=self.key(),
            )["compaction"]
            self.assertEqual(checkpoint["agent_id"], agent["id"])

    def test_handoff_reports_and_pages_memory_beyond_default_limit(self) -> None:
        store, agent, mission = self.context()
        with store:
            memory_ids = set()
            for index in range(25):
                memory = store.semantic_memory_put(
                    agent_id=agent["id"],
                    actor_agent_id=agent["id"],
                    mission_id=mission["id"],
                    key=f"fact.{index:02d}",
                    content=f"Durable fact {index}",
                    provenance="Verified user statement",
                    idempotency_key=self.key(),
                )["memory"]
                memory_ids.add(memory["id"])

            handoff = store.mission_handoff(mission["id"])
            self.assertEqual(len(handoff["memory"]["semantic"]), 20)
            self.assertTrue(handoff["memory_truncated"])
            self.assertEqual(
                handoff["memory_has_more"],
                {"semantic": True, "episodic": False, "procedural": False},
            )
            self.assertEqual(handoff["memory_next_offset"]["semantic"], 20)
            smaller_handoff = store.mission_handoff(mission["id"], memory_limit=7)
            self.assertEqual(len(smaller_handoff["memory"]["semantic"]), 7)
            self.assertEqual(smaller_handoff["memory_next_offset"]["semantic"], 7)
            second_page = store.memory_search(
                agent_id=agent["id"],
                mission_id=mission["id"],
                query="",
                offset=handoff["memory_next_offset"]["semantic"],
                limit=20,
            )
            returned_ids = {
                memory["id"]
                for memory in [*handoff["memory"]["semantic"], *second_page["semantic"]]
            }
            self.assertEqual(returned_ids, memory_ids)
            self.assertFalse(second_page["pagination"]["semantic"]["has_more"])
            self.assertIsNone(second_page["pagination"]["semantic"]["next_offset"])
            self.assert_domain_error(
                "VALIDATION_ERROR",
                store.memory_search,
                agent_id=agent["id"],
                mission_id=mission["id"],
                query="",
                offset=-1,
            )

    def test_global_pending_procedure_is_recoverable_after_restart(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "mission.db")
            with MissionStore(path, clock=MutableClock()) as first:
                agent = first.agent_onboard(
                    name="Durable agent",
                    role="Executive assistant",
                    idempotency_key="agent",
                    agent_id="agent-owner",
                )["agent"]
                mission = first.mission_create(
                    owner_agent_id=agent["id"],
                    title="Durable mission",
                    goal="Resume with governance intact",
                    constraints=[],
                    acceptance_criteria=["Pending procedures are visible"],
                    idempotency_key="mission",
                    mission_id="mission-durable",
                )["mission"]
                change = first.procedural_memory_change_propose(
                    agent_id=agent["id"],
                    proposed_by=agent["id"],
                    name="briefing_format",
                    content="Lead with decisions and open questions",
                    expected_version=0,
                    reason="Improve every briefing",
                    idempotency_key="procedure",
                )["change"]

            with MissionStore(path, clock=MutableClock()) as reopened:
                pending = reopened.procedural_memory_change_list_pending(
                    agent_id=agent["id"],
                    mission_id=mission["id"],
                )
                self.assertEqual([proposal["id"] for proposal in pending], [change["id"]])
                handoff = reopened.mission_handoff(mission["id"])
                self.assertEqual(
                    [proposal["id"] for proposal in handoff["pending_procedure_changes"]],
                    [change["id"]],
                )
                self.assertEqual(handoff["board"]["pending_changes"], [])

    def test_delegate_private_procedure_does_not_block_owner_closure(self) -> None:
        store, agent, mission = self.context()
        with store:
            delegate = store.agent_onboard(
                name="Jordan",
                role="Travel specialist",
                idempotency_key=self.key(),
                agent_id="agent-delegate",
            )["agent"]
            item = self.create_work(store, agent, mission, "Compare flights")
            assigned = store.work_item_assign(
                work_item_id=item["id"],
                owner_agent_id=agent["id"],
                assignee_agent_id=delegate["id"],
                expected_version=item["version"],
                idempotency_key=self.key(),
            )["work_item"]
            store.work_item_transition(
                work_item_id=assigned["id"],
                actor_agent_id=delegate["id"],
                expected_version=assigned["version"],
                new_state="done",
                verification_evidence=[{"summary": "Flight comparison delivered"}],
                idempotency_key=self.key(),
            )
            proposal = store.procedural_memory_change_propose(
                agent_id=delegate["id"],
                proposed_by=delegate["id"],
                mission_id=mission["id"],
                name="flight_comparison",
                content="Compare refundable fares first",
                expected_version=0,
                reason="Improve future comparisons",
                idempotency_key=self.key(),
            )["change"]

            self.assertEqual(store.board_snapshot(mission["id"])["pending_changes"], [])
            self.assertEqual(store.mission_handoff(mission["id"])["pending_procedure_changes"], [])
            delegate_handoff = store.mission_handoff(mission["id"], agent_id=delegate["id"])
            self.assertEqual(
                [change["id"] for change in delegate_handoff["pending_procedure_changes"]],
                [proposal["id"]],
            )
            closed = store.mission_close(
                mission_id=mission["id"],
                owner_agent_id=agent["id"],
                expected_version=mission["version"],
                closure_evidence=[{"summary": "The flight comparison is complete"}],
                idempotency_key=self.key(),
            )["mission"]
            self.assertEqual(closed["status"], "closed")
            decision = store.mission_change_decide(
                change_id=proposal["id"],
                human_actor="human:davis",
                approve=True,
                decision_reason="Useful across future flight work",
                idempotency_key=self.key(),
            )
            self.assertEqual(decision["change"]["status"], "approved")
            self.assertEqual(decision["procedure"]["name"], "flight_comparison")
            self.assertEqual(
                store.procedural_memory_change_list_pending(
                    agent_id=delegate["id"],
                    mission_id=mission["id"],
                ),
                [],
            )


class MCPIntegrationTest(unittest.IsolatedAsyncioTestCase):
    AGENT_TOOLS = {
        "agent_get",
        "mission_list",
        "mission_get",
        "mission_change_propose",
        "mission_close",
        "action_prepare",
        "action_cancel",
        "board_snapshot",
        "work_item_get",
        "work_item_list",
        "work_item_create",
        "work_item_update",
        "work_item_assign",
        "work_item_add_dependency",
        "work_item_remove_dependency",
        "work_item_transition",
        "work_item_archive",
        "semantic_memory_put",
        "semantic_memory_history",
        "episodic_memory_append",
        "procedural_memory_change_propose",
        "procedural_memory_change_list_pending",
        "memory_search",
        "conversation_history",
        "conversation_compact",
        "mission_handoff",
        "agent_conversation_history",
    }
    CONTROL_TOOLS = {
        "agent_onboard",
        "agent_list",
        "account_reference_add",
        "account_reference_list",
        "mission_create",
        "mission_list",
        "mission_get",
        "board_snapshot",
        "mission_change_decide",
        "action_decide",
        "procedural_memory_change_list_pending",
        "conversation_append",
        "agent_conversation_append",
        "audit_list",
        "agent_conversation_history",
    }
    RUNTIME_TOOLS = {
        "ready_work_claim",
        "ready_work_release",
        "wakeup_claim_due",
        "wakeup_resolve",
        "action_redeem",
        "action_resolve",
    }

    async def tool_names(self, server, *, resource_template_count: int = 0) -> set[str]:
        async with Client(server, raise_exceptions=True) as client:
            self.assertEqual(client.protocol_version, "2026-07-28")
            self.assertEqual(
                len((await client.list_resource_templates()).resource_templates),
                resource_template_count,
            )
            return {tool.name for tool in (await client.list_tools()).tools}

    async def test_protocol_surfaces_identity_binding_and_resources(self) -> None:
        store = MissionStore()
        try:
            owner = store.agent_onboard(
                name="Owner",
                role="Executive assistant",
                idempotency_key="mcp-owner",
                agent_id="agent-owner-mcp",
            )["agent"]
            delegate = store.agent_onboard(
                name="Delegate",
                role="Travel specialist",
                idempotency_key="mcp-delegate",
                agent_id="agent-delegate-mcp",
            )["agent"]
            outsider = store.agent_onboard(
                name="Outsider",
                role="Unrelated assistant",
                idempotency_key="mcp-outsider",
                agent_id="agent-outsider-mcp",
            )["agent"]
            mission = store.mission_create(
                owner_agent_id=owner["id"],
                title="MCP mission",
                goal="Prove the transport and privacy boundary",
                constraints=[],
                acceptance_criteria=["A real tool and resource call succeed"],
                action_policy={
                    "default": "allow",
                    "rules": [{"action": "email.*", "effect": "require_approval"}],
                },
                idempotency_key="mcp-mission",
            )["mission"]
            delegate_mission = store.mission_create(
                owner_agent_id=delegate["id"],
                title="Delegate-owned mission",
                goal="Prove owner-filtered discovery",
                constraints=[],
                acceptance_criteria=["Only owned missions appear when filtered"],
                idempotency_key="mcp-delegate-mission",
            )["mission"]
            outsider_mission = store.mission_create(
                owner_agent_id=outsider["id"],
                title="Private outsider mission",
                goal="Remain invisible to the delegate",
                constraints=[],
                acceptance_criteria=["No cross-mission access"],
                idempotency_key="mcp-outsider-mission",
            )["mission"]
            store.work_item_create(
                mission_id=mission["id"],
                actor_agent_id=owner["id"],
                assignee_agent_id=delegate["id"],
                title="Delegated work",
                description="Resume without owner-private context",
                idempotency_key="mcp-delegated-work",
            )
            store.semantic_memory_put(
                agent_id=owner["id"],
                actor_agent_id=owner["id"],
                mission_id=mission["id"],
                key="owner.private",
                content="Do not expose this owner-only fact",
                provenance="Owner chat",
                idempotency_key="mcp-owner-memory",
            )
            for index in range(2):
                store.semantic_memory_put(
                    agent_id=delegate["id"],
                    actor_agent_id=delegate["id"],
                    mission_id=mission["id"],
                    key=f"delegate.fact.{index}",
                    content=f"Delegate fact {index}",
                    provenance="Delegation briefing",
                    idempotency_key=f"mcp-delegate-memory-{index}",
                )
            store.agent_conversation_append(
                agent_id=owner["id"],
                role="user",
                actor_id="human:davis",
                content="Do not expose this owner-only chat",
                idempotency_key="mcp-owner-chat",
            )
            store.procedural_memory_change_propose(
                agent_id=owner["id"],
                proposed_by=owner["id"],
                mission_id=mission["id"],
                name="owner_private_process",
                content="Do not expose this owner-only procedure",
                expected_version=0,
                reason="Owner-only improvement",
                idempotency_key="mcp-owner-procedure",
            )
            pending_action = store.action_prepare(
                mission_id=mission["id"],
                agent_id=owner["id"],
                action_type="email.send",
                payload={"to": "review@example.com", "subject": "Exact approval payload"},
                idempotency_key="mcp-pending-action",
            )["action_request"]

            with self.assertRaises(ValueError):
                build_server(store)
            with self.assertRaises(ValueError):
                build_server(store, "agent", "missing-agent")

            self.assertEqual(
                await self.tool_names(build_server(store, "control")),
                self.CONTROL_TOOLS,
            )
            self.assertEqual(
                await self.tool_names(build_server(store, "runtime")),
                self.RUNTIME_TOOLS,
            )
            self.assertEqual(
                await self.tool_names(build_server(store, "all"), resource_template_count=2),
                self.AGENT_TOOLS | self.CONTROL_TOOLS | self.RUNTIME_TOOLS,
            )

            async with Client(build_server(store, "control"), raise_exceptions=True) as control:
                recovered_missions = await control.call_tool("mission_list", {})
                self.assertFalse(recovered_missions.is_error)
                self.assertIn(
                    mission["id"],
                    {value["id"] for value in recovered_missions.structured_content["result"]},
                )
                recovered_mission = await control.call_tool(
                    "mission_get", {"mission_id": mission["id"]}
                )
                self.assertFalse(recovered_mission.is_error)
                self.assertEqual(recovered_mission.structured_content["id"], mission["id"])
                recovered_board = await control.call_tool(
                    "board_snapshot", {"mission_id": mission["id"]}
                )
                self.assertFalse(recovered_board.is_error)
                recovered_action = recovered_board.structured_content["live_actions"][0]
                self.assertEqual(recovered_action["id"], pending_action["id"])
                self.assertEqual(recovered_action["version"], pending_action["version"])
                self.assertEqual(
                    recovered_action["payload"],
                    {"to": "review@example.com", "subject": "Exact approval payload"},
                )

                owner_pending = await control.call_tool(
                    "procedural_memory_change_list_pending",
                    {"agent_id": owner["id"], "mission_id": mission["id"]},
                )
                self.assertFalse(owner_pending.is_error)
                self.assertEqual(len(owner_pending.structured_content["result"]), 1)
                delegated_message = await control.call_tool(
                    "conversation_append",
                    {
                        "mission_id": mission["id"],
                        "agent_id": delegate["id"],
                        "role": "assistant",
                        "actor_id": delegate["id"],
                        "content": "A scoped delegate update",
                        "idempotency_key": "mcp-delegate-message",
                    },
                )
                self.assertFalse(delegated_message.is_error)

            async with Client(
                build_server(store, "agent", owner["id"]), raise_exceptions=True
            ) as owner_client:
                compacted = await owner_client.call_tool(
                    "conversation_compact",
                    {
                        "mission_id": mission["id"],
                        "owner_agent_id": owner["id"],
                        "through_seq": delegated_message.structured_content["message"]["seq"],
                        "summary": "The delegate began work.",
                        "idempotency_key": "mcp-owner-compaction",
                    },
                )
                self.assertFalse(compacted.is_error)

            async with Client(
                build_server(store, "agent", delegate["id"]), raise_exceptions=True
            ) as client:
                self.assertEqual(client.protocol_version, "2026-07-28")
                self.assertEqual(
                    {tool.name for tool in (await client.list_tools()).tools},
                    self.AGENT_TOOLS,
                )
                visible_missions = await client.call_tool("mission_list", {})
                self.assertFalse(visible_missions.is_error)
                self.assertEqual(
                    {value["id"] for value in visible_missions.structured_content["result"]},
                    {mission["id"], delegate_mission["id"]},
                )
                owned_missions = await client.call_tool(
                    "mission_list", {"owner_agent_id": delegate["id"]}
                )
                self.assertFalse(owned_missions.is_error)
                self.assertEqual(
                    [value["id"] for value in owned_missions.structured_content["result"]],
                    [delegate_mission["id"]],
                )
                templates = await client.list_resource_templates()
                self.assertEqual(
                    {str(template.uri_template) for template in templates.resource_templates},
                    {
                        "mission://{mission_id}/board",
                        "mission://{mission_id}/handoff",
                    },
                )

                mission_result = await client.call_tool(
                    "mission_get", {"mission_id": mission["id"]}
                )
                self.assertFalse(mission_result.is_error)
                self.assertEqual(mission_result.structured_content["id"], mission["id"])

                handoff_result = await client.call_tool(
                    "mission_handoff",
                    {"mission_id": mission["id"], "memory_limit": 1},
                )
                self.assertFalse(handoff_result.is_error)
                self.assertEqual(
                    handoff_result.structured_content["recipient_agent_id"], delegate["id"]
                )
                self.assertTrue(handoff_result.structured_content["memory_truncated"])
                self.assertNotIn("Do not expose this owner-only fact", str(handoff_result))
                self.assertNotIn("Do not expose this owner-only chat", str(handoff_result))
                self.assertNotIn("Do not expose this owner-only procedure", str(handoff_result))

                memory_page = await client.call_tool(
                    "memory_search",
                    {
                        "agent_id": delegate["id"],
                        "mission_id": mission["id"],
                        "query": "Delegate fact",
                        "limit": 1,
                        "offset": 0,
                    },
                )
                self.assertFalse(memory_page.is_error)
                self.assertEqual(len(memory_page.structured_content["semantic"]), 1)
                self.assertEqual(
                    memory_page.structured_content["pagination"]["semantic"]["next_offset"], 1
                )

                pending = await client.call_tool(
                    "procedural_memory_change_list_pending",
                    {"agent_id": delegate["id"], "mission_id": mission["id"]},
                )
                self.assertFalse(pending.is_error)
                self.assertEqual(pending.structured_content["result"], [])

                board_result = await client.call_tool(
                    "board_snapshot", {"mission_id": mission["id"]}
                )
                self.assertFalse(board_result.is_error)
                self.assertEqual(board_result.structured_content["pending_changes"], [])
                self.assertNotIn("Do not expose this owner-only procedure", str(board_result))

                resource = await client.read_resource(f"mission://{mission['id']}/handoff")
                resource_payload = json.loads(resource.contents[0].text)
                self.assertEqual(resource_payload["recipient_agent_id"], delegate["id"])
                self.assertNotIn("Do not expose this owner-only fact", str(resource_payload))
                self.assertNotIn("Do not expose this owner-only chat", str(resource_payload))
                self.assertNotIn("Do not expose this owner-only procedure", str(resource_payload))

                board_resource = await client.read_resource(f"mission://{mission['id']}/board")
                board_payload = json.loads(board_resource.contents[0].text)
                self.assertEqual(board_payload["mission"]["id"], mission["id"])
                self.assertEqual(board_payload["pending_changes"], [])
                self.assertNotIn("Do not expose this owner-only procedure", str(board_payload))

                impersonation = await client.call_tool("agent_get", {"agent_id": owner["id"]})
                self.assertTrue(impersonation.is_error)
                self.assertIn("FORBIDDEN", impersonation.content[0].text)

                private_pending = await client.call_tool(
                    "procedural_memory_change_list_pending",
                    {"agent_id": owner["id"], "mission_id": mission["id"]},
                )
                self.assertTrue(private_pending.is_error)
                self.assertIn("FORBIDDEN", private_pending.content[0].text)

                outsider_access = await client.call_tool(
                    "mission_get", {"mission_id": outsider_mission["id"]}
                )
                self.assertTrue(outsider_access.is_error)
                self.assertIn("FORBIDDEN", outsider_access.content[0].text)

                with self.assertRaises(Exception) as outsider_resource:
                    await client.read_resource(f"mission://{outsider_mission['id']}/board")
                self.assertIn("FORBIDDEN", str(outsider_resource.exception))

                missing = await client.call_tool("mission_get", {"mission_id": "missing"})
                self.assertTrue(missing.is_error)
                self.assertIn("NOT_FOUND", missing.content[0].text)

                with self.assertRaises(Exception) as resource_error:
                    await client.read_resource("mission://missing/board")
                self.assertIn("NOT_FOUND", str(resource_error.exception))
        finally:
            store.close()

    async def test_runtime_action_claim_resolution_and_replay_fence(self) -> None:
        store = MissionStore()
        try:
            agent = store.agent_onboard(
                name="Action agent",
                role="Executive assistant",
                idempotency_key="runtime-agent",
            )["agent"]
            mission = store.mission_create(
                owner_agent_id=agent["id"],
                title="Send update",
                goal="Send exactly one approved update",
                constraints=[],
                acceptance_criteria=["Connector outcome is retained"],
                idempotency_key="runtime-mission",
            )["mission"]
            action = store.action_prepare(
                mission_id=mission["id"],
                agent_id=agent["id"],
                action_type="email.send",
                payload={"to": "recipient-id", "subject": "Update"},
                idempotency_key="runtime-prepare",
            )["action_request"]
            claim_arguments = {
                "action_request_id": action["id"],
                "gateway_id": "email-gateway",
                "expected_version": action["version"],
                "idempotency_key": "runtime-claim",
                "lease_seconds": 60,
            }

            async with Client(build_server(store, "runtime"), raise_exceptions=True) as client:
                claimed = await client.call_tool("action_redeem", claim_arguments)
                self.assertFalse(claimed.is_error)
                self.assertEqual(claimed.structured_content["permit"]["id"], action["id"])
                self.assertEqual(
                    claimed.structured_content["permit"]["execution_key"], action["id"]
                )
                self.assertEqual(
                    claimed.structured_content["permit"]["payload"],
                    {"to": "recipient-id", "subject": "Update"},
                )
                self.assertTrue(claimed.structured_content["permit"]["lease_expires_at"])
                self.assertEqual(
                    claimed.structured_content["action_request"]["status"], "executing"
                )

                resolved = await client.call_tool(
                    "action_resolve",
                    {
                        "action_request_id": action["id"],
                        "gateway_id": "email-gateway",
                        "expected_version": claimed.structured_content["action_request"]["version"],
                        "success": True,
                        "outcome_evidence": [{"provider_message_id": "message-123"}],
                        "idempotency_key": "runtime-resolve",
                    },
                )
                self.assertFalse(resolved.is_error)
                self.assertEqual(
                    resolved.structured_content["action_request"]["status"], "completed"
                )
                self.assertEqual(
                    resolved.structured_content["action_request"]["outcome_evidence"],
                    [{"provider_message_id": "message-123"}],
                )

                replay = await client.call_tool("action_redeem", claim_arguments)
                self.assertTrue(replay.is_error)
                self.assertIn("PRECONDITION_FAILED", replay.content[0].text)
                self.assertNotIn("recipient-id", str(replay))
        finally:
            store.close()

    async def test_installed_stdio_entrypoint(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            parameters = StdioServerParameters(
                command=sys.executable,
                args=[
                    "-m",
                    "mission_mcp",
                    "--surface",
                    "control",
                    "--db",
                    str(Path(directory) / "stdio.db"),
                ],
            )
            async with Client(parameters, raise_exceptions=True) as client:
                self.assertEqual(client.protocol_version, "2026-07-28")
                self.assertEqual(
                    {tool.name for tool in (await client.list_tools()).tools},
                    self.CONTROL_TOOLS,
                )
                result = await client.call_tool(
                    "agent_onboard",
                    {
                        "name": "Stdio agent",
                        "role": "Executive assistant",
                        "idempotency_key": "stdio-agent",
                    },
                )
                self.assertFalse(result.is_error)
                self.assertEqual(result.structured_content["agent"]["name"], "Stdio agent")


if __name__ == "__main__":
    unittest.main()
