from __future__ import annotations

import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path

from mission_mcp import DomainError, MissionStore


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


if __name__ == "__main__":
    unittest.main()
