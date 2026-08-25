from __future__ import annotations

import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path

from mission_mcp import DomainError, MissionStore


class FixedClock:
    def __call__(self) -> datetime:
        return datetime(2026, 8, 24, 12, 0, tzinfo=UTC)


class MissionStoreTest(unittest.TestCase):
    def test_agent_and_mission_survive_store_restart(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "mission.db")
            with MissionStore(path, clock=FixedClock()) as first:
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

            with MissionStore(path, clock=FixedClock()) as reopened:
                self.assertEqual(reopened.agent_get(agent["id"])["id"], agent["id"])
                loaded = reopened.mission_get(mission["id"])
                self.assertEqual(loaded["owner_agent_id"], agent["id"])
                self.assertEqual(loaded["goal"], "Outlive this process")

    def test_account_references_accept_identifiers_but_reject_secrets(self) -> None:
        with MissionStore(clock=FixedClock()) as store:
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

            with self.assertRaises(DomainError) as caught:
                store.account_reference_add(
                    agent_id=agent["id"],
                    service="gmail",
                    account_ref="unsafe-reference",
                    metadata={"oauth": {"api_token": "must-not-be-stored"}},
                    idempotency_key="unsafe-account",
                )
            self.assertEqual(caught.exception.code, "SECRET_REJECTED")
            self.assertEqual(len(store.account_reference_list(agent["id"])), 1)

    def test_mission_inherits_a_validated_onboarding_action_policy(self) -> None:
        policy = {
            "default": "allow",
            "rules": [{"action": "email.*", "effect": "require_approval"}],
        }
        with MissionStore(clock=FixedClock()) as store:
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


if __name__ == "__main__":
    unittest.main()
