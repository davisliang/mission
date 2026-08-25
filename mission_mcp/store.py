"""Thread-safe durable state for agents and their missions.

This first slice deliberately contains no MCP transport. It establishes the
storage rules that later tools depend on: atomic writes, request-bound
idempotency, immutable audit events, validated action policy, and durable
identity that is independent of any model or provider.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable, Iterable
from contextlib import contextmanager
from datetime import UTC, datetime
from hashlib import sha256
from threading import RLock
from typing import Any
from uuid import uuid4

POLICY_EFFECTS = {"allow", "require_approval", "deny"}
JSON_COLUMNS = {
    "default_policy_json",
    "scopes_json",
    "metadata_json",
    "constraints_json",
    "acceptance_criteria_json",
    "action_policy_json",
    "payload_json",
}
SECRET_KEY_SUFFIXES = {
    "apikey",
    "authorization",
    "authorizationheader",
    "cookie",
    "credential",
    "credentials",
    "password",
    "privatekey",
    "secret",
    "sessioncookie",
    "token",
}


class DomainError(RuntimeError):
    """A domain failure with a stable code and structured details."""

    def __init__(self, code: str, message: str, **details: Any) -> None:
        super().__init__(message)
        self.code = code
        self.details = details

    def as_dict(self) -> dict[str, Any]:
        return {"code": self.code, "message": str(self), "details": self.details}


class MissionStore:
    """A thread-safe SQLite store for one trusted personal-agent workspace."""

    def __init__(
        self,
        path: str = ":memory:",
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._clock = clock or (lambda: datetime.now(UTC))
        self._lock = RLock()
        self._closed = False
        self.db = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA foreign_keys = ON")
        self.db.execute("PRAGMA busy_timeout = 5000")
        if path != ":memory:":
            self.db.execute("PRAGMA journal_mode = WAL")
        self._create_schema()

    def __enter__(self) -> MissionStore:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def close(self) -> None:
        """Close the SQLite connection; repeated closes are harmless."""
        with self._lock:
            if not self._closed:
                self.db.close()
                self._closed = True

    def _now(self) -> str:
        value = self._clock()
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")

    @staticmethod
    def _id() -> str:
        return str(uuid4())

    @staticmethod
    def _json(value: Any) -> str:
        return json.dumps(value, separators=(",", ":"), sort_keys=True)

    @staticmethod
    def _require(condition: Any, code: str, message: str, **details: Any) -> None:
        if not condition:
            raise DomainError(code, message, **details)

    def _decode(self, row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        result = dict(row)
        for column in tuple(result):
            if column in JSON_COLUMNS:
                encoded = result.pop(column)
                result[column.removesuffix("_json")] = json.loads(encoded)
        return result

    def _one(self, sql: str, arguments: Iterable[Any] = ()) -> dict[str, Any] | None:
        return self._decode(self.db.execute(sql, tuple(arguments)).fetchone())

    def _all(self, sql: str, arguments: Iterable[Any] = ()) -> list[dict[str, Any]]:
        return [self._decode(row) or {} for row in self.db.execute(sql, tuple(arguments))]

    def _must(self, table: str, record_id: str, label: str) -> dict[str, Any]:
        record = self._one(f"SELECT * FROM {table} WHERE id=?", (record_id,))
        self._require(record, "NOT_FOUND", f"{label} not found", id=record_id)
        return record

    @contextmanager
    def _transaction(self):
        with self._lock:
            self.db.execute("BEGIN IMMEDIATE")
            try:
                yield
            except Exception:
                self.db.execute("ROLLBACK")
                raise
            else:
                self.db.execute("COMMIT")

    def _mutate(
        self,
        tool: str,
        key: str,
        request: dict[str, Any],
        operation: Callable[[], dict[str, Any]],
    ) -> dict[str, Any]:
        """Atomically bind an idempotency key to one tool and exact request."""
        self._require(
            isinstance(key, str) and key.strip(),
            "VALIDATION_ERROR",
            "idempotency_key is required",
        )
        request_hash = sha256(self._json({"tool": tool, "arguments": request}).encode()).hexdigest()
        with self._transaction():
            prior = self.db.execute(
                "SELECT tool,request_hash,response_json FROM idempotency WHERE key=?", (key,)
            ).fetchone()
            if prior:
                self._require(
                    prior["tool"] == tool and prior["request_hash"] == request_hash,
                    "IDEMPOTENCY_CONFLICT",
                    "idempotency_key was already used for a different request",
                )
                return json.loads(prior["response_json"])
            try:
                response = operation()
            except sqlite3.IntegrityError as error:
                raise DomainError(
                    "CONFLICT", "Write violates a uniqueness or reference constraint"
                ) from error
            self.db.execute(
                "INSERT INTO idempotency(key,tool,request_hash,response_json,created_at) "
                "VALUES(?,?,?,?,?)",
                (key, tool, request_hash, self._json(response), self._now()),
            )
            return response

    def _event(
        self,
        event_type: str,
        *,
        mission_id: str | None,
        actor_id: str,
        payload: dict[str, Any],
    ) -> str:
        event_id = self._id()
        self.db.execute(
            "INSERT INTO events(id,mission_id,actor_id,type,payload_json,created_at) "
            "VALUES(?,?,?,?,?,?)",
            (event_id, mission_id, actor_id, event_type, self._json(payload), self._now()),
        )
        return event_id

    @staticmethod
    def _string_list(value: Any, label: str, *, allow_empty: bool) -> list[str]:
        if not isinstance(value, list) or (not allow_empty and not value):
            raise DomainError("VALIDATION_ERROR", f"{label} must be a non-empty list")
        cleaned = []
        for position, item in enumerate(value):
            if not isinstance(item, str) or not item.strip():
                raise DomainError(
                    "VALIDATION_ERROR",
                    f"{label} entries must be non-empty strings",
                    index=position,
                )
            cleaned.append(item.strip())
        return cleaned

    def _validate_policy(self, policy: Any) -> dict[str, Any]:
        self._require(
            isinstance(policy, dict) and set(policy) == {"default", "rules"},
            "VALIDATION_ERROR",
            "policy needs exactly default and rules",
        )
        self._require(
            policy["default"] in POLICY_EFFECTS,
            "VALIDATION_ERROR",
            "policy.default must be allow, require_approval, or deny",
        )
        self._require(
            isinstance(policy["rules"], list),
            "VALIDATION_ERROR",
            "policy.rules must be a list",
        )
        rules = []
        for position, rule in enumerate(policy["rules"]):
            self._require(
                isinstance(rule, dict) and set(rule) == {"action", "effect"},
                "VALIDATION_ERROR",
                "Each policy rule needs exactly action and effect",
                rule_index=position,
            )
            self._require(
                isinstance(rule["action"], str) and rule["action"].strip(),
                "VALIDATION_ERROR",
                "Policy rule action must be a non-empty exact name or glob",
                rule_index=position,
            )
            self._require(
                rule["effect"] in POLICY_EFFECTS,
                "VALIDATION_ERROR",
                "Policy rule effect must be allow, require_approval, or deny",
                rule_index=position,
            )
            rules.append({"action": rule["action"].strip(), "effect": rule["effect"]})
        return {"default": policy["default"], "rules": rules}

    @classmethod
    def _secret_metadata_path(cls, value: Any, path: str = "metadata") -> str | None:
        if isinstance(value, dict):
            for key, child in value.items():
                compact = "".join(
                    character.lower() for character in str(key) if character.isalnum()
                )
                child_path = f"{path}.{key}"
                if any(compact.endswith(secret) for secret in SECRET_KEY_SUFFIXES):
                    return child_path
                found = cls._secret_metadata_path(child, child_path)
                if found:
                    return found
        elif isinstance(value, list):
            for index, child in enumerate(value):
                found = cls._secret_metadata_path(child, f"{path}[{index}]")
                if found:
                    return found
        return None

    def _create_schema(self) -> None:
        with self._lock:
            self.db.executescript(
                """
                CREATE TABLE IF NOT EXISTS agents (
                  id TEXT PRIMARY KEY,
                  name TEXT NOT NULL,
                  role TEXT NOT NULL,
                  manager TEXT,
                  biography TEXT NOT NULL,
                  default_policy_json TEXT NOT NULL,
                  status TEXT NOT NULL DEFAULT 'active',
                  version INTEGER NOT NULL DEFAULT 1,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS account_references (
                  id TEXT PRIMARY KEY,
                  agent_id TEXT NOT NULL REFERENCES agents(id),
                  service TEXT NOT NULL,
                  account_ref TEXT NOT NULL,
                  scopes_json TEXT NOT NULL,
                  metadata_json TEXT NOT NULL,
                  status TEXT NOT NULL DEFAULT 'active',
                  version INTEGER NOT NULL DEFAULT 1,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  UNIQUE(agent_id, service, account_ref)
                );
                CREATE TABLE IF NOT EXISTS missions (
                  id TEXT PRIMARY KEY,
                  owner_agent_id TEXT NOT NULL REFERENCES agents(id),
                  title TEXT NOT NULL,
                  goal TEXT NOT NULL,
                  constraints_json TEXT NOT NULL,
                  acceptance_criteria_json TEXT NOT NULL,
                  action_policy_json TEXT NOT NULL,
                  status TEXT NOT NULL DEFAULT 'open',
                  version INTEGER NOT NULL DEFAULT 1,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS events (
                  seq INTEGER PRIMARY KEY AUTOINCREMENT,
                  id TEXT UNIQUE NOT NULL,
                  mission_id TEXT REFERENCES missions(id),
                  actor_id TEXT NOT NULL,
                  type TEXT NOT NULL,
                  payload_json TEXT NOT NULL,
                  created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS idempotency (
                  key TEXT PRIMARY KEY,
                  tool TEXT NOT NULL,
                  request_hash TEXT NOT NULL,
                  response_json TEXT NOT NULL,
                  created_at TEXT NOT NULL
                );
                CREATE TRIGGER IF NOT EXISTS events_no_update
                BEFORE UPDATE ON events BEGIN
                  SELECT RAISE(ABORT, 'events are append-only');
                END;
                CREATE TRIGGER IF NOT EXISTS events_no_delete
                BEFORE DELETE ON events BEGIN
                  SELECT RAISE(ABORT, 'events are append-only');
                END;
                """
            )

    def agent_onboard(
        self,
        *,
        name: str,
        role: str,
        idempotency_key: str,
        manager: str | None = None,
        biography: str = "",
        default_policy: dict[str, Any] | None = None,
        agent_id: str | None = None,
    ) -> dict[str, Any]:
        request = {
            "name": name,
            "role": role,
            "manager": manager,
            "biography": biography,
            "default_policy": default_policy,
            "agent_id": agent_id,
        }

        def operation() -> dict[str, Any]:
            self._require(
                isinstance(name, str) and name.strip() and isinstance(role, str) and role.strip(),
                "VALIDATION_ERROR",
                "name and role are required",
            )
            policy = self._validate_policy(
                {"default": "allow", "rules": []} if default_policy is None else default_policy
            )
            record_id, now = agent_id or self._id(), self._now()
            self.db.execute(
                "INSERT INTO agents(id,name,role,manager,biography,default_policy_json,"
                "created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)",
                (
                    record_id,
                    name.strip(),
                    role.strip(),
                    manager,
                    biography,
                    self._json(policy),
                    now,
                    now,
                ),
            )
            agent = self.agent_get(record_id)
            event_id = self._event(
                "agent.onboarded",
                mission_id=None,
                actor_id=record_id,
                payload={"agent": agent},
            )
            return {"agent": agent, "event_id": event_id}

        return self._mutate("agent_onboard", idempotency_key, request, operation)

    def agent_get(self, agent_id: str) -> dict[str, Any]:
        with self._lock:
            return self._must("agents", agent_id, "Agent")

    def agent_list(self) -> list[dict[str, Any]]:
        with self._lock:
            return self._all("SELECT * FROM agents ORDER BY created_at,id")

    def account_reference_add(
        self,
        *,
        agent_id: str,
        service: str,
        account_ref: str,
        idempotency_key: str,
        scopes: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        request = {
            "agent_id": agent_id,
            "service": service,
            "account_ref": account_ref,
            "scopes": scopes,
            "metadata": metadata,
        }

        def operation() -> dict[str, Any]:
            self._must("agents", agent_id, "Agent")
            self._require(
                isinstance(service, str)
                and service.strip()
                and isinstance(account_ref, str)
                and account_ref.strip(),
                "VALIDATION_ERROR",
                "service and account_ref are required",
            )
            scope_values = (
                [] if scopes is None else self._string_list(scopes, "scopes", allow_empty=True)
            )
            metadata_value = {} if metadata is None else metadata
            self._require(
                isinstance(metadata_value, dict),
                "VALIDATION_ERROR",
                "metadata must be an object",
            )
            secret_path = self._secret_metadata_path(metadata_value)
            self._require(
                not secret_path,
                "SECRET_REJECTED",
                "Account metadata may contain references, never credentials",
                field=secret_path,
            )
            record_id, now = self._id(), self._now()
            self.db.execute(
                "INSERT INTO account_references(id,agent_id,service,account_ref,scopes_json,"
                "metadata_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)",
                (
                    record_id,
                    agent_id,
                    service.strip(),
                    account_ref.strip(),
                    self._json(scope_values),
                    self._json(metadata_value),
                    now,
                    now,
                ),
            )
            account = self._must("account_references", record_id, "Account reference")
            event_id = self._event(
                "agent.account_added",
                mission_id=None,
                actor_id=agent_id,
                payload={"account_reference": account},
            )
            return {"account_reference": account, "event_id": event_id}

        return self._mutate("account_reference_add", idempotency_key, request, operation)

    def account_reference_list(self, agent_id: str) -> list[dict[str, Any]]:
        with self._lock:
            self._must("agents", agent_id, "Agent")
            return self._all(
                "SELECT * FROM account_references WHERE agent_id=? ORDER BY service,account_ref,id",
                (agent_id,),
            )

    def mission_create(
        self,
        *,
        owner_agent_id: str,
        title: str,
        goal: str,
        constraints: list[str],
        acceptance_criteria: list[str],
        idempotency_key: str,
        action_policy: dict[str, Any] | None = None,
        mission_id: str | None = None,
    ) -> dict[str, Any]:
        request = {
            "owner_agent_id": owner_agent_id,
            "title": title,
            "goal": goal,
            "constraints": constraints,
            "acceptance_criteria": acceptance_criteria,
            "action_policy": action_policy,
            "mission_id": mission_id,
        }

        def operation() -> dict[str, Any]:
            owner = self._must("agents", owner_agent_id, "Owner agent")
            self._require(
                isinstance(title, str) and title.strip() and isinstance(goal, str) and goal.strip(),
                "VALIDATION_ERROR",
                "title and goal are required",
            )
            constraint_values = self._string_list(constraints, "constraints", allow_empty=True)
            criterion_values = self._string_list(
                acceptance_criteria, "acceptance_criteria", allow_empty=False
            )
            policy = self._validate_policy(
                owner["default_policy"] if action_policy is None else action_policy
            )
            record_id, now = mission_id or self._id(), self._now()
            self.db.execute(
                "INSERT INTO missions(id,owner_agent_id,title,goal,constraints_json,"
                "acceptance_criteria_json,action_policy_json,created_at,updated_at) "
                "VALUES(?,?,?,?,?,?,?,?,?)",
                (
                    record_id,
                    owner_agent_id,
                    title.strip(),
                    goal.strip(),
                    self._json(constraint_values),
                    self._json(criterion_values),
                    self._json(policy),
                    now,
                    now,
                ),
            )
            mission = self.mission_get(record_id)
            event_id = self._event(
                "mission.created",
                mission_id=record_id,
                actor_id=owner_agent_id,
                payload={"mission": mission},
            )
            return {"mission": mission, "event_id": event_id}

        return self._mutate("mission_create", idempotency_key, request, operation)

    def mission_get(self, mission_id: str) -> dict[str, Any]:
        with self._lock:
            return self._must("missions", mission_id, "Mission")

    def mission_list(self, owner_agent_id: str | None = None) -> list[dict[str, Any]]:
        with self._lock:
            if owner_agent_id is None:
                return self._all("SELECT * FROM missions ORDER BY created_at,id")
            self._must("agents", owner_agent_id, "Owner agent")
            return self._all(
                "SELECT * FROM missions WHERE owner_agent_id=? ORDER BY created_at,id",
                (owner_agent_id,),
            )
