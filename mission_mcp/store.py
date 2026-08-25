"""Thread-safe durable state for agents and their missions.

This module deliberately contains no MCP transport. It owns the rules that
must survive model and host changes: atomic writes, request-bound idempotency,
the five work states, single-writer versions, dependency integrity, evidence,
and durable execution and wake leases.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable, Iterable
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from threading import RLock
from typing import Any
from uuid import uuid4

POLICY_EFFECTS = {"allow", "require_approval", "deny"}
WORK_STATES = {"doing", "waiting", "blocked", "needs_input", "done"}
JSON_COLUMNS = {
    "default_policy_json",
    "scopes_json",
    "metadata_json",
    "constraints_json",
    "acceptance_criteria_json",
    "action_policy_json",
    "closure_evidence_json",
    "verification_evidence_json",
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
    "accesskeyid",
    "secretaccesskey",
    "secretkey",
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
    def _parse_time(value: str, field: str) -> datetime:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except (AttributeError, ValueError) as error:
            raise DomainError("VALIDATION_ERROR", f"{field} must be an ISO timestamp") from error
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)

    @classmethod
    def _iso(cls, value: str, field: str) -> str:
        return cls._parse_time(value, field).isoformat().replace("+00:00", "Z")

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
                result[column.removesuffix("_json")] = (
                    json.loads(encoded) if encoded is not None else None
                )
        if "archived" in result:
            result["archived"] = bool(result["archived"])
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

    def _bump_board(self, mission_id: str) -> int:
        self.db.execute(
            "UPDATE missions SET board_revision=board_revision+1,updated_at=? WHERE id=?",
            (self._now(), mission_id),
        )
        return int(
            self.db.execute(
                "SELECT board_revision FROM missions WHERE id=?", (mission_id,)
            ).fetchone()[0]
        )

    def _mission_open(self, mission_id: str) -> dict[str, Any]:
        mission = self._must("missions", mission_id, "Mission")
        self._require(mission["status"] == "open", "MISSION_CLOSED", "Mission is closed")
        return mission

    def _version(self, record: dict[str, Any], expected: int) -> None:
        self._require(
            record["version"] == expected,
            "VERSION_CONFLICT",
            "The record changed; read it again before writing",
            expected=expected,
            actual=record["version"],
        )

    def _writer(self, item: dict[str, Any], actor_agent_id: str) -> None:
        self._require(not item["archived"], "WORK_ARCHIVED", "Archived work is immutable")
        self._require(
            item["assignee_agent_id"] == actor_agent_id,
            "WRITER_CONFLICT",
            "Only the assigned writer may mutate this work item",
            assignee_agent_id=item["assignee_agent_id"],
        )

    def _validate_evidence(
        self,
        evidence: list[dict[str, Any]],
        label: str,
        *,
        code: str = "VALIDATION_ERROR",
    ) -> None:
        self._require(
            isinstance(evidence, list) and evidence, code, f"{label} evidence is required"
        )
        for position, record in enumerate(evidence):
            self._require(
                isinstance(record, dict)
                and any(value not in (None, "", [], {}) for value in record.values()),
                code,
                f"Each {label} evidence record must contain a concrete value",
                evidence_index=position,
            )

    def _lease_until(self, seconds: int) -> str:
        now = self._parse_time(self._now(), "clock")
        return (now + timedelta(seconds=max(1, seconds))).isoformat().replace("+00:00", "Z")

    @staticmethod
    def _string_list(value: Any, label: str, *, allow_empty: bool) -> list[str]:
        if not isinstance(value, list) or (not allow_empty and not value):
            requirement = "a list" if allow_empty else "a non-empty list"
            raise DomainError("VALIDATION_ERROR", f"{label} must be {requirement}")
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
            isinstance(policy["default"], str) and policy["default"] in POLICY_EFFECTS,
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
                isinstance(rule["effect"], str) and rule["effect"] in POLICY_EFFECTS,
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

    def _ensure_column(self, table: str, column: str, declaration: str) -> None:
        columns = {row["name"] for row in self.db.execute(f"PRAGMA table_info({table})")}
        if column not in columns:
            self.db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {declaration}")

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
                  board_revision INTEGER NOT NULL DEFAULT 0,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  closed_at TEXT,
                  closure_evidence_json TEXT
                );
                CREATE TABLE IF NOT EXISTS work_items (
                  id TEXT PRIMARY KEY,
                  mission_id TEXT NOT NULL REFERENCES missions(id),
                  title TEXT NOT NULL,
                  description TEXT NOT NULL DEFAULT '',
                  state TEXT NOT NULL CHECK(state IN ('doing','waiting','blocked','needs_input','done')),
                  assignee_agent_id TEXT NOT NULL REFERENCES agents(id),
                  wait_condition TEXT,
                  next_check_at TEXT,
                  input_request TEXT,
                  verification_evidence_json TEXT,
                  archived INTEGER NOT NULL DEFAULT 0 CHECK(archived IN (0,1)),
                  archive_reason TEXT,
                  version INTEGER NOT NULL DEFAULT 1,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS dependencies (
                  work_item_id TEXT NOT NULL REFERENCES work_items(id),
                  depends_on_id TEXT NOT NULL REFERENCES work_items(id),
                  PRIMARY KEY(work_item_id, depends_on_id),
                  CHECK(work_item_id <> depends_on_id)
                );
                CREATE TABLE IF NOT EXISTS wakeups (
                  id TEXT PRIMARY KEY,
                  work_item_id TEXT NOT NULL REFERENCES work_items(id),
                  mission_id TEXT NOT NULL REFERENCES missions(id),
                  due_at TEXT NOT NULL,
                  condition TEXT,
                  status TEXT NOT NULL DEFAULT 'scheduled'
                    CHECK(status IN ('scheduled','claimed','completed','cancelled')),
                  claimed_by TEXT,
                  lease_expires_at TEXT,
                  version INTEGER NOT NULL DEFAULT 1,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS one_live_wakeup_per_item
                  ON wakeups(work_item_id) WHERE status IN ('scheduled','claimed');
                CREATE TABLE IF NOT EXISTS execution_claims (
                  id TEXT PRIMARY KEY,
                  work_item_id TEXT NOT NULL UNIQUE REFERENCES work_items(id),
                  mission_id TEXT NOT NULL REFERENCES missions(id),
                  worker_id TEXT NOT NULL,
                  status TEXT NOT NULL CHECK(status IN ('claimed','released')),
                  lease_expires_at TEXT NOT NULL,
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
            self._ensure_column("missions", "board_revision", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column("missions", "closed_at", "TEXT")
            self._ensure_column("missions", "closure_evidence_json", "TEXT")

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

    def mission_get_for_agent(self, mission_id: str, agent_id: str) -> dict[str, Any]:
        """Read a mission when the agent owns it or has work assigned on its board."""
        with self._lock:
            self._must("agents", agent_id, "Agent")
            mission = self._must("missions", mission_id, "Mission")
            assigned = self.db.execute(
                "SELECT 1 FROM work_items WHERE mission_id=? AND assignee_agent_id=? LIMIT 1",
                (mission_id, agent_id),
            ).fetchone()
            self._require(
                mission["owner_agent_id"] == agent_id or assigned,
                "FORBIDDEN",
                "Agent does not participate in this mission",
                agent_id=agent_id,
                mission_id=mission_id,
            )
            return mission

    def mission_list_for_agent(self, agent_id: str) -> list[dict[str, Any]]:
        with self._lock:
            self._must("agents", agent_id, "Agent")
            return self._all(
                "SELECT DISTINCT m.* FROM missions m "
                "LEFT JOIN work_items w ON w.mission_id=m.id "
                "WHERE m.owner_agent_id=? OR w.assignee_agent_id=? ORDER BY m.created_at,m.id",
                (agent_id, agent_id),
            )

    def mission_close(
        self,
        *,
        mission_id: str,
        owner_agent_id: str,
        expected_version: int,
        closure_evidence: list[dict[str, Any]],
        idempotency_key: str,
    ) -> dict[str, Any]:
        request = {
            "mission_id": mission_id,
            "owner_agent_id": owner_agent_id,
            "expected_version": expected_version,
            "closure_evidence": closure_evidence,
        }

        def operation() -> dict[str, Any]:
            mission = self._mission_open(mission_id)
            self._version(mission, expected_version)
            self._require(
                mission["owner_agent_id"] == owner_agent_id,
                "FORBIDDEN",
                "Only the owner may close the mission",
            )
            unfinished = self.db.execute(
                "SELECT COUNT(*) FROM work_items "
                "WHERE mission_id=? AND archived=0 AND state<>'done'",
                (mission_id,),
            ).fetchone()[0]
            live_wakes = self.db.execute(
                "SELECT COUNT(*) FROM wakeups "
                "WHERE mission_id=? AND status IN ('scheduled','claimed')",
                (mission_id,),
            ).fetchone()[0]
            self._require(
                not unfinished and not live_wakes,
                "PRECONDITION_FAILED",
                "Mission still has active work or wakeups",
            )
            self._validate_evidence(closure_evidence, "mission closure")
            now = self._now()
            self.db.execute(
                "UPDATE missions SET status='closed',version=version+1,closed_at=?,"
                "closure_evidence_json=?,updated_at=? WHERE id=?",
                (now, self._json(closure_evidence), now, mission_id),
            )
            closed = self.mission_get(mission_id)
            event_id = self._event(
                "mission.closed",
                mission_id=mission_id,
                actor_id=owner_agent_id,
                payload={"mission": closed, "evidence": closure_evidence},
            )
            return {"mission": closed, "event_id": event_id}

        return self._mutate("mission_close", idempotency_key, request, operation)

    def audit_list(
        self,
        mission_id: str | None = None,
        *,
        after_seq: int = 0,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        with self._lock:
            size = max(1, min(limit, 500))
            if mission_id is None:
                return self._all(
                    "SELECT * FROM events WHERE seq>? ORDER BY seq LIMIT ?", (after_seq, size)
                )
            self._must("missions", mission_id, "Mission")
            return self._all(
                "SELECT * FROM events WHERE mission_id=? AND seq>? ORDER BY seq LIMIT ?",
                (mission_id, after_seq, size),
            )

    # --- Board -------------------------------------------------------

    def _open_dependency_ids(self, work_item_id: str) -> list[str]:
        return [
            row["depends_on_id"]
            for row in self.db.execute(
                "SELECT d.depends_on_id FROM dependencies d "
                "JOIN work_items w ON w.id=d.depends_on_id "
                "WHERE d.work_item_id=? AND w.state<>'done' ORDER BY d.depends_on_id",
                (work_item_id,),
            )
        ]

    def _would_cycle(self, work_item_id: str, dependency_id: str) -> bool:
        pending, seen = [dependency_id], set()
        while pending:
            current = pending.pop()
            if current == work_item_id:
                return True
            if current in seen:
                continue
            seen.add(current)
            pending.extend(
                row["depends_on_id"]
                for row in self.db.execute(
                    "SELECT depends_on_id FROM dependencies WHERE work_item_id=?",
                    (current,),
                )
            )
        return False

    def _validate_dependency(
        self, mission_id: str, work_item_id: str, dependency_id: str
    ) -> dict[str, Any]:
        dependency = self._must("work_items", dependency_id, "Dependency")
        self._require(
            dependency["mission_id"] == mission_id,
            "OUT_OF_SCOPE",
            "Dependency must be in the same mission",
        )
        self._require(
            not dependency["archived"],
            "PRECONDITION_FAILED",
            "Archived work cannot be an active dependency",
        )
        self._require(
            dependency_id != work_item_id,
            "VALIDATION_ERROR",
            "A work item cannot depend on itself",
        )
        self._require(
            not self._would_cycle(work_item_id, dependency_id),
            "DEPENDENCY_CYCLE",
            "Dependency would create a cycle",
        )
        return dependency

    def _cancel_wakes(self, work_item_id: str) -> None:
        self.db.execute(
            "UPDATE wakeups SET status='cancelled',version=version+1,updated_at=? "
            "WHERE work_item_id=? AND status IN ('scheduled','claimed')",
            (self._now(), work_item_id),
        )

    def _schedule_wakeup(self, item: dict[str, Any], due_at: str, condition: str | None) -> str:
        wakeup_id, now = self._id(), self._now()
        self.db.execute(
            "INSERT INTO wakeups(id,work_item_id,mission_id,due_at,condition,created_at,updated_at) "
            "VALUES(?,?,?,?,?,?,?)",
            (wakeup_id, item["id"], item["mission_id"], due_at, condition, now, now),
        )
        return wakeup_id

    def _unblock_dependents(self, dependency_id: str) -> list[str]:
        unblocked = []
        dependent_ids = [
            row["work_item_id"]
            for row in self.db.execute(
                "SELECT work_item_id FROM dependencies WHERE depends_on_id=? ORDER BY work_item_id",
                (dependency_id,),
            )
        ]
        for dependent_id in dependent_ids:
            dependent = self._must("work_items", dependent_id, "Dependent work")
            if (
                not dependent["archived"]
                and dependent["state"] == "blocked"
                and not self._open_dependency_ids(dependent_id)
            ):
                self.db.execute(
                    "UPDATE work_items SET state='doing',version=version+1,updated_at=? WHERE id=?",
                    (self._now(), dependent_id),
                )
                unblocked.append(dependent_id)
        return unblocked

    def _reblock_dependents(self, dependency_id: str) -> list[str]:
        """Recursively invalidate every active downstream card, including Done cards."""
        pending, seen, reblocked = [dependency_id], {dependency_id}, []
        while pending:
            current = pending.pop(0)
            dependent_ids = [
                row["work_item_id"]
                for row in self.db.execute(
                    "SELECT work_item_id FROM dependencies "
                    "WHERE depends_on_id=? ORDER BY work_item_id",
                    (current,),
                )
            ]
            for dependent_id in dependent_ids:
                if dependent_id in seen:
                    continue
                seen.add(dependent_id)
                dependent = self._must("work_items", dependent_id, "Dependent work")
                if dependent["archived"]:
                    continue
                if dependent["state"] != "blocked":
                    self._cancel_wakes(dependent_id)
                    self.db.execute(
                        "UPDATE work_items SET state='blocked',wait_condition=NULL,"
                        "next_check_at=NULL,input_request=NULL,verification_evidence_json=NULL,"
                        "version=version+1,updated_at=? WHERE id=?",
                        (self._now(), dependent_id),
                    )
                    reblocked.append(dependent_id)
                pending.append(dependent_id)
        return reblocked

    def work_item_create(
        self,
        *,
        mission_id: str,
        actor_agent_id: str,
        title: str,
        idempotency_key: str,
        description: str = "",
        state: str = "doing",
        assignee_agent_id: str | None = None,
        dependency_ids: list[str] | None = None,
        next_check_at: str | None = None,
        wait_condition: str | None = None,
        input_request: str | None = None,
    ) -> dict[str, Any]:
        request = {
            "mission_id": mission_id,
            "actor_agent_id": actor_agent_id,
            "title": title,
            "description": description,
            "state": state,
            "assignee_agent_id": assignee_agent_id,
            "dependency_ids": dependency_ids,
            "next_check_at": next_check_at,
            "wait_condition": wait_condition,
            "input_request": input_request,
        }

        def operation() -> dict[str, Any]:
            mission = self._mission_open(mission_id)
            self._require(
                mission["owner_agent_id"] == actor_agent_id,
                "FORBIDDEN",
                "Only the owner may create work",
            )
            self._require(
                isinstance(title, str) and title.strip(),
                "VALIDATION_ERROR",
                "title is required",
            )
            self._require(
                isinstance(description, str),
                "VALIDATION_ERROR",
                "description must be text",
            )
            self._require(
                state in WORK_STATES and state != "done",
                "INVALID_STATE",
                "Invalid initial state",
            )
            assignee = assignee_agent_id or actor_agent_id
            self._must("agents", assignee, "Assignee")
            dependencies = [] if dependency_ids is None else dependency_ids
            self._require(
                isinstance(dependencies, list)
                and all(isinstance(dependency, str) for dependency in dependencies),
                "VALIDATION_ERROR",
                "dependency_ids must be a list of work item IDs",
            )
            self._require(
                len(dependencies) == len(set(dependencies)),
                "VALIDATION_ERROR",
                "Dependencies must be unique",
            )
            work_item_id, now = self._id(), self._now()
            dependency_records = [
                self._validate_dependency(mission_id, work_item_id, dependency_id)
                for dependency_id in dependencies
            ]
            open_dependencies = [
                dependency for dependency in dependency_records if dependency["state"] != "done"
            ]
            due = None
            if state == "waiting":
                self._require(next_check_at, "INVALID_STATE", "Waiting requires next_check_at")
                due = self._iso(next_check_at, "next_check_at")
                self._require(
                    self._parse_time(due, "next_check_at") > self._parse_time(now, "clock"),
                    "INVALID_STATE",
                    "next_check_at must be in the future",
                )
            if state == "blocked":
                self._require(
                    open_dependencies,
                    "INVALID_STATE",
                    "Blocked requires at least one open dependency",
                )
            elif open_dependencies:
                raise DomainError(
                    "INVALID_STATE",
                    "Work with an open dependency must start Blocked",
                    dependency_ids=[dependency["id"] for dependency in open_dependencies],
                )
            if state == "needs_input":
                self._require(
                    isinstance(input_request, str) and input_request.strip(),
                    "INVALID_STATE",
                    "Needs Input requires a concrete request",
                )
            self.db.execute(
                "INSERT INTO work_items(id,mission_id,title,description,state,assignee_agent_id,"
                "wait_condition,next_check_at,input_request,created_at,updated_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (
                    work_item_id,
                    mission_id,
                    title.strip(),
                    description,
                    state,
                    assignee,
                    wait_condition if state == "waiting" else None,
                    due,
                    input_request.strip() if state == "needs_input" else None,
                    now,
                    now,
                ),
            )
            for dependency_id in dependencies:
                self.db.execute(
                    "INSERT INTO dependencies(work_item_id,depends_on_id) VALUES(?,?)",
                    (work_item_id, dependency_id),
                )
            item = self.work_item_get(work_item_id)
            wakeup_ids = [self._schedule_wakeup(item, due, wait_condition)] if due else []
            revision = self._bump_board(mission_id)
            event_id = self._event(
                "work.created",
                mission_id=mission_id,
                actor_id=actor_agent_id,
                payload={"work_item": item},
            )
            return {
                "work_item": item,
                "board_revision": revision,
                "wake_ids": wakeup_ids,
                "event_id": event_id,
            }

        return self._mutate("work_item_create", idempotency_key, request, operation)

    def work_item_get(self, work_item_id: str) -> dict[str, Any]:
        with self._lock:
            item = self._must("work_items", work_item_id, "Work item")
            item["dependency_ids"] = [
                row["depends_on_id"]
                for row in self.db.execute(
                    "SELECT depends_on_id FROM dependencies "
                    "WHERE work_item_id=? ORDER BY depends_on_id",
                    (work_item_id,),
                )
            ]
            return item

    def work_item_list(self, mission_id: str, state: str | None = None) -> list[dict[str, Any]]:
        with self._lock:
            self._must("missions", mission_id, "Mission")
            query, arguments = "SELECT id FROM work_items WHERE mission_id=?", [mission_id]
            if state is not None:
                self._require(state in WORK_STATES, "VALIDATION_ERROR", "Unknown work state")
                query += " AND state=?"
                arguments.append(state)
            return [
                self.work_item_get(row["id"])
                for row in self.db.execute(query + " ORDER BY created_at,id", arguments)
            ]

    def work_item_update(
        self,
        *,
        work_item_id: str,
        actor_agent_id: str,
        expected_version: int,
        idempotency_key: str,
        title: str | None = None,
        description: str | None = None,
    ) -> dict[str, Any]:
        request = {
            "work_item_id": work_item_id,
            "actor_agent_id": actor_agent_id,
            "expected_version": expected_version,
            "title": title,
            "description": description,
        }

        def operation() -> dict[str, Any]:
            item = self.work_item_get(work_item_id)
            self._mission_open(item["mission_id"])
            self._writer(item, actor_agent_id)
            self._version(item, expected_version)
            self._require(
                title is not None or description is not None,
                "VALIDATION_ERROR",
                "No update supplied",
            )
            if title is not None:
                self._require(
                    isinstance(title, str) and title.strip(),
                    "VALIDATION_ERROR",
                    "title must not be empty",
                )
            if description is not None:
                self._require(
                    isinstance(description, str),
                    "VALIDATION_ERROR",
                    "description must be text",
                )
            self.db.execute(
                "UPDATE work_items SET title=COALESCE(?,title),description=COALESCE(?,description),"
                "version=version+1,updated_at=? WHERE id=?",
                (
                    title.strip() if title is not None else None,
                    description,
                    self._now(),
                    work_item_id,
                ),
            )
            updated = self.work_item_get(work_item_id)
            revision = self._bump_board(item["mission_id"])
            event_id = self._event(
                "work.updated",
                mission_id=item["mission_id"],
                actor_id=actor_agent_id,
                payload={"work_item": updated},
            )
            return {"work_item": updated, "board_revision": revision, "event_id": event_id}

        return self._mutate("work_item_update", idempotency_key, request, operation)

    def work_item_assign(
        self,
        *,
        work_item_id: str,
        owner_agent_id: str,
        assignee_agent_id: str,
        expected_version: int,
        idempotency_key: str,
    ) -> dict[str, Any]:
        request = {
            "work_item_id": work_item_id,
            "owner_agent_id": owner_agent_id,
            "assignee_agent_id": assignee_agent_id,
            "expected_version": expected_version,
        }

        def operation() -> dict[str, Any]:
            item = self.work_item_get(work_item_id)
            mission = self._mission_open(item["mission_id"])
            self._version(item, expected_version)
            self._require(
                mission["owner_agent_id"] == owner_agent_id,
                "FORBIDDEN",
                "Only the mission owner may assign work",
            )
            self._require(not item["archived"], "WORK_ARCHIVED", "Archived work is immutable")
            self._must("agents", assignee_agent_id, "Assignee")
            self.db.execute(
                "UPDATE work_items SET assignee_agent_id=?,version=version+1,updated_at=? WHERE id=?",
                (assignee_agent_id, self._now(), work_item_id),
            )
            updated = self.work_item_get(work_item_id)
            revision = self._bump_board(item["mission_id"])
            event_id = self._event(
                "work.assigned",
                mission_id=item["mission_id"],
                actor_id=owner_agent_id,
                payload={"work_item": updated},
            )
            return {"work_item": updated, "board_revision": revision, "event_id": event_id}

        return self._mutate("work_item_assign", idempotency_key, request, operation)

    def work_item_add_dependency(
        self,
        *,
        work_item_id: str,
        dependency_id: str,
        actor_agent_id: str,
        expected_version: int,
        idempotency_key: str,
    ) -> dict[str, Any]:
        request = {
            "work_item_id": work_item_id,
            "dependency_id": dependency_id,
            "actor_agent_id": actor_agent_id,
            "expected_version": expected_version,
        }

        def operation() -> dict[str, Any]:
            item = self.work_item_get(work_item_id)
            self._mission_open(item["mission_id"])
            self._writer(item, actor_agent_id)
            self._version(item, expected_version)
            dependency = self._validate_dependency(item["mission_id"], work_item_id, dependency_id)
            exists = self.db.execute(
                "SELECT 1 FROM dependencies WHERE work_item_id=? AND depends_on_id=?",
                (work_item_id, dependency_id),
            ).fetchone()
            self._require(not exists, "CONFLICT", "Dependency already exists")
            self.db.execute(
                "INSERT INTO dependencies(work_item_id,depends_on_id) VALUES(?,?)",
                (work_item_id, dependency_id),
            )
            became_blocked = dependency["state"] != "done" and item["state"] != "blocked"
            was_done = item["state"] == "done"
            if became_blocked:
                self._cancel_wakes(work_item_id)
                self.db.execute(
                    "UPDATE work_items SET state='blocked',wait_condition=NULL,next_check_at=NULL,"
                    "input_request=NULL,verification_evidence_json=NULL,version=version+1,"
                    "updated_at=? WHERE id=?",
                    (self._now(), work_item_id),
                )
            else:
                self.db.execute(
                    "UPDATE work_items SET version=version+1,updated_at=? WHERE id=?",
                    (self._now(), work_item_id),
                )
            reblocked = (
                self._reblock_dependents(work_item_id) if was_done and became_blocked else []
            )
            updated = self.work_item_get(work_item_id)
            revision = self._bump_board(item["mission_id"])
            event_id = self._event(
                "work.dependency_added",
                mission_id=item["mission_id"],
                actor_id=actor_agent_id,
                payload={
                    "work_item": updated,
                    "dependency_id": dependency_id,
                    "reblocked": reblocked,
                },
            )
            return {
                "work_item": updated,
                "board_revision": revision,
                "reblocked_work_item_ids": reblocked,
                "event_id": event_id,
            }

        return self._mutate("work_item_add_dependency", idempotency_key, request, operation)

    def work_item_remove_dependency(
        self,
        *,
        work_item_id: str,
        dependency_id: str,
        actor_agent_id: str,
        expected_version: int,
        idempotency_key: str,
    ) -> dict[str, Any]:
        request = {
            "work_item_id": work_item_id,
            "dependency_id": dependency_id,
            "actor_agent_id": actor_agent_id,
            "expected_version": expected_version,
        }

        def operation() -> dict[str, Any]:
            item = self.work_item_get(work_item_id)
            self._mission_open(item["mission_id"])
            self._writer(item, actor_agent_id)
            self._version(item, expected_version)
            self._require(
                dependency_id in item["dependency_ids"],
                "NOT_FOUND",
                "Dependency edge not found",
                dependency_id=dependency_id,
            )
            self.db.execute(
                "DELETE FROM dependencies WHERE work_item_id=? AND depends_on_id=?",
                (work_item_id, dependency_id),
            )
            became_actionable = item["state"] == "blocked" and not self._open_dependency_ids(
                work_item_id
            )
            new_state = "doing" if became_actionable else item["state"]
            self.db.execute(
                "UPDATE work_items SET state=?,version=version+1,updated_at=? WHERE id=?",
                (new_state, self._now(), work_item_id),
            )
            updated = self.work_item_get(work_item_id)
            revision = self._bump_board(item["mission_id"])
            event_id = self._event(
                "work.dependency_removed",
                mission_id=item["mission_id"],
                actor_id=actor_agent_id,
                payload={
                    "work_item": updated,
                    "dependency_id": dependency_id,
                    "became_actionable": became_actionable,
                },
            )
            return {
                "work_item": updated,
                "board_revision": revision,
                "became_actionable": became_actionable,
                "event_id": event_id,
            }

        return self._mutate("work_item_remove_dependency", idempotency_key, request, operation)

    def work_item_transition(
        self,
        *,
        work_item_id: str,
        actor_agent_id: str,
        expected_version: int,
        new_state: str,
        idempotency_key: str,
        next_check_at: str | None = None,
        wait_condition: str | None = None,
        input_request: str | None = None,
        verification_evidence: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        request = {
            "work_item_id": work_item_id,
            "actor_agent_id": actor_agent_id,
            "expected_version": expected_version,
            "new_state": new_state,
            "next_check_at": next_check_at,
            "wait_condition": wait_condition,
            "input_request": input_request,
            "verification_evidence": verification_evidence,
        }

        def operation() -> dict[str, Any]:
            item = self.work_item_get(work_item_id)
            self._mission_open(item["mission_id"])
            self._writer(item, actor_agent_id)
            self._version(item, expected_version)
            self._require(new_state in WORK_STATES, "INVALID_STATE", "Unknown work state")
            open_dependencies = self._open_dependency_ids(work_item_id)
            self._require(
                not open_dependencies or new_state == "blocked",
                "PRECONDITION_FAILED",
                "Open dependencies require the Blocked state",
                dependency_ids=open_dependencies,
            )
            due = None
            if new_state == "waiting":
                self._require(next_check_at, "INVALID_STATE", "Waiting requires next_check_at")
                due = self._iso(next_check_at, "next_check_at")
                self._require(
                    self._parse_time(due, "next_check_at") > self._parse_time(self._now(), "clock"),
                    "INVALID_STATE",
                    "next_check_at must be in the future",
                )
            elif new_state == "blocked":
                self._require(
                    open_dependencies,
                    "INVALID_STATE",
                    "Blocked requires at least one open dependency",
                )
            elif new_state == "needs_input":
                self._require(
                    isinstance(input_request, str) and input_request.strip(),
                    "INVALID_STATE",
                    "Needs Input requires a concrete request",
                )
            elif new_state == "done":
                self._validate_evidence(
                    verification_evidence or [], "completion", code="INVALID_STATE"
                )
            self._cancel_wakes(work_item_id)
            self.db.execute(
                "UPDATE work_items SET state=?,wait_condition=?,next_check_at=?,input_request=?,"
                "verification_evidence_json=?,version=version+1,updated_at=? WHERE id=?",
                (
                    new_state,
                    wait_condition if new_state == "waiting" else None,
                    due,
                    input_request.strip() if new_state == "needs_input" else None,
                    self._json(verification_evidence) if new_state == "done" else None,
                    self._now(),
                    work_item_id,
                ),
            )
            updated = self.work_item_get(work_item_id)
            wakeup_ids = [self._schedule_wakeup(updated, due, wait_condition)] if due else []
            unblocked = self._unblock_dependents(work_item_id) if new_state == "done" else []
            reblocked = (
                self._reblock_dependents(work_item_id)
                if item["state"] == "done" and new_state != "done"
                else []
            )
            revision = self._bump_board(item["mission_id"])
            event_id = self._event(
                "work.transitioned",
                mission_id=item["mission_id"],
                actor_id=actor_agent_id,
                payload={
                    "work_item": updated,
                    "from": item["state"],
                    "to": new_state,
                    "unblocked": unblocked,
                    "reblocked": reblocked,
                    "verification_evidence": (
                        verification_evidence if new_state == "done" else None
                    ),
                },
            )
            return {
                "work_item": updated,
                "board_revision": revision,
                "wake_ids": wakeup_ids,
                "unblocked_work_item_ids": unblocked,
                "reblocked_work_item_ids": reblocked,
                "event_id": event_id,
            }

        return self._mutate("work_item_transition", idempotency_key, request, operation)

    def work_item_archive(
        self,
        *,
        work_item_id: str,
        owner_agent_id: str,
        expected_version: int,
        reason: str,
        idempotency_key: str,
    ) -> dict[str, Any]:
        request = {
            "work_item_id": work_item_id,
            "owner_agent_id": owner_agent_id,
            "expected_version": expected_version,
            "reason": reason,
        }

        def operation() -> dict[str, Any]:
            item = self.work_item_get(work_item_id)
            mission = self._mission_open(item["mission_id"])
            self._version(item, expected_version)
            self._require(
                mission["owner_agent_id"] == owner_agent_id,
                "FORBIDDEN",
                "Only the owner may archive work",
            )
            self._require(not item["archived"], "WORK_ARCHIVED", "Work is already archived")
            self._require(
                isinstance(reason, str) and reason.strip(),
                "VALIDATION_ERROR",
                "Archive reason is required",
            )
            active_dependents = self.db.execute(
                "SELECT COUNT(*) FROM dependencies d "
                "JOIN work_items w ON w.id=d.work_item_id "
                "WHERE d.depends_on_id=? AND w.archived=0",
                (work_item_id,),
            ).fetchone()[0]
            self._require(
                not active_dependents,
                "PRECONDITION_FAILED",
                "Remove or archive active dependents before archiving this work",
                active_dependent_count=active_dependents,
            )
            self._cancel_wakes(work_item_id)
            self.db.execute(
                "UPDATE work_items SET archived=1,archive_reason=?,version=version+1,"
                "updated_at=? WHERE id=?",
                (reason.strip(), self._now(), work_item_id),
            )
            archived = self.work_item_get(work_item_id)
            revision = self._bump_board(item["mission_id"])
            event_id = self._event(
                "work.archived",
                mission_id=item["mission_id"],
                actor_id=owner_agent_id,
                payload={"work_item": archived, "reason": reason.strip()},
            )
            return {"work_item": archived, "board_revision": revision, "event_id": event_id}

        return self._mutate("work_item_archive", idempotency_key, request, operation)

    def board_snapshot(self, mission_id: str) -> dict[str, Any]:
        with self._lock:
            mission = self.mission_get(mission_id)
            items = self.work_item_list(mission_id)
            now = self._now()
            return {
                "mission": mission,
                "work_items": items,
                "ready_work": [
                    item for item in items if item["state"] == "doing" and not item["archived"]
                ],
                "wakeups": self._all(
                    "SELECT * FROM wakeups WHERE mission_id=? "
                    "AND status IN ('scheduled','claimed') ORDER BY due_at,id",
                    (mission_id,),
                ),
                "execution_claims": self._all(
                    "SELECT * FROM execution_claims WHERE mission_id=? AND status='claimed' "
                    "AND julianday(lease_expires_at)>julianday(?) ORDER BY updated_at,id",
                    (mission_id, now),
                ),
            }

    # --- Runtime leases ---------------------------------------------

    def ready_work_claim(
        self,
        *,
        worker_id: str,
        idempotency_key: str,
        limit: int = 10,
        lease_seconds: int = 300,
    ) -> dict[str, Any]:
        request = {
            "worker_id": worker_id,
            "limit": limit,
            "lease_seconds": lease_seconds,
        }

        def operation() -> dict[str, Any]:
            self._require(
                isinstance(worker_id, str) and worker_id.strip(),
                "VALIDATION_ERROR",
                "worker_id is required",
            )
            self._require(
                isinstance(limit, int) and limit > 0,
                "VALIDATION_ERROR",
                "limit must be positive",
            )
            self._require(
                isinstance(lease_seconds, int) and lease_seconds > 0,
                "VALIDATION_ERROR",
                "lease_seconds must be positive",
            )
            now, lease_expires_at = self._now(), self._lease_until(lease_seconds)
            rows = self.db.execute(
                "SELECT i.id FROM work_items i "
                "JOIN missions m ON m.id=i.mission_id "
                "LEFT JOIN execution_claims c ON c.work_item_id=i.id "
                "WHERE i.state='doing' AND i.archived=0 AND m.status='open' "
                "AND NOT EXISTS (SELECT 1 FROM dependencies d JOIN work_items p "
                "ON p.id=d.depends_on_id WHERE d.work_item_id=i.id AND p.state<>'done') "
                "AND (c.id IS NULL OR c.status='released' "
                "OR julianday(c.lease_expires_at)<=julianday(?)) "
                "ORDER BY i.updated_at,i.id LIMIT ?",
                (now, min(limit, 100)),
            ).fetchall()
            claims, event_ids = [], []
            for row in rows:
                item = self.work_item_get(row["id"])
                current = self._one(
                    "SELECT * FROM execution_claims WHERE work_item_id=?", (item["id"],)
                )
                if current:
                    claim_id = current["id"]
                    self.db.execute(
                        "UPDATE execution_claims SET worker_id=?,status='claimed',"
                        "lease_expires_at=?,version=version+1,updated_at=? WHERE id=?",
                        (worker_id.strip(), lease_expires_at, now, claim_id),
                    )
                else:
                    claim_id = self._id()
                    self.db.execute(
                        "INSERT INTO execution_claims(id,work_item_id,mission_id,worker_id,status,"
                        "lease_expires_at,created_at,updated_at) VALUES(?,?,?,?,'claimed',?,?,?)",
                        (
                            claim_id,
                            item["id"],
                            item["mission_id"],
                            worker_id.strip(),
                            lease_expires_at,
                            now,
                            now,
                        ),
                    )
                claim = self._must("execution_claims", claim_id, "Execution claim")
                revision = self._bump_board(item["mission_id"])
                event_ids.append(
                    self._event(
                        "work.execution_claimed",
                        mission_id=item["mission_id"],
                        actor_id=worker_id.strip(),
                        payload={"claim": claim, "work_item_id": item["id"]},
                    )
                )
                claims.append({"claim": claim, "work_item": item, "board_revision": revision})
            if not event_ids:
                event_ids.append(
                    self._event(
                        "work.execution_claimed",
                        mission_id=None,
                        actor_id=worker_id.strip(),
                        payload={"claim_ids": []},
                    )
                )
            return {"claims": claims, "event_ids": event_ids}

        return self._mutate("ready_work_claim", idempotency_key, request, operation)

    def ready_work_release(
        self,
        *,
        claim_id: str,
        worker_id: str,
        expected_version: int,
        idempotency_key: str,
    ) -> dict[str, Any]:
        request = {
            "claim_id": claim_id,
            "worker_id": worker_id,
            "expected_version": expected_version,
        }

        def operation() -> dict[str, Any]:
            claim = self._must("execution_claims", claim_id, "Execution claim")
            self._version(claim, expected_version)
            self._require(
                claim["status"] == "claimed" and claim["worker_id"] == worker_id,
                "LEASE_CONFLICT",
                "Worker does not own this execution lease",
            )
            self.db.execute(
                "UPDATE execution_claims SET status='released',version=version+1,updated_at=? "
                "WHERE id=?",
                (self._now(), claim_id),
            )
            released = self._must("execution_claims", claim_id, "Execution claim")
            revision = self._bump_board(claim["mission_id"])
            event_id = self._event(
                "work.execution_released",
                mission_id=claim["mission_id"],
                actor_id=worker_id,
                payload={"claim": released},
            )
            return {
                "claim": released,
                "work_item": self.work_item_get(claim["work_item_id"]),
                "board_revision": revision,
                "event_id": event_id,
            }

        return self._mutate("ready_work_release", idempotency_key, request, operation)

    def wakeup_claim_due(
        self,
        *,
        worker_id: str,
        idempotency_key: str,
        limit: int = 10,
        lease_seconds: int = 300,
    ) -> dict[str, Any]:
        request = {
            "worker_id": worker_id,
            "limit": limit,
            "lease_seconds": lease_seconds,
        }

        def operation() -> dict[str, Any]:
            self._require(
                isinstance(worker_id, str) and worker_id.strip(),
                "VALIDATION_ERROR",
                "worker_id is required",
            )
            self._require(
                isinstance(limit, int) and limit > 0,
                "VALIDATION_ERROR",
                "limit must be positive",
            )
            self._require(
                isinstance(lease_seconds, int) and lease_seconds > 0,
                "VALIDATION_ERROR",
                "lease_seconds must be positive",
            )
            now, lease_expires_at = self._now(), self._lease_until(lease_seconds)
            rows = self.db.execute(
                "SELECT w.id FROM wakeups w "
                "JOIN work_items i ON i.id=w.work_item_id "
                "JOIN missions m ON m.id=w.mission_id "
                "WHERE i.state='waiting' AND i.archived=0 AND m.status='open' "
                "AND julianday(w.due_at)<=julianday(?) "
                "AND (w.status='scheduled' OR (w.status='claimed' "
                "AND julianday(w.lease_expires_at)<=julianday(?))) "
                "ORDER BY w.due_at,w.id LIMIT ?",
                (now, now, min(limit, 100)),
            ).fetchall()
            wakeups, event_ids = [], []
            for row in rows:
                self.db.execute(
                    "UPDATE wakeups SET status='claimed',claimed_by=?,lease_expires_at=?,"
                    "version=version+1,updated_at=? WHERE id=?",
                    (worker_id.strip(), lease_expires_at, now, row["id"]),
                )
                wakeup = self._must("wakeups", row["id"], "Wakeup")
                revision = self._bump_board(wakeup["mission_id"])
                wakeup["work_item"] = self.work_item_get(wakeup["work_item_id"])
                wakeup["board_revision"] = revision
                wakeups.append(wakeup)
                event_ids.append(
                    self._event(
                        "wakeup.claimed",
                        mission_id=wakeup["mission_id"],
                        actor_id=worker_id.strip(),
                        payload={"wakeup_id": wakeup["id"]},
                    )
                )
            if not event_ids:
                event_ids.append(
                    self._event(
                        "wakeup.claimed",
                        mission_id=None,
                        actor_id=worker_id.strip(),
                        payload={"wakeup_ids": []},
                    )
                )
            return {"wakeups": wakeups, "event_ids": event_ids}

        return self._mutate("wakeup_claim_due", idempotency_key, request, operation)

    def wakeup_resolve(
        self,
        *,
        wakeup_id: str,
        worker_id: str,
        expected_version: int,
        ready: bool,
        idempotency_key: str,
        next_check_at: str | None = None,
        condition: str | None = None,
    ) -> dict[str, Any]:
        request = {
            "wakeup_id": wakeup_id,
            "worker_id": worker_id,
            "expected_version": expected_version,
            "ready": ready,
            "next_check_at": next_check_at,
            "condition": condition,
        }

        def operation() -> dict[str, Any]:
            wakeup = self._must("wakeups", wakeup_id, "Wakeup")
            self._version(wakeup, expected_version)
            self._require(
                wakeup["status"] == "claimed" and wakeup["claimed_by"] == worker_id,
                "LEASE_CONFLICT",
                "Worker does not own this wake lease",
            )
            self._require(
                self._parse_time(wakeup["lease_expires_at"], "lease_expires_at")
                > self._parse_time(self._now(), "clock"),
                "LEASE_CONFLICT",
                "Wake lease expired",
            )
            self._require(isinstance(ready, bool), "VALIDATION_ERROR", "ready must be boolean")
            item = self.work_item_get(wakeup["work_item_id"])
            self._mission_open(item["mission_id"])
            self._require(
                item["state"] == "waiting" and not item["archived"],
                "INVALID_STATE",
                "Work item is no longer waiting",
            )
            now = self._now()
            self.db.execute(
                "UPDATE wakeups SET status='completed',version=version+1,updated_at=? WHERE id=?",
                (now, wakeup_id),
            )
            new_wakeup_id = None
            if ready:
                self._require(
                    not self._open_dependency_ids(item["id"]),
                    "PRECONDITION_FAILED",
                    "Open dependencies still block this work",
                )
                self.db.execute(
                    "UPDATE work_items SET state='doing',wait_condition=NULL,next_check_at=NULL,"
                    "version=version+1,updated_at=? WHERE id=?",
                    (now, item["id"]),
                )
            else:
                self._require(
                    next_check_at,
                    "INVALID_STATE",
                    "An unmet condition requires next_check_at",
                )
                due = self._iso(next_check_at, "next_check_at")
                self._require(
                    self._parse_time(due, "next_check_at") > self._parse_time(now, "clock"),
                    "INVALID_STATE",
                    "next_check_at must be in the future",
                )
                next_condition = condition if condition is not None else item["wait_condition"]
                self.db.execute(
                    "UPDATE work_items SET wait_condition=?,next_check_at=?,version=version+1,"
                    "updated_at=? WHERE id=?",
                    (next_condition, due, now, item["id"]),
                )
                new_wakeup_id = self._schedule_wakeup(
                    self.work_item_get(item["id"]), due, next_condition
                )
            updated = self.work_item_get(item["id"])
            revision = self._bump_board(item["mission_id"])
            event_id = self._event(
                "wakeup.resolved",
                mission_id=item["mission_id"],
                actor_id=worker_id,
                payload={
                    "wakeup_id": wakeup_id,
                    "ready": ready,
                    "new_wakeup_id": new_wakeup_id,
                },
            )
            return {
                "work_item": updated,
                "board_revision": revision,
                "new_wakeup_id": new_wakeup_id,
                "event_id": event_id,
            }

        return self._mutate("wakeup_resolve", idempotency_key, request, operation)
