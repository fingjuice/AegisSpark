#!/usr/bin/env python3
"""
AegisSpark (ABE-Spark) defense simulator for malicious-worker paper experiments.

Faithfully mirrors the Write Verification write-gate trust chain in:
  org.apache.spark.abe.WriteVerificationTee
  abe-eac/native/src/write_verification_tee.cpp

Payload formats match Java/C++:
  TaskTicket:          job|task|worker_ip|allowed_target_path|expires_at
  AdminEndorsement:    app_code_hash|root1,root2|timestamp
  WriteConfirm:        job|task|worker_ip|target|payload_bytes|issued_at

Plus paper-level executor write request signature (sig_exe) verified against
an enrolled Worker trust list, and a lightweight CP-ABE policy check for SK_u.
"""

from __future__ import annotations

import hashlib
import re
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Sequence, Set, Tuple

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey


# ---------------------------------------------------------------------------
# Path helpers (aligned with HdfsPathUtil / PathUtil)
# ---------------------------------------------------------------------------

def normalize_path(hdfs_uri: str) -> str:
    out = (hdfs_uri or "").strip()
    if len(out) > 1 and out.endswith("/"):
        out = out[:-1]
    return out


def is_under_root(target: str, root: str) -> bool:
    t, r = normalize_path(target), normalize_path(root)
    if not t or not r:
        return False
    if t == r:
        return True
    if not t.startswith(r):
        return False
    return len(t) > len(r) and t[len(r)] == "/"


def is_under_any_root(target: str, roots: Sequence[str]) -> bool:
    return any(is_under_root(target, r) for r in roots)


def ticket_binds_target(ticket_path: str, write_target: str) -> bool:
    t, w = normalize_path(ticket_path), normalize_path(write_target)
    if not t or not w:
        return False
    return t == w or is_under_root(w, t)


# ---------------------------------------------------------------------------
# Crypto helpers (Ed25519 PureEdDSA, same as abe-spark.conf)
# ---------------------------------------------------------------------------

class EcdsaKeyPair:
    """Historically named EcdsaKeyPair; now implements Ed25519."""

    def __init__(self, private_key: Optional[Ed25519PrivateKey] = None):
        self._sk = private_key or Ed25519PrivateKey.generate()
        self._vk = self._sk.public_key()

    def sign(self, message: bytes) -> bytes:
        return self._sk.sign(message)

    def verify(self, message: bytes, signature: bytes) -> bool:
        try:
            self._vk.verify(signature, message)
            return True
        except InvalidSignature:
            return False

    def public_bytes(self) -> bytes:
        return self._vk.public_bytes(
            serialization.Encoding.DER,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )

    def fingerprint(self) -> str:
        return hashlib.sha256(self.public_bytes()).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Trust objects
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class AdminEndorsement:
    app_code_hash: str
    allowed_root_directories: Tuple[str, ...]
    timestamp: int
    admin_signature: bytes

    @staticmethod
    def build_payload(app_code_hash: str, roots: Sequence[str], timestamp: int) -> bytes:
        return f"{app_code_hash}|{','.join(roots)}|{timestamp}".encode("utf-8")


@dataclass(frozen=True)
class TaskTicket:
    job_id: str
    task_id: str
    worker_ip: str
    allowed_target_path: str
    expires_at: int
    driver_signature: bytes

    @staticmethod
    def build_payload(
        job_id: str,
        task_id: str,
        worker_ip: str,
        allowed_target_path: str,
        expires_at: int,
    ) -> bytes:
        return (
            f"{job_id}|{task_id}|{worker_ip}|{allowed_target_path}|{expires_at}"
        ).encode("utf-8")


@dataclass(frozen=True)
class WriteRequest:
    """Worker → EAC write request with executor signature (sig_exe)."""
    ticket: TaskTicket
    endorsement: AdminEndorsement
    target_path: str
    payload_bytes: int
    worker_id: str
    sig_exe: bytes
    # Scenario C (read/decrypt side)
    user_attributes: frozenset = frozenset()
    policy: str = ""
    now_epoch_sec: int = 0


@dataclass
class VerifyResult:
    ok: bool
    reason: str = ""
    latency_ns: int = 0


class AttackType(str, Enum):
    NONE = "benign"
    A_TICKET_TAMPER = "A_ticket_tamper"
    B_FORGED_SIG_EXE = "B_forged_sig_exe"
    C_REPLAY_OR_ATTR = "C_replay_or_attr"


# ---------------------------------------------------------------------------
# Lightweight CP-ABE policy evaluator (attribute set ⊆ policy tree)
# Supports: attr | (P and Q) | (P or Q)  — matches abe-spark.conf policy style
# ---------------------------------------------------------------------------

_TOKEN = re.compile(r"\(|\)|and|or|[A-Za-z0-9_.:-]+")


def _tokenize(policy: str) -> List[str]:
    return _TOKEN.findall(policy.strip().lower())


def evaluate_policy(policy: str, attrs: Set[str]) -> bool:
    """Return True iff attribute set satisfies CP-ABE policy expression."""
    if not policy:
        return True
    tokens = _tokenize(policy)
    attrs_l = {a.lower() for a in attrs}
    pos = 0

    def peek() -> Optional[str]:
        return tokens[pos] if pos < len(tokens) else None

    def consume(expected: Optional[str] = None) -> str:
        nonlocal pos
        if pos >= len(tokens):
            raise ValueError("unexpected end of policy")
        tok = tokens[pos]
        if expected is not None and tok != expected:
            raise ValueError(f"expected {expected}, got {tok}")
        pos += 1
        return tok

    def parse_or() -> bool:
        left = parse_and()
        while peek() == "or":
            consume("or")
            right = parse_and()
            left = left or right
        return left

    def parse_and() -> bool:
        left = parse_primary()
        while peek() == "and":
            consume("and")
            right = parse_primary()
            left = left and right
        return left

    def parse_primary() -> bool:
        tok = peek()
        if tok == "(":
            consume("(")
            val = parse_or()
            consume(")")
            return val
        attr = consume()
        return attr in attrs_l

    try:
        result = parse_or()
        return result and pos == len(tokens)
    except ValueError:
        return False


# ---------------------------------------------------------------------------
# AegisSpark EAC + Worker TEE defense
# ---------------------------------------------------------------------------

class AegisSparkDefense:
    """
    Local TEE write-gate + CP-ABE attribute gate.

    Verification order mirrors WriteVerificationTee (Java) with an added
    sig_exe trust-list check used by the paper's Scenario B.
    """

    def __init__(
        self,
        admin_vk: EcdsaKeyPair,
        driver_vk: EcdsaKeyPair,
        trusted_worker_keys: Dict[str, EcdsaKeyPair],
        name: str = "AegisSpark",
    ):
        self.name = name
        self._admin_vk = admin_vk
        self._driver_vk = driver_vk
        self._trusted_workers = dict(trusted_worker_keys)
        self._lock = threading.Lock()
        self.stats = {
            "total": 0,
            "accepted": 0,
            "rejected": 0,
            "reject_by_reason": {},
        }

    def enroll_worker(self, worker_id: str, key: EcdsaKeyPair) -> None:
        with self._lock:
            self._trusted_workers[worker_id] = key

    def _record(self, ok: bool, reason: str) -> None:
        with self._lock:
            self.stats["total"] += 1
            if ok:
                self.stats["accepted"] += 1
            else:
                self.stats["rejected"] += 1
                self.stats["reject_by_reason"][reason] = (
                    self.stats["reject_by_reason"].get(reason, 0) + 1
                )

    def verify_write(self, req: WriteRequest) -> VerifyResult:
        t0 = time.perf_counter_ns()

        # 1) Expiry (Java checks this first)
        if req.now_epoch_sec > req.ticket.expires_at:
            return self._fail("task ticket expired", t0)

        # 2) Admin whitelist
        if not is_under_any_root(
            req.target_path, req.endorsement.allowed_root_directories
        ):
            return self._fail("target path outside admin whitelist", t0)

        # 3) Ticket path scope
        if not ticket_binds_target(req.ticket.allowed_target_path, req.target_path):
            return self._fail("target path outside task ticket scope", t0)

        # 4) Admin endorsement Ed25519
        admin_payload = AdminEndorsement.build_payload(
            req.endorsement.app_code_hash,
            req.endorsement.allowed_root_directories,
            req.endorsement.timestamp,
        )
        if not self._admin_vk.verify(admin_payload, req.endorsement.admin_signature):
            return self._fail("admin endorsement verification failed", t0)

        # 5) Driver TaskTicket Ed25519
        ticket_payload = TaskTicket.build_payload(
            req.ticket.job_id,
            req.ticket.task_id,
            req.ticket.worker_ip,
            req.ticket.allowed_target_path,
            req.ticket.expires_at,
        )
        if not self._driver_vk.verify(ticket_payload, req.ticket.driver_signature):
            return self._fail("driver task ticket verification failed", t0)

        # 6) Executor signature (sig_exe) against enrolled Worker trust list
        worker_key = self._trusted_workers.get(req.worker_id)
        if worker_key is None:
            return self._fail("executor not in trust list", t0)
        exe_payload = self._sig_exe_payload(req)
        if not worker_key.verify(exe_payload, req.sig_exe):
            return self._fail("invalid executor signature (sig_exe)", t0)

        # 7) CP-ABE attribute gate (Scenario C illegal SK_u)
        if req.policy and not evaluate_policy(req.policy, set(req.user_attributes)):
            return self._fail("CP-ABE policy unsatisfied (SK_u)", t0)

        latency = time.perf_counter_ns() - t0
        self._record(True, "")
        return VerifyResult(ok=True, latency_ns=latency)

    def _fail(self, reason: str, t0: int) -> VerifyResult:
        latency = time.perf_counter_ns() - t0
        self._record(False, reason)
        return VerifyResult(ok=False, reason=reason, latency_ns=latency)

    @staticmethod
    def _sig_exe_payload(req: WriteRequest) -> bytes:
        return (
            f"{req.ticket.job_id}|{req.ticket.task_id}|{req.worker_id}|"
            f"{req.target_path}|{req.payload_bytes}|{req.now_epoch_sec}"
        ).encode("utf-8")


# ---------------------------------------------------------------------------
# Centralized ACL baseline (remote NameNode / ACL service)
# ---------------------------------------------------------------------------

class CentralizedAclBaseline:
    """
    Baseline: every write hits a remote ACL service (network RTT + ACL lookup).
    Models Hadoop/ACL-style centralized authorization without TEE binding.

    Throughput is capped by a semaphore of in-flight remote calls (ACL server
    fan-in limit), so capacity ≈ max_inflight / (rtt + lookup).
    """

    def __init__(
        self,
        allowed_roots: Sequence[str],
        network_rtt_us: float = 800.0,
        lookup_us: float = 50.0,
        max_inflight: int = 16,
        name: str = "CentralizedACL",
    ):
        self.name = name
        self._allowed_roots = tuple(allowed_roots)
        self._network_rtt_us = network_rtt_us
        self._lookup_us = lookup_us
        self._inflight = threading.Semaphore(max(1, max_inflight))
        self._lock = threading.Lock()
        self.stats = {
            "total": 0,
            "accepted": 0,
            "rejected": 0,
            "reject_by_reason": {},
        }
        self._acl: Dict[str, Set[str]] = {}

    def set_acl(self, path_prefix: str, worker_ids: Set[str]) -> None:
        self._acl[normalize_path(path_prefix)] = set(worker_ids)

    def verify_write(self, req: WriteRequest) -> VerifyResult:
        t0 = time.perf_counter_ns()
        sleep_s = (self._network_rtt_us + self._lookup_us) * 1e-6
        self._inflight.acquire()
        try:
            if sleep_s > 0:
                time.sleep(sleep_s)

            if req.now_epoch_sec > req.ticket.expires_at:
                return self._fail("ACL: ticket expired", t0)

            if not is_under_any_root(req.target_path, self._allowed_roots):
                return self._fail("ACL: path denied", t0)

            allowed = False
            target = normalize_path(req.target_path)
            for prefix, workers in self._acl.items():
                if (target == prefix or is_under_root(target, prefix)) and req.worker_id in workers:
                    allowed = True
                    break
            if not allowed:
                return self._fail("ACL: worker unauthorized", t0)

            if req.policy and not evaluate_policy(req.policy, set(req.user_attributes)):
                return self._fail("ACL: attribute policy denied", t0)

            latency = time.perf_counter_ns() - t0
            with self._lock:
                self.stats["total"] += 1
                self.stats["accepted"] += 1
            return VerifyResult(ok=True, latency_ns=latency)
        finally:
            self._inflight.release()

    def _fail(self, reason: str, t0: int) -> VerifyResult:
        latency = time.perf_counter_ns() - t0
        with self._lock:
            self.stats["total"] += 1
            self.stats["rejected"] += 1
            self.stats["reject_by_reason"][reason] = (
                self.stats["reject_by_reason"].get(reason, 0) + 1
            )
        return VerifyResult(ok=False, reason=reason, latency_ns=latency)


# ---------------------------------------------------------------------------
# Honest issuer / request factory
# ---------------------------------------------------------------------------

@dataclass
class TrustMaterial:
    admin: EcdsaKeyPair
    driver: EcdsaKeyPair
    eac: EcdsaKeyPair
    workers: Dict[str, EcdsaKeyPair] = field(default_factory=dict)
    endorsement: Optional[AdminEndorsement] = None
    allowed_root: str = "hdfs://cluster/data/jobs"
    policy_sensitive: str = "(role:admin and clearance:5)"
    policy_public: str = "role:analyst"


def bootstrap_trust(
    n_workers: int = 8,
    allowed_root: str = "hdfs://cluster/data/jobs",
    now: Optional[int] = None,
) -> TrustMaterial:
    now = now or int(time.time())
    tm = TrustMaterial(
        admin=EcdsaKeyPair(),
        driver=EcdsaKeyPair(),
        eac=EcdsaKeyPair(),
        allowed_root=allowed_root,
    )
    for i in range(n_workers):
        wid = f"worker-{i}"
        tm.workers[wid] = EcdsaKeyPair()

    app_hash = hashlib.sha256(b"aegis-spark-job-jar").hexdigest()
    payload = AdminEndorsement.build_payload(app_hash, [allowed_root], now)
    tm.endorsement = AdminEndorsement(
        app_code_hash=app_hash,
        allowed_root_directories=(allowed_root,),
        timestamp=now,
        admin_signature=tm.admin.sign(payload),
    )
    return tm


def issue_ticket(
    tm: TrustMaterial,
    job_id: str,
    task_id: str,
    worker_ip: str,
    allowed_target_path: str,
    expires_at: int,
) -> TaskTicket:
    payload = TaskTicket.build_payload(
        job_id, task_id, worker_ip, allowed_target_path, expires_at
    )
    return TaskTicket(
        job_id=job_id,
        task_id=task_id,
        worker_ip=worker_ip,
        allowed_target_path=allowed_target_path,
        expires_at=expires_at,
        driver_signature=tm.driver.sign(payload),
    )


def make_benign_request(
    tm: TrustMaterial,
    worker_id: str,
    task_seq: int,
    now: int,
    payload_bytes: int = 4096,
    ttl_sec: int = 3600,
    use_sensitive_policy: bool = False,
) -> WriteRequest:
    target = f"{tm.allowed_root}/job-001/part-{task_seq:05d}"
    ticket = issue_ticket(
        tm,
        job_id="job-001",
        task_id=f"task-{task_seq}",
        worker_ip=f"10.0.0.{(task_seq % 250) + 1}",
        allowed_target_path=target,
        expires_at=now + ttl_sec,
    )
    policy = tm.policy_sensitive if use_sensitive_policy else tm.policy_public
    attrs = (
        frozenset({"role:admin", "clearance:5"})
        if use_sensitive_policy
        else frozenset({"role:analyst", "dept:finance"})
    )
    req = WriteRequest(
        ticket=ticket,
        endorsement=tm.endorsement,
        target_path=target,
        payload_bytes=payload_bytes,
        worker_id=worker_id,
        sig_exe=b"",
        user_attributes=attrs,
        policy=policy,
        now_epoch_sec=now,
    )
    sig = tm.workers[worker_id].sign(AegisSparkDefense._sig_exe_payload(req))
    return WriteRequest(
        ticket=req.ticket,
        endorsement=req.endorsement,
        target_path=req.target_path,
        payload_bytes=req.payload_bytes,
        worker_id=req.worker_id,
        sig_exe=sig,
        user_attributes=req.user_attributes,
        policy=req.policy,
        now_epoch_sec=req.now_epoch_sec,
    )


def resign_sig_exe(tm: TrustMaterial, req: WriteRequest, worker_id: Optional[str] = None) -> WriteRequest:
    wid = worker_id or req.worker_id
    key = tm.workers[wid]
    updated = WriteRequest(
        ticket=req.ticket,
        endorsement=req.endorsement,
        target_path=req.target_path,
        payload_bytes=req.payload_bytes,
        worker_id=wid,
        sig_exe=b"",
        user_attributes=req.user_attributes,
        policy=req.policy,
        now_epoch_sec=req.now_epoch_sec,
    )
    sig = key.sign(AegisSparkDefense._sig_exe_payload(updated))
    return WriteRequest(
        ticket=updated.ticket,
        endorsement=updated.endorsement,
        target_path=updated.target_path,
        payload_bytes=updated.payload_bytes,
        worker_id=updated.worker_id,
        sig_exe=sig,
        user_attributes=updated.user_attributes,
        policy=updated.policy,
        now_epoch_sec=updated.now_epoch_sec,
    )
