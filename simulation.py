"""
simulation.py
─────────────────────────────────────────────────────────
Simulates the full etcd Raft-based leader election and
distributed-lock workflow without requiring a real etcd
cluster or PostgreSQL instance.

State is held purely in memory so the Flask dev-server
can drive it through SSE events.
"""

import threading
import time
import random
import queue
from dataclasses import dataclass, field
from typing import Optional, List, Dict
from enum import Enum

# ─────────────────────────────────────────
# Enums / constants
# ─────────────────────────────────────────

class NodeRole(str, Enum):
    FOLLOWER  = "follower"
    CANDIDATE = "candidate"
    LEADER    = "leader"
    CRASHED   = "crashed"

class AppClientRole(str, Enum):
    IDLE        = "idle"
    CONTESTING  = "contesting"
    LEADER      = "leader"
    FOLLOWER    = "follower"

LOCK_KEY    = "/db/critical_lock"
LEASE_TTL   = 10          # seconds for demo
TICK        = 0.5         # simulation tick

# ─────────────────────────────────────────
# Data structures
# ─────────────────────────────────────────

@dataclass
class EtcdNode:
    node_id:    str
    system:     str           # "System1" | "System2"
    port:       int
    role:       NodeRole      = NodeRole.FOLLOWER
    term:       int           = 0
    voted_for:  Optional[str] = None
    alive:      bool          = True
    vote_count: int           = 0

@dataclass
class AppClient:
    client_id: str
    system:    str
    role:      AppClientRole  = AppClientRole.IDLE
    has_lock:  bool           = False

@dataclass
class DistributedLock:
    held_by:      Optional[str] = None   # client_id
    lease_expiry: float         = 0.0
    revision:     int           = 0

@dataclass
class PostgresDB:
    host:      str = "System1"
    port:      int = 5432
    records:   List[Dict] = field(default_factory=list)
    write_log: List[str]  = field(default_factory=list)

# ─────────────────────────────────────────
# Global simulation state
# ─────────────────────────────────────────

class SimulationState:
    def __init__(self):
        self.etcd_nodes: Dict[str, EtcdNode] = {
            "etcd-1": EtcdNode("etcd-1", "System1", 2379),
            "etcd-2": EtcdNode("etcd-2", "System1", 2380),
            "etcd-3": EtcdNode("etcd-3", "System2", 2381),
        }
        self.clients: Dict[str, AppClient] = {
            "client-1": AppClient("client-1", "System1"),
            "client-2": AppClient("client-2", "System2"),
        }
        self.lock    = DistributedLock()
        self.db      = PostgresDB()
        self.phase   = "idle"        # idle | init | election | lock | db_write | failure | recovery
        self.step    = 0
        self.logs: List[str] = []
        self.event_queue: queue.Queue = queue.Queue()
        self._lock   = threading.Lock()
        self.running = False
        self._thread: Optional[threading.Thread] = None

    # ── helpers ──────────────────────────

    def _log(self, msg: str):
        ts = time.strftime("%H:%M:%S")
        entry = f"[{ts}] {msg}"
        self.logs.append(entry)
        self.event_queue.put({"type": "log", "message": entry})

    def _emit(self, event_type: str, data: dict):
        data["type"] = event_type
        self.event_queue.put(data)

    def snapshot(self) -> dict:
        """Return a JSON-serialisable snapshot of current state."""
        with self._lock:
            nodes = {
                nid: {
                    "node_id": n.node_id,
                    "system":  n.system,
                    "port":    n.port,
                    "role":    n.role.value,
                    "term":    n.term,
                    "alive":   n.alive,
                }
                for nid, n in self.etcd_nodes.items()
            }
            clients = {
                cid: {
                    "client_id": c.client_id,
                    "system":    c.system,
                    "role":      c.role.value,
                    "has_lock":  c.has_lock,
                }
                for cid, c in self.clients.items()
            }
            lock_data = {
                "held_by":      self.lock.held_by,
                "ttl_remaining": max(0, round(self.lock.lease_expiry - time.time(), 1))
                                  if self.lock.held_by else 0,
                "revision":     self.lock.revision,
            }
            db_data = {
                "host":      self.db.host,
                "port":      self.db.port,
                "records":   self.db.records[-10:],  # match write_log window
                "write_log": self.db.write_log[-10:],
            }
            return {
                "phase":   self.phase,
                "step":    self.step,
                "nodes":   nodes,
                "clients": clients,
                "lock":    lock_data,
                "db":      db_data,
                "logs":    self.logs[-30:],
            }

    # ── Raft election simulation ──────────

    def _raft_elect_leader(self):
        """Simulate a Raft leader election among alive nodes."""
        alive = [n for n in self.etcd_nodes.values() if n.alive]
        if len(alive) < 2:
            return None

        # Increment term
        new_term = max(n.term for n in alive) + 1
        for n in alive:
            n.term       = new_term
            n.role       = NodeRole.FOLLOWER
            n.voted_for  = None
            n.vote_count = 0

        self._log(f"Raft: New election started – Term {new_term}")
        self._emit("raft_election", {"term": new_term})
        time.sleep(0.6)

        # Each alive node votes for a random candidate (simplified)
        candidate = random.choice(alive)
        candidate.role = NodeRole.CANDIDATE
        self._log(f"Raft: {candidate.node_id} becomes CANDIDATE (Term {new_term})")
        time.sleep(0.4)

        for voter in alive:
            if voter.node_id != candidate.node_id and voter.alive:
                voter.voted_for = candidate.node_id
                candidate.vote_count += 1
                self._log(f"Raft: {voter.node_id} votes for {candidate.node_id}")
                time.sleep(0.25)

        quorum = len(alive) // 2 + 1
        if candidate.vote_count + 1 >= quorum:
            candidate.role = NodeRole.LEADER
            for n in alive:
                if n.node_id != candidate.node_id:
                    n.role = NodeRole.FOLLOWER
            self._log(f"Raft: {candidate.node_id} wins election → LEADER (Term {new_term}, votes {candidate.vote_count+1}/{len(alive)})")
            self._emit("raft_leader_elected", {"leader": candidate.node_id, "term": new_term})
            return candidate.node_id
        return None

    # ── Distributed lock ──────────────────

    def _acquire_lock(self, client_id: str) -> bool:
        """Try to acquire the etcd distributed lock (CAS semantics)."""
        with self._lock:
            now = time.time()
            if self.lock.held_by is None or now > self.lock.lease_expiry:
                self.lock.held_by      = client_id
                self.lock.lease_expiry = now + LEASE_TTL
                self.lock.revision    += 1
                return True
        return False

    def _release_lock(self, client_id: str):
        with self._lock:
            if self.lock.held_by == client_id:
                self.lock.held_by      = None
                self.lock.lease_expiry = 0.0

    # ── DB write ─────────────────────────

    def _db_write(self, client_id: str, payload: str):
        # Payload format: "operation_name   SQL text -- comment"
        # Split on first whitespace cluster to get op_name vs full SQL
        parts    = payload.split(None, 1)
        op_name  = parts[0] if parts else "write"
        sql_body = parts[1].strip() if len(parts) > 1 else payload
        entry = {
            "id":        len(self.db.records) + 1,
            "written_by": client_id,
            "payload":   sql_body,
            "operation": op_name,
            "timestamp": time.strftime("%H:%M:%S"),
        }
        self.db.records.append(entry)
        # Log line format matches live-mode: "[ts] client → op_name"
        log_line = f"[{entry['timestamp']}] {client_id} → {op_name}"
        self.db.write_log.append(log_line)
        self._log(f"PostgreSQL: {sql_body}")
        self._emit("db_write", {**entry, "log_line": log_line})

    # ── Main workflow ─────────────────────

    def run_workflow(self):
        """Execute the four-step workflow from the case study."""
        self.running = True
        self.step    = 0
        self.logs.clear()

        try:
            # ── STEP 1 – Cluster Initialisation ─────────────────
            self.phase = "init"
            self.step  = 1
            self._emit("phase_change", {"phase": self.phase, "step": self.step,
                                         "title": "Step 1 – Cluster Initialisation"})
            self._log("=== STEP 1: etcd Cluster Initialisation ===")
            self._log("Starting etcd node etcd-1 on System1:2379 …")
            time.sleep(0.7)
            self._log("Starting etcd node etcd-2 on System1:2380 …")
            time.sleep(0.7)
            self._log("Starting etcd node etcd-3 on System2:2381 …")
            time.sleep(0.7)
            self._log("All 3 etcd members joined cluster – quorum achieved (2/3)")
            for n in self.etcd_nodes.values():
                n.alive = True
                n.role  = NodeRole.FOLLOWER
            self._emit("state_update", self.snapshot())
            time.sleep(1.0)

            # ── Raft leader election (internal) ──────────────────
            self.phase = "election"
            self.step  = 1
            self._emit("phase_change", {"phase": self.phase, "step": self.step,
                                         "title": "Step 1 – Raft Internal Leader Election"})
            self._log("=== Raft consensus leader election begins ===")
            raft_leader = self._raft_elect_leader()
            self._emit("state_update", self.snapshot())
            time.sleep(0.8)

            # ── STEP 2 – Application Leader Election ─────────────
            self.phase = "lock"
            self.step  = 2
            self._emit("phase_change", {"phase": self.phase, "step": self.step,
                                         "title": "Step 2 – Application Leader Election (Distributed Lock)"})
            self._log("=== STEP 2: Application Leader Election ===")

            for c in self.clients.values():
                c.role = AppClientRole.CONTESTING
            self._emit("state_update", self.snapshot())

            self._log("client-1 (System1) issues etcd PUT /db/critical_lock with lease TTL=10s …")
            time.sleep(0.5)
            self._log("client-2 (System2) issues etcd PUT /db/critical_lock with lease TTL=10s …")
            time.sleep(0.5)

            # Race: client-1 wins (deterministic for demo)
            winner, loser = "client-1", "client-2"
            acquired = self._acquire_lock(winner)
            if acquired:
                self.clients[winner].has_lock = True
                self.clients[winner].role     = AppClientRole.LEADER
                self.clients[loser].role      = AppClientRole.FOLLOWER
                self._log(f"etcd CAS: {winner} successfully acquired lock (revision {self.lock.revision}, TTL {LEASE_TTL}s)")
                self._log(f"etcd CAS: {loser} lock acquisition FAILED – key already exists")
                self._log(f"{winner} → APPLICATION LEADER")
                self._log(f"{loser}  → APPLICATION FOLLOWER (passive)")
                self._emit("lock_acquired", {"winner": winner, "loser": loser,
                                              "lease_ttl": LEASE_TTL})
            self._emit("state_update", self.snapshot())
            time.sleep(1.0)

            # ── STEP 3 – Critical DB Operation ───────────────────
            self.phase = "db_write"
            self.step  = 3
            self._emit("phase_change", {"phase": self.phase, "step": self.step,
                                         "title": "Step 3 – Critical Database Operation"})
            self._log("=== STEP 3: Critical Database Operation ===")
            self._log(f"{winner} holds lock → connecting to PostgreSQL on {self.db.host}:{self.db.port}")
            time.sleep(0.6)

            # ── Round 1: New customer order ────────────────────────────────────
            self._log(f"{winner} → BEGIN TRANSACTION (order #ORD-7821)")
            batch1 = [
                "order_insert      INSERT INTO orders(id,sku,qty,unit_price) VALUES(7821,'SKU-409',12,89.99)  -- total $1,079.88",
                "inventory_decrement  UPDATE inventory SET stock=stock-12, reserved=reserved+12 WHERE sku='SKU-409'  -- stock 847→835",
                "ledger_debit       UPDATE accounts SET balance=balance-1079.88 WHERE acc_id='ACC-1042'  -- bal $8,320.12→$7,240.24",
                "shipment_create    INSERT INTO shipments(order_id,carrier,weight_kg,eta) VALUES(7821,'FedEx',3.2,'2026-03-05')",
                "audit_log          INSERT INTO audit_log(event,order_id,amount,ts) VALUES('order_confirmed',7821,1079.88,NOW())",
            ]
            for sql in batch1:
                self._db_write(winner, sql)
                time.sleep(0.45)
            self._log(f"{winner} → COMMIT  (order #ORD-7821 persisted, 5 rows affected)")
            time.sleep(0.5)

            # ── Round 2: Stock replenishment ───────────────────────────────────
            self._log(f"{winner} → BEGIN TRANSACTION (stock replenish batch)")
            batch2 = [
                "inventory_replenish  UPDATE inventory SET stock=stock+200, reorder_flag=false WHERE sku='SKU-409'  -- stock 835→1035",
                "inventory_replenish  UPDATE inventory SET stock=stock+150 WHERE sku='SKU-112'  -- stock 43→193",
                "inventory_replenish  UPDATE inventory SET stock=stock+500 WHERE sku='SKU-774'  -- stock 12→512",
                "po_close             UPDATE purchase_orders SET status='received', received_at=NOW() WHERE po_id=4419",
                "warehouse_log        INSERT INTO warehouse_log(zone,action,units,user) VALUES('B7','inbound',850,'system')",
            ]
            for sql in batch2:
                self._db_write(winner, sql)
                time.sleep(0.45)
            self._log(f"{winner} → COMMIT  (replenish batch, 3 SKUs updated)")
            time.sleep(0.5)

            # ── Round 3: Payment settlement ────────────────────────────────────
            self._log(f"{winner} → BEGIN TRANSACTION (payment settlement TX-3301)")
            batch3 = [
                "payment_receive    INSERT INTO payments(tx_id,from_acc,amount,method) VALUES(3301,'ACC-2289',4575.00,'wire')",
                "ledger_credit      UPDATE accounts SET balance=balance+4575.00 WHERE acc_id='ACC-2289'  -- bal $1,200.00→$5,775.00",
                "receivables_clear  UPDATE receivables SET status='paid',paid_at=NOW() WHERE inv_id=8843  -- inv $4,575.00",
                "tax_provision      INSERT INTO tax_ledger(period,amount,type) VALUES('2026-Q1',686.25,'VAT_15pct')",
                "reconcile_log      INSERT INTO reconcile_log(batch,matched,unmatched,ts) VALUES('B-031',142,0,NOW())",
            ]
            for sql in batch3:
                self._db_write(winner, sql)
                time.sleep(0.45)
            self._log(f"{winner} → COMMIT  (TX-3301 settled, receivables cleared)")
            time.sleep(0.5)

            self._log(f"{loser} is PASSIVE – no DB writes performed (lock not held)")
            self._emit("state_update", self.snapshot())
            time.sleep(1.0)

            # ── STEP 4 – Failure Scenario ─────────────────────────
            self.phase = "failure"
            self.step  = 4
            self._emit("phase_change", {"phase": self.phase, "step": self.step,
                                         "title": "Step 4 – Failure Scenario"})
            self._log("=== STEP 4: Failure Scenario ===")
            self._log(f"⚠  Simulating crash of {winner} (System1) …")
            time.sleep(0.6)

            # Crash the leader client
            self.clients[winner].has_lock = False
            self.clients[winner].role     = AppClientRole.IDLE
            # Crash etcd-1 and etcd-2 (both on System1)
            self.etcd_nodes["etcd-1"].alive = False
            self.etcd_nodes["etcd-1"].role  = NodeRole.CRASHED
            self.etcd_nodes["etcd-2"].alive = False
            self.etcd_nodes["etcd-2"].role  = NodeRole.CRASHED
            self._emit("node_crash", {"crashed": ["etcd-1", "etcd-2"], "system": "System1"})
            self._log("etcd-1 and etcd-2 (System1) CRASHED — lease heartbeats stopped")
            time.sleep(0.5)

            self._log(f"etcd: lease TTL expired → lock key '{LOCK_KEY}' auto-deleted")
            self._release_lock(winner)  # simulate TTL expiry
            self._emit("state_update", self.snapshot())
            time.sleep(0.8)

            # ── STEP 4b – Recovery / New Election ─────────────────
            self.phase = "recovery"
            self._emit("phase_change", {"phase": self.phase, "step": self.step,
                                         "title": "Step 4 – Recovery & New Leader Election"})
            self._log("=== Recovery: New etcd election with surviving node ===")
            # etcd-3 is the only alive node — in real Raft it needs quorum; here we
            # show re-join scenario (etcd-1 restarts)
            time.sleep(0.5)
            self._log("etcd-1 restarting (node recovery) …")
            time.sleep(0.8)
            self.etcd_nodes["etcd-1"].alive = True
            self.etcd_nodes["etcd-1"].role  = NodeRole.FOLLOWER
            self._log("etcd-1 rejoined cluster – quorum restored (2/3)")
            time.sleep(0.4)

            new_raft_leader = self._raft_elect_leader()
            self._emit("state_update", self.snapshot())
            time.sleep(0.5)

            self._log(f"{loser} (System2) detects lock key absent → attempting acquisition …")
            time.sleep(0.5)
            acquired2 = self._acquire_lock(loser)
            if acquired2:
                self.clients[loser].has_lock = True
                self.clients[loser].role     = AppClientRole.LEADER
                self._log(f"{loser} acquired lock (revision {self.lock.revision}) → NEW APPLICATION LEADER")
                self._emit("lock_acquired", {"winner": loser, "loser": winner,
                                              "lease_ttl": LEASE_TTL})
                self._log(f"{loser} resumes critical DB writes after failover …")
                time.sleep(0.4)
                recovery_writes = [
                    "failover_log       INSERT INTO recovery_log(event,prev_leader,new_leader,ts) VALUES('leader_failover','client-1','client-2',NOW())",
                    "system_state       UPDATE system_state SET active_leader='client-2', failover_count=failover_count+1, last_failover=NOW()",
                    "order_resume       INSERT INTO orders(id,sku,qty,unit_price) VALUES(7822,'SKU-112',8,149.50)  -- order resumed by client-2",
                    "inventory_decrement  UPDATE inventory SET stock=stock-8 WHERE sku='SKU-112'  -- stock 193→185",
                    "audit_log          INSERT INTO audit_log(event,order_id,amount,ts) VALUES('order_confirmed',7822,1196.00,NOW())",
                ]
                for sql in recovery_writes:
                    self._db_write(loser, sql)
                    time.sleep(0.4)
            self._emit("state_update", self.snapshot())
            time.sleep(0.5)

            self._log("✓ System is consistent and available – failover complete.")
            self._emit("workflow_complete", {"message": "Workflow completed successfully."})

        except Exception as exc:
            self._log(f"[ERROR] {exc}")
        finally:
            self.running = False
            self.phase   = "done"

    def start(self):
        if self.running:
            return
        # Reset state
        self.__init__()
        self._thread = threading.Thread(target=self.run_workflow, daemon=True)
        self._thread.start()

    def reset(self):
        self.running = False
        self.__init__()
        self._emit("reset", {})


# Singleton
sim = SimulationState()
