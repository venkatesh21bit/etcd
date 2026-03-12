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

ALGORITHM_RAFT  = "raft"
ALGORITHM_BULLY = "bully"

# ── Use-case definitions ──────────────────────────────────────────────────────
USE_CASES = {
    "ecommerce_order": {
        "id": "ecommerce_order", "name": "E-Commerce Order", "icon": "\U0001f6d2",
        "description": "Place a customer order, deduct inventory, charge payment, and create shipment.",
        "operations": [
            "order_insert      INSERT INTO orders(id,cust_id,sku,qty,unit_price) VALUES(8001,'C-419','SKU-309',3,249.99)  -- total $749.97",
            "inv_decrement     UPDATE inventory SET stock=stock-3, reserved=reserved+3 WHERE sku='SKU-309'  -- stock 312\u2192309",
            "payment_charge    INSERT INTO payments(tx_id,order_id,amount,method) VALUES(5501,8001,749.97,'CARD-****4521')  -- Visa",
            "shipment_create   INSERT INTO shipments(order_id,carrier,weight_kg,eta) VALUES(8001,'UPS',1.4,'2026-03-14')  -- 2-day delivery",
            "audit_log         INSERT INTO audit_log(event,entity_id,amount,ts) VALUES('order_placed',8001,749.97,NOW())",
        ],
        "commit_msg": "Order #8001 placed \u2013 5 rows committed",
    },
    "bank_transfer": {
        "id": "bank_transfer", "name": "Bank Transfer", "icon": "\U0001f4b3",
        "description": "Atomically debit source account, credit destination, and record the wire transfer.",
        "operations": [
            "balance_check     SELECT balance FROM accounts WHERE acc_id='ACC-1042'  -- balance $12,400.00 \u2713",
            "debit_source      UPDATE accounts SET balance=balance-5000.00, tx_count=tx_count+1 WHERE acc_id='ACC-1042'  -- $12,400\u2192$7,400",
            "credit_dest       UPDATE accounts SET balance=balance+5000.00 WHERE acc_id='ACC-7731'  -- $800\u2192$5,800",
            "tx_record         INSERT INTO transactions(tx_id,from_acc,to_acc,amount,method) VALUES(9901,'ACC-1042','ACC-7731',5000.00,'wire')",
            "ledger_entry      INSERT INTO ledger(tx_id,dr_acc,cr_acc,amount,period) VALUES(9901,'ACC-1042','ACC-7731',5000.00,'2026-Q1')",
        ],
        "commit_msg": "Wire TX-9901 committed \u2013 $5,000 transferred",
    },
    "inventory_restock": {
        "id": "inventory_restock", "name": "Inventory Restock", "icon": "\U0001f4e6",
        "description": "Process a supplier delivery: update stock levels, close PO, log warehouse activity.",
        "operations": [
            "po_verify         SELECT * FROM purchase_orders WHERE po_id=4419 AND status='pending'  -- PO found \u2713",
            "stock_update      UPDATE inventory SET stock=stock+500, reorder_flag=false WHERE sku='SKU-774'  -- 12\u2192512",
            "stock_update      UPDATE inventory SET stock=stock+300 WHERE sku='SKU-112'  -- 43\u2192343",
            "po_close          UPDATE purchase_orders SET status='received', received_at=NOW() WHERE po_id=4419",
            "warehouse_log     INSERT INTO warehouse_log(zone,action,units,supplier) VALUES('C4','inbound',800,'SUP-Acme')",
        ],
        "commit_msg": "PO-4419 received \u2013 800 units restocked across 2 SKUs",
    },
    "user_onboarding": {
        "id": "user_onboarding", "name": "User Onboarding", "icon": "\U0001f464",
        "description": "Register new user, assign roles, provision storage quota, and log access creation.",
        "operations": [
            "user_create       INSERT INTO users(id,email,name,status) VALUES('U-9921','alice@corp.io','Alice Sharma','active')",
            "role_assign       INSERT INTO user_roles(user_id,role,granted_by) VALUES('U-9921','engineer','admin')  -- default role",
            "quota_provision   INSERT INTO quotas(user_id,storage_gb,api_rpm) VALUES('U-9921',100,1000)  -- standard tier",
            "notify_queue      INSERT INTO notification_queue(user_id,template,status) VALUES('U-9921','welcome_email','queued')",
            "audit_log         INSERT INTO audit_log(event,entity_id,actor,ts) VALUES('user_created','U-9921','admin',NOW())",
        ],
        "commit_msg": "User U-9921 onboarded \u2013 roles, quota, and notifications provisioned",
    },
    "analytics_snapshot": {
        "id": "analytics_snapshot", "name": "Analytics Snapshot", "icon": "\U0001f4ca",
        "description": "Aggregate daily sales, compute KPI metrics, and persist a dashboard snapshot.",
        "operations": [
            "sales_aggregate   INSERT INTO daily_sales(date,sku,units_sold,revenue) SELECT NOW()::date,sku,SUM(qty),SUM(qty*unit_price) FROM orders GROUP BY sku",
            "kpi_compute       INSERT INTO kpi_metrics(date,metric,value) VALUES(NOW()::date,'daily_revenue',24381.50)  -- +12% DoD",
            "kpi_compute       INSERT INTO kpi_metrics(date,metric,value) VALUES(NOW()::date,'orders_placed',87)  -- vs 79 yesterday",
            "snapshot_store    INSERT INTO dashboard_snapshots(ts,rev,orders,gmv) VALUES(NOW(),14,87,24381.50)",
            "event_publish     INSERT INTO outbox(topic,event_date,gmv,status) VALUES('analytics.daily','2026-03-12',24381.50,'pending')",
        ],
        "commit_msg": "Daily analytics snapshot committed \u2013 KPIs and outbox event persisted",
    },
}

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
            "client-3": AppClient("client-3", "System2"),
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
        self.selected_algorithm: str = ALGORITHM_RAFT
        self.selected_usecase:   str = "ecommerce_order"

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
                "phase":     self.phase,
                "step":      self.step,
                "running":   self.running,
                "algorithm": self.selected_algorithm,
                "usecase":   self.selected_usecase,
                "nodes":     nodes,
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

    # ── Raft log replication ──────────────

    def _raft_replicate_logs(self, leader_id: str):
        """Simulate Raft AppendEntries log replication to all followers."""
        alive     = [n for n in self.etcd_nodes.values() if n.alive]
        followers = [n for n in alive if n.node_id != leader_id]
        node_ids  = [n.node_id for n in alive]

        entries = [
            f"/cluster/leader-key → {leader_id}",
            f"/config/lease-ttl → {LEASE_TTL}",
            "/db/critical_lock → <vacant>",
        ]
        self._log("Raft Replication: leader will send AppendEntries RPCs to all followers")
        for idx, entry in enumerate(entries, start=1):
            self._log(f"Raft Replication: {leader_id} appends log entry #{idx}: {entry}")
            self._emit("raft_replicate", {
                "leader":    leader_id,
                "entry":     entry,
                "index":     idx,
                "phase":     "append",
                "acks":      [],
                "committed": False,
                "nodes":     node_ids,
            })
            time.sleep(0.4)

            acks = []
            for follower in followers:
                self._log(f"Raft Replication: AppendEntries RPC → {follower.node_id} … ACK ✓")
                acks.append(follower.node_id)
                self._emit("raft_replicate", {
                    "leader":    leader_id,
                    "entry":     entry,
                    "index":     idx,
                    "phase":     "ack",
                    "acks":      list(acks),
                    "committed": False,
                    "nodes":     node_ids,
                })
                time.sleep(0.3)

            quorum = len(alive) // 2 + 1
            if len(acks) + 1 >= quorum:
                self._log(f"Raft Replication: Entry #{idx} COMMITTED – quorum {len(acks)+1}/{len(alive)} reached ✓")
                self._emit("raft_replicate", {
                    "leader":    leader_id,
                    "entry":     entry,
                    "index":     idx,
                    "phase":     "committed",
                    "acks":      list(acks),
                    "committed": True,
                    "nodes":     node_ids,
                })
            time.sleep(0.35)

    # ── Raft heartbeats ───────────────────

    def _raft_heartbeats(self, leader_id: str, rounds: int = 3):
        """Simulate periodic Raft heartbeat (empty AppendEntries) broadcasts."""
        alive  = [n for n in self.etcd_nodes.values() if n.alive]
        term   = max(n.term for n in alive) if alive else 0
        for r in range(1, rounds + 1):
            self._log(f"Raft Heartbeat #{r}: {leader_id} → AppendEntries(empty) → followers (Term {term})")
            self._emit("raft_heartbeat", {
                "leader": leader_id,
                "term":   term,
                "round":  r,
                "total":  rounds,
                "nodes":  [n.node_id for n in alive],
            })
            time.sleep(0.7)

    # ── Bully Algorithm election ──────────────

    def _bully_elect_leader(self):
        """Simulate the Bully Algorithm leader election."""
        alive = [n for n in self.etcd_nodes.values() if n.alive]
        if len(alive) < 1:
            return None

        def priority(node):
            return int(node.node_id.split("-")[1])

        alive_sorted = sorted(alive, key=priority)
        new_term = max(n.term for n in alive) + 1
        for n in alive:
            n.term       = new_term
            n.role       = NodeRole.FOLLOWER
            n.voted_for  = None
            n.vote_count = 0

        prio_str = ", ".join(f"{n.node_id}=P{priority(n)}" for n in alive_sorted)
        self._log(f"Bully: Election started \u2013 node priorities: [{prio_str}]")
        self._emit("bully_election", {
            "term":  new_term,
            "nodes": [{"id": n.node_id, "priority": priority(n)} for n in alive_sorted],
        })
        time.sleep(0.5)

        # Each node from lowest to highest sends ELECTION up, receives OK back
        for i, node in enumerate(alive_sorted[:-1]):
            node.role = NodeRole.CANDIDATE
            higher    = alive_sorted[i + 1:]
            self._log(f"Bully: {node.node_id} (P{priority(node)}) \u2192 ELECTION \u2192 {', '.join(h.node_id for h in higher)}")
            for h in higher:
                self._emit("bully_message", {
                    "from": node.node_id, "to": h.node_id,
                    "msg_type": "ELECTION", "term": new_term,
                })
                time.sleep(0.38)
            for h in higher:
                self._log(f"Bully: {h.node_id} (P{priority(h)}) \u2192 OK \u2192 {node.node_id}")
                self._emit("bully_message", {
                    "from": h.node_id, "to": node.node_id,
                    "msg_type": "OK", "term": new_term,
                })
                time.sleep(0.3)
            node.role = NodeRole.FOLLOWER
            self._log(f"Bully: {node.node_id} steps back \u2013 received OK from higher-priority node")
            time.sleep(0.25)

        # Highest-priority alive node becomes coordinator
        coordinator = alive_sorted[-1]
        coordinator.role = NodeRole.LEADER
        for n in alive:
            if n.node_id != coordinator.node_id:
                n.role = NodeRole.FOLLOWER
        self._log(f"Bully: {coordinator.node_id} (P{priority(coordinator)}) \u2192 no higher node \u2192 broadcasts COORDINATOR")
        for n in [x for x in alive_sorted if x.node_id != coordinator.node_id]:
            self._emit("bully_message", {
                "from": coordinator.node_id, "to": n.node_id,
                "msg_type": "COORDINATOR", "term": new_term,
            })
            self._log(f"Bully: {coordinator.node_id} \u2192 COORDINATOR \u2192 {n.node_id}")
            time.sleep(0.35)
        self._log(f"Bully: {coordinator.node_id} is COORDINATOR (P{priority(coordinator)}, Term {new_term})")
        self._emit("bully_leader_elected", {
            "leader":   coordinator.node_id,
            "term":     new_term,
            "priority": priority(coordinator),
        })
        return coordinator.node_id

    # ── Use-case runner ──────────────────

    def _run_usecase(self, client_id: str, uc: dict):
        """Execute a use case's operations as a single atomic DB transaction."""
        self._log(f"{client_id} \u2192 BEGIN TRANSACTION ({uc['name']})")
        for op in uc.get("operations", []):
            self._db_write(client_id, op)
            time.sleep(0.45)
        self._log(f"{client_id} \u2192 COMMIT  ({uc['commit_msg']})")
        time.sleep(0.4)

    # ── DB write ─────────────────────────

    def _db_write(self, client_id: str, payload: str):
        # Payload format: "operation_name   SQL text -- comment"
        # Split on first run of whitespace to get op_name vs full SQL body
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
        # Include full sql_body in log line so the frontend can parse detail + comment
        log_line = f"[{entry['timestamp']}] {client_id} → {op_name}  {sql_body}"
        self.db.write_log.append(log_line)
        self._log(f"PostgreSQL: {op_name} – {sql_body[:80]}")
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

            # ── Raft Log Replication ──────────────────────────────
            self.phase = "replication"
            self._emit("phase_change", {"phase": self.phase, "step": self.step,
                                         "title": "Step 1 – Raft Log Replication (AppendEntries)"})
            self._log("=== Raft Log Replication – AppendEntries RPCs ===")
            self._log("All writes go through the Raft leader and are replicated to followers before commit")
            if raft_leader:
                self._raft_replicate_logs(raft_leader)
            self._emit("state_update", self.snapshot())
            time.sleep(0.5)

            # ── Raft Heartbeats ───────────────────────────────────
            self.phase = "heartbeat"
            self._emit("phase_change", {"phase": self.phase, "step": self.step,
                                         "title": "Step 1 – Raft Heartbeat Monitoring"})
            self._log("=== Raft Heartbeat Broadcast – leader keepalive ===")
            self._log("Leader sends periodic empty AppendEntries to suppress follower election timeouts")
            if raft_leader:
                self._raft_heartbeats(raft_leader, rounds=3)
            self._emit("state_update", self.snapshot())
            time.sleep(0.5)

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
            self._log("client-3 (System2) issues etcd PUT /db/critical_lock with lease TTL=10s …")
            time.sleep(0.5)

            # Race: client-1 wins (deterministic for demo)
            winner       = "client-1"
            first_loser  = "client-2"
            second_loser = "client-3"
            acquired = self._acquire_lock(winner)
            if acquired:
                self.clients[winner].has_lock       = True
                self.clients[winner].role           = AppClientRole.LEADER
                self.clients[first_loser].role      = AppClientRole.FOLLOWER
                self.clients[second_loser].role     = AppClientRole.FOLLOWER
                self._log(f"etcd CAS: {winner} successfully acquired lock (revision {self.lock.revision}, TTL {LEASE_TTL}s)")
                self._log(f"etcd CAS: {first_loser} lock acquisition FAILED – key already exists")
                self._log(f"etcd CAS: {second_loser} lock acquisition FAILED – key already exists")
                self._log(f"{winner} → APPLICATION LEADER")
                self._log(f"{first_loser}  → APPLICATION FOLLOWER (passive)")
                self._log(f"{second_loser}  → APPLICATION FOLLOWER (passive)")
                self._emit("lock_acquired", {"winner": winner, "loser": f"{first_loser} and {second_loser}",
                                              "lease_ttl": LEASE_TTL})
            self._emit("state_update", self.snapshot())
            time.sleep(1.0)

            # ── STEP 3 – Critical DB Operation ───────────────────
            self.phase = "db_write"
            self.step  = 3
            uc = USE_CASES.get(self.selected_usecase, USE_CASES["ecommerce_order"])
            self._emit("phase_change", {"phase": self.phase, "step": self.step,
                                         "title": f"Step 3 – {uc['icon']} {uc['name']}"})
            self._log(f"=== STEP 3: {uc['name']} ===")
            self._log(f"{winner} holds lock → connecting to PostgreSQL on {self.db.host}:{self.db.port}")
            time.sleep(0.6)
            self._run_usecase(winner, uc)
            self._log(f"{first_loser} is PASSIVE – no DB writes performed (lock not held)")
            self._log(f"{second_loser} is PASSIVE – no DB writes performed (lock not held)")
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

            if self.selected_algorithm == ALGORITHM_BULLY:
                new_raft_leader = self._bully_elect_leader()
            else:
                new_raft_leader = self._raft_elect_leader()
            self._emit("state_update", self.snapshot())
            time.sleep(0.5)

            self._log(f"{first_loser} (System2) detects lock key absent → attempting acquisition …")
            self._log(f"{second_loser} (System2) also contests lock acquisition …")
            time.sleep(0.5)
            acquired2 = self._acquire_lock(first_loser)
            if acquired2:
                self.clients[first_loser].has_lock  = True
                self.clients[first_loser].role      = AppClientRole.LEADER
                self.clients[second_loser].role     = AppClientRole.FOLLOWER
                self._log(f"{first_loser} acquired lock (revision {self.lock.revision}) → NEW APPLICATION LEADER")
                self._log(f"{second_loser} lock acquisition failed → remains FOLLOWER")
                self._emit("lock_acquired", {"winner": first_loser, "loser": f"{winner} and {second_loser}",
                                              "lease_ttl": LEASE_TTL})
                self._log(f"{first_loser} resumes critical DB writes after failover …")
                time.sleep(0.4)
                uc_r = USE_CASES.get(self.selected_usecase, USE_CASES["ecommerce_order"])
                recovery_writes = [
                    "failover_log       INSERT INTO recovery_log(event,prev_leader,new_leader,ts) VALUES('leader_failover','client-1','client-2',NOW())",
                    "system_state       UPDATE system_state SET active_leader='client-2', failover_count=failover_count+1, last_failover=NOW()",
                ] + list(uc_r.get("operations", [])[:3])
                for sql in recovery_writes:
                    self._db_write(first_loser, sql)
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

    def start(self, algorithm: str = ALGORITHM_RAFT, usecase: str = "ecommerce_order"):
        if self.running:
            return
        self.__init__()
        self.selected_algorithm = algorithm
        self.selected_usecase   = usecase
        self._thread = threading.Thread(target=self.run_workflow, daemon=True)
        self._thread.start()

    def reset(self):
        self.running = False
        self.__init__()
        self._emit("reset", {})


# Singleton
sim = SimulationState()
