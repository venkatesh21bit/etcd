"""
etcd_state.py  –  Live state reader for the Flask visualiser (LIVE_MODE=true)
═══════════════════════════════════════════════════════════════════════════════
Replaces simulation.py when the app is connected to a real etcd cluster
and PostgreSQL database.  Returns the same snapshot() shape so the frontend
needs zero changes.

Environment variables:
    ETCD_HOST      – e.g. "monorail.proxy.rlwy.net"
    ETCD_PORT      – e.g. 2379
    DATABASE_URL   – postgres://user:pass@host:port/dbname
    LEASE_TTL      – integer seconds (default 15)
"""

import os
import time
import queue
import threading
import logging
import grpc
import etcd3
import psycopg2
import psycopg2.extras
from typing import Optional, Dict, Any, List

log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
ETCD_HOST    = os.environ.get("ETCD_HOST",  "localhost")
ETCD_PORT    = int(os.environ.get("ETCD_PORT", "2379"))
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:password@localhost:5432/etcd_demo")
LEASE_TTL    = int(os.environ.get("LEASE_TTL", "15"))
LOCK_KEY     = "/db/critical_lock"

# ── Module-level singletons ───────────────────────────────────────────────────
_etcd_client:   Optional[etcd3.Etcd3Client] = None
_db_conn                                     = None
_event_queue: queue.Queue                   = queue.Queue()
_watch_thread: Optional[threading.Thread]   = None
_connected = False


# ══════════════════════════════════════════════════════════════════════════════
# Connection helpers
# ══════════════════════════════════════════════════════════════════════════════

def _get_etcd() -> etcd3.Etcd3Client:
    global _etcd_client, _connected
    if _etcd_client is not None:
        return _etcd_client
    try:
        _etcd_client = etcd3.client(host=ETCD_HOST, port=ETCD_PORT, timeout=4)
        _etcd_client.status()
        _connected = True
        log.info("etcd_state: connected to etcd %s:%d", ETCD_HOST, ETCD_PORT)
    except Exception as exc:
        log.warning("etcd_state: cannot reach etcd: %s", exc)
        _etcd_client = None
        _connected   = False
    return _etcd_client


def _get_db():
    global _db_conn
    if _db_conn is not None:
        try:
            _db_conn.cursor().execute("SELECT 1")
            return _db_conn
        except Exception:
            _db_conn = None
    try:
        _db_conn = psycopg2.connect(DATABASE_URL, connect_timeout=5)
        _db_conn.autocommit = True
        log.info("etcd_state: connected to PostgreSQL")
    except Exception as exc:
        log.warning("etcd_state: cannot reach PostgreSQL: %s", exc)
        _db_conn = None
    return _db_conn


# ══════════════════════════════════════════════════════════════════════════════
# etcd data readers
# ══════════════════════════════════════════════════════════════════════════════

def get_cluster_members() -> List[Dict]:
    """Return info about etcd cluster members from the etcd API."""
    client = _get_etcd()
    if not client:
        return []
    try:
        members = list(client.members)   # materialise the generator
        result  = []
        # Get cluster status to find the Raft leader.
        # etcd3 library: status.leader is the Member object of the current Raft
        # leader, NOT an integer.  status.leader_id does NOT exist.
        status        = client.status()
        leader_member = getattr(status, "leader", None)
        leader_id     = leader_member.id if leader_member is not None else None

        for m in members:
            is_leader = (leader_id is not None and m.id == leader_id)
            result.append({
                "node_id": m.name or str(m.id),
                "member_id": str(m.id),
                "system":  _infer_system(m.name),
                "port":    _extract_port(m.client_urls),
                "role":    "leader" if is_leader else "follower",
                "term":    getattr(status, "raft_term", 0),
                "alive":   True,
            })
        return result
    except Exception as exc:
        log.debug("get_cluster_members error: %s", exc)
        return []


def get_client_presence() -> Dict[str, Dict]:
    """
    Read /clients/<id>/status keys that each client.py writes.
    Dynamically discovers any registered clients then overlays lock ownership.
    Returns { <client_id>: { "alive": bool, ... }, ... }
    """
    client = _get_etcd()
    # Default skeleton for the two known IDs so the UI always has something to render
    presence: Dict[str, Dict] = {
        "client-1": {"client_id": "client-1", "system": "System1", "role": "idle", "has_lock": False, "alive": False},
        "client-2": {"client_id": "client-2", "system": "System2", "role": "idle", "has_lock": False, "alive": False},
    }
    if not client:
        return presence
    try:
        for val, meta in client.get_prefix("/clients/"):
            key = meta.key.decode()
            # key = /clients/<client_id>/status
            parts = key.strip("/").split("/")
            if len(parts) == 3 and parts[2] == "status":
                raw_cid = parts[1]   # may be "2", "client-2", etc.
                alive   = (val.decode() == "alive")

                # Map bare numeric/short IDs to canonical client-N names
                if raw_cid in presence:
                    cid = raw_cid
                elif raw_cid.isdigit():
                    # "1" → "client-1", "2" → "client-2"
                    cid = f"client-{raw_cid}"
                else:
                    # Unknown id — add dynamically
                    cid = raw_cid
                    sys_n = "System2" if raw_cid.endswith("2") else "System1"
                    presence[cid] = {"client_id": cid, "system": sys_n, "role": "idle", "has_lock": False, "alive": False}

                presence[cid]["alive"] = alive
    except Exception as exc:
        log.debug("get_client_presence error: %s", exc)
    return presence


def get_lock_state() -> Dict:
    """Read the current distributed lock state from etcd."""
    client = _get_etcd()
    if not client:
        return {"held_by": None, "ttl_remaining": 0, "revision": 0}
    try:
        val, meta = client.get(LOCK_KEY)
        if val is None:
            return {"held_by": None, "ttl_remaining": 0, "revision": meta.create_revision if meta else 0}

        held_by  = val.decode()
        revision = meta.mod_revision

        # Get TTL from the lease
        ttl_remaining = 0
        if meta.lease:
            try:
                ttl_info        = client.get_lease_info(meta.lease)
                ttl_remaining   = ttl_info.TTL if hasattr(ttl_info, "TTL") else 0
            except Exception:
                pass

        return {
            "held_by":      held_by,
            "ttl_remaining": ttl_remaining,
            "revision":      revision,
        }
    except Exception as exc:
        log.debug("get_lock_state error: %s", exc)
        return {"held_by": None, "ttl_remaining": 0, "revision": 0}


def send_crash_signal(client_id: str) -> bool:
    """
    Write /signal/crash/<client_id> = '1' with a short lease.
    client.py watches this key and calls sys.exit() when it appears.
    """
    client = _get_etcd()
    if not client:
        return False
    try:
        short_lease = client.lease(5)   # auto-cleaned after 5s
        client.put(f"/signal/crash/{client_id}", "1", lease=short_lease)
        _emit("crash_signalled", {
            "client_id":   client_id,
            "message":     f"Crash signal sent to {client_id} — process will exit within seconds.",
        })
        log.warning("Crash signal sent to %s", client_id)
        return True
    except Exception as exc:
        log.error("send_crash_signal error: %s", exc)
        return False


def clear_crash_signal(client_id: str) -> bool:
    """Delete /signal/crash/<client_id> (useful to allow a restart)."""
    client = _get_etcd()
    if not client:
        return False
    try:
        client.delete(f"/signal/crash/{client_id}")
        return True
    except Exception as exc:
        log.error("clear_crash_signal error: %s", exc)
        return False


# ══════════════════════════════════════════════════════════════════════════════
# PostgreSQL data reader
# ══════════════════════════════════════════════════════════════════════════════

def get_db_writes(limit: int = 15) -> Dict:
    """Return recent rows from distributed_ops table."""
    conn = _get_db()
    if not conn:
        return {"host": ETCD_HOST, "port": 5432, "records": [], "write_log": []}
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id, written_by, operation, ts FROM distributed_ops ORDER BY ts DESC LIMIT %s",
                (limit,),
            )
            rows = [dict(r) for r in cur.fetchall()]
            # Convert timestamps to strings
            for r in rows:
                if r.get("ts"):
                    r["ts"] = r["ts"].strftime("%H:%M:%S")
    except Exception as exc:
        log.debug("get_db_writes error: %s", exc)
        rows = []

    write_log = [
        f"[{r['ts']}] {r['written_by']} → {r['operation']}"
        for r in rows
    ]
    records = [
        {"id": r["id"], "written_by": r["written_by"],
         "payload": r["operation"], "timestamp": r["ts"]}
        for r in rows
    ]
    return {
        "host":      ETCD_HOST,
        "port":      5432,
        "records":   records,
        "write_log": write_log,
    }


def get_total_writes() -> int:
    """Quick count for the event log."""
    conn = _get_db()
    if not conn:
        return 0
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM distributed_ops")
            return cur.fetchone()[0]
    except Exception:
        return 0


# ══════════════════════════════════════════════════════════════════════════════
# Full snapshot (same shape as simulation.py)
# ══════════════════════════════════════════════════════════════════════════════

_last_lock_state:   Dict = {}
_last_total_writes: int  = 0

def _normalize_client_id(raw: str) -> str:
    """Map short IDs like '1','2' to canonical 'client-1','client-2'."""
    if raw and raw.isdigit():
        return f"client-{raw}"
    return raw


def snapshot() -> Dict[str, Any]:
    """
    Build and return a JSON-serialisable snapshot of LIVE state.
    Matches the exact shape produced by simulation.py's SimulationState.snapshot().
    """
    global _last_lock_state, _last_total_writes

    members  = get_cluster_members()
    presence = get_client_presence()
    lock     = get_lock_state()
    db       = get_db_writes()

    # Normalise the lock holder ID so it always matches the presence keys
    if lock.get("held_by"):
        lock["held_by"] = _normalize_client_id(lock["held_by"])

    # Enrich presence dicts with lock info
    for cid, cdata in presence.items():
        cdata["has_lock"] = (lock["held_by"] == cid)
        if cdata["has_lock"]:
            cdata["role"] = "leader"
        elif cdata["alive"]:
            cdata["role"] = "follower"
        else:
            cdata["role"] = "idle"

    # Detect events by diffing against previous snapshot
    _detect_and_emit_events(lock, db)
    _last_lock_state   = lock
    _last_total_writes = len(db["records"])

    # Build node map (fall back to static list if etcd unreachable)
    if not members:
        members = _static_node_list()
    nodes = {m["node_id"]: m for m in members}

    # Determine phase
    phase = _infer_phase(lock, presence)

    return {
        "phase":   phase,
        "step":    _phase_to_step(phase),
        "nodes":   nodes,
        "clients": presence,
        "lock":    lock,
        "db":      db,
        "logs":    _recent_logs(),
        "live":    True,
        "connected": _connected,
    }


# ══════════════════════════════════════════════════════════════════════════════
# Event detection (diff-based)
# ══════════════════════════════════════════════════════════════════════════════

_log_buffer:  List[str] = []
_log_lock     = threading.Lock()

def _emit(event_type: str, data: dict):
    data["type"] = event_type
    _event_queue.put(data)

def _log(msg: str):
    ts = time.strftime("%H:%M:%S")
    entry = f"[{ts}] {msg}"
    with _log_lock:
        _log_buffer.append(entry)
        if len(_log_buffer) > 200:
            _log_buffer.pop(0)
    _event_queue.put({"type": "log", "message": entry})

def _recent_logs() -> List[str]:
    with _log_lock:
        return list(_log_buffer[-30:])

def _detect_and_emit_events(lock: Dict, db: Dict):
    global _last_lock_state, _last_total_writes
    prev_held = _last_lock_state.get("held_by")
    curr_held = lock.get("held_by")

    if prev_held != curr_held:
        if curr_held and not prev_held:
            _log(f"🔒 Lock ACQUIRED by {curr_held} (revision {lock['revision']})")
            _emit("lock_acquired", {
                "winner":    curr_held,
                "loser":     "client-2" if curr_held == "client-1" else "client-1",
                "lease_ttl": LEASE_TTL,
            })
        elif not curr_held and prev_held:
            _log(f"🔓 Lock RELEASED — {prev_held} no longer holds the lock")
            _emit("lock_released", {"previous_holder": prev_held})

    new_writes = len(db["records"])
    if new_writes > _last_total_writes and db["records"]:
        newest = db["records"][0]
        _emit("db_write", {
            "id":         newest["id"],
            "written_by": newest["written_by"],
            "payload":    newest["payload"],
            "timestamp":  newest["timestamp"],
        })
        _log(f"🐘 DB WRITE by {newest['written_by']}: {newest['payload']}")


# ══════════════════════════════════════════════════════════════════════════════
# Background poller — pushes SSE state_update events every 2s
# ══════════════════════════════════════════════════════════════════════════════

_poll_running  = False
_poll_thread   = None

def start_background_poll(interval: float = 2.0):
    global _poll_running, _poll_thread
    if _poll_running:
        return
    _poll_running = True

    def _loop():
        log.info("etcd_state: background poll started (%.1fs interval)", interval)
        while _poll_running:
            try:
                snap = snapshot()
                _event_queue.put({"type": "state_update", **snap})
            except Exception as exc:
                log.debug("poll error: %s", exc)
            time.sleep(interval)

    _poll_thread = threading.Thread(target=_loop, daemon=True)
    _poll_thread.start()


def stop_background_poll():
    global _poll_running
    _poll_running = False


# ══════════════════════════════════════════════════════════════════════════════
# Helper utilities
# ══════════════════════════════════════════════════════════════════════════════

def _infer_system(name: Optional[str]) -> str:
    if name and ("1" in name or "node1" in name or name.endswith("-1")):
        return "System1"
    return "System2"


def _extract_port(urls) -> int:
    for url in (urls or []):
        try:
            return int(url.rsplit(":", 1)[-1])
        except Exception:
            pass
    return 2379


def _static_node_list() -> List[Dict]:
    """Fallback node list when etcd is unreachable."""
    return [
        {"node_id": "etcd-1", "member_id": "1", "system": "System1",
         "port": 2379, "role": "unknown", "term": 0, "alive": False},
        {"node_id": "etcd-2", "member_id": "2", "system": "System1",
         "port": 2381, "role": "unknown", "term": 0, "alive": False},
        {"node_id": "etcd-3", "member_id": "3", "system": "System2",
         "port": 2383, "role": "unknown", "term": 0, "alive": False},
    ]


def _infer_phase(lock: Dict, presence: Dict) -> str:
    any_alive = any(v.get("alive") for v in presence.values())
    if not any_alive:
        return "idle"
    if lock.get("held_by"):
        return "db_write"
    return "lock"


def _phase_to_step(phase: str) -> int:
    return {"idle": 0, "init": 1, "election": 1,
            "lock": 2, "db_write": 3,
            "failure": 4, "recovery": 4, "done": 4}.get(phase, 0)


# Module-level event queue alias (used by app.py)
# Do NOT recreate — just expose the existing _event_queue under a public name
event_queue = _event_queue
