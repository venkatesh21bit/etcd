"""
client.py  –  Real etcd-backed Python client for the Leader Election demo
═══════════════════════════════════════════════════════════════════════════
Run on EACH machine (or as two Railway services):

    python client.py

Required environment variables (or .env file):
    CLIENT_ID      – unique name, e.g. "client-1"  or  "client-2"
    ETCD_HOST      – etcd endpoint host, e.g. "monorail.proxy.rlwy.net"
    ETCD_PORT      – etcd client port, e.g. "2379"
    DATABASE_URL   – postgres://user:pass@host:port/dbname
    LEASE_TTL      – integer seconds (default 15)
"""

import os
import sys
import time
import signal
import threading
import logging
import grpc
import psycopg2
import psycopg2.extras
import etcd3
from dotenv import load_dotenv

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────
CLIENT_ID    = os.environ.get("CLIENT_ID",  "client-1")
ETCD_HOST    = os.environ.get("ETCD_HOST",  "localhost")
ETCD_PORT    = int(os.environ.get("ETCD_PORT", "2379"))
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:password@localhost:5432/etcd_demo")
LEASE_TTL    = int(os.environ.get("LEASE_TTL", "15"))
LOCK_KEY     = "/db/critical_lock"
SIGNAL_KEY   = f"/signal/crash/{CLIENT_ID}"

# File written at crash time so the restarted process knows to back off
_CRASH_STAMP = f"/tmp/etcd_crash_{CLIENT_ID}"
# How long to wait before re-contesting after a crash (gives the other client time to take over)
CRASH_BACKOFF = LEASE_TTL + 20

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format=f"[%(asctime)s] [{CLIENT_ID}] %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── State ─────────────────────────────────────────────────────────────────────
_etcd:    etcd3.Etcd3Client = None
_db_conn = None
_lease   = None
_is_leader = False
_stop_event = threading.Event()


# ══════════════════════════════════════════════════════════════════════════════
# Connections
# ══════════════════════════════════════════════════════════════════════════════

def connect_etcd() -> etcd3.Etcd3Client:
    """Create and return an etcd3 client with retry logic."""
    for attempt in range(1, 10):
        try:
            client = etcd3.client(host=ETCD_HOST, port=ETCD_PORT, timeout=5)
            client.status()          # verify connectivity
            log.info("Connected to etcd %s:%d", ETCD_HOST, ETCD_PORT)
            return client
        except Exception as exc:
            log.warning("etcd connect attempt %d failed: %s", attempt, exc)
            time.sleep(3)
    log.error("Could not connect to etcd after retries – exiting.")
    sys.exit(1)


def connect_db():
    """Return a psycopg2 connection."""
    for attempt in range(1, 10):
        try:
            conn = psycopg2.connect(DATABASE_URL, connect_timeout=10)
            conn.autocommit = False
            log.info("Connected to PostgreSQL")
            return conn
        except Exception as exc:
            log.warning("DB connect attempt %d failed: %s", attempt, exc)
            time.sleep(3)
    log.error("Could not connect to PostgreSQL after retries – exiting.")
    sys.exit(1)


def ensure_schema(conn):
    """Create the distributed_ops table if it doesn't exist."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS distributed_ops (
                id         SERIAL PRIMARY KEY,
                written_by TEXT        NOT NULL,
                operation  TEXT        NOT NULL,
                ts         TIMESTAMPTZ DEFAULT NOW()
            );
        """)
    conn.commit()
    log.info("Schema ready (distributed_ops table)")


# ══════════════════════════════════════════════════════════════════════════════
# Crash signal watcher
# ══════════════════════════════════════════════════════════════════════════════

def watch_crash_signal():
    """
    Watch /signal/crash/<CLIENT_ID> in etcd.
    If the visualiser writes '1' to this key the process exits immediately,
    simulating a real crash.  We first write a crash-stamp file so the
    restarted process knows to back off before re-contesting.
    """
    log.info("Watching crash signal key: %s", SIGNAL_KEY)

    def _callback(event):
        if hasattr(event, "events"):
            for e in event.events:
                if isinstance(e, etcd3.events.PutEvent):
                    log.warning("💥 Crash signal received! Writing backoff stamp and exiting NOW.")
                    # Write timestamp so the restarted process backs off
                    try:
                        with open(_CRASH_STAMP, "w") as f:
                            f.write(str(time.time()))
                    except Exception:
                        pass
                    _stop_event.set()
                    os.kill(os.getpid(), signal.SIGTERM)

    try:
        watch_id = _etcd.add_watch_callback(SIGNAL_KEY, _callback)
        return watch_id
    except Exception as exc:
        log.error("Could not start crash watcher: %s", exc)
        return None


# ══════════════════════════════════════════════════════════════════════════════
# Leader lock acquisition
# ══════════════════════════════════════════════════════════════════════════════

def try_acquire_lock() -> bool:
    """
    Attempt to acquire the distributed lock using etcd's CAS semantics:
    PUT key only if it doesn't already exist (create_revision == 0).
    Returns True if this client becomes the leader.
    """
    global _lease, _is_leader
    try:
        _lease = _etcd.lease(LEASE_TTL)
        # Transactional compare-and-swap: put if key absent
        success, _ = _etcd.transaction(
            compare=[_etcd.transactions.create(LOCK_KEY) == "0"],
            success=[_etcd.transactions.put(LOCK_KEY, CLIENT_ID, lease=_lease)],
            failure=[],
        )
        if success:
            log.info("🔒 Lock ACQUIRED — I am the APPLICATION LEADER")
            _is_leader = True
        else:
            log.info("Lock held by another client — I am a FOLLOWER")
            _is_leader = False
            # Let the orphaned lease expire
            try:
                _lease.revoke()
            except Exception:
                pass
            _lease = None
        return success
    except Exception as exc:
        log.error("Lock acquisition error: %s", exc)
        return False


def release_lock():
    """Gracefully revoke lease (deletes the lock key automatically)."""
    global _lease, _is_leader
    if _lease:
        try:
            _lease.revoke()
            log.info("Lock released (lease revoked)")
        except Exception as exc:
            log.warning("Could not revoke lease: %s", exc)
    _is_leader = False
    _lease = None


# ══════════════════════════════════════════════════════════════════════════════
# Leader work loop — DB writes
# ══════════════════════════════════════════════════════════════════════════════

def leader_work_loop(conn):
    """
    Continuously insert rows into distributed_ops while holding the lock.
    Each iteration also does a lease keepalive to prevent expiry.
    Exits gracefully when _stop_event is set or lease is lost.
    """
    global _lease
    counter = 0

    # Ordered write batches – each entry is (short_op_name, detail_string)
    # detail uses realistic mock numbers so the visualiser looks live
    import random
    def _batch():
        order_id  = random.randint(7800, 7999)
        skus      = ["SKU-409", "SKU-112", "SKU-774", "SKU-033", "SKU-281"]
        carriers  = ["FedEx", "UPS", "DHL", "USPS"]
        methods   = ["wire", "card", "credit", "BNPL"]
        sku       = random.choice(skus)
        qty       = random.randint(1, 30)
        price     = round(random.uniform(19.99, 249.99), 2)
        total     = round(qty * price, 2)
        stock_old = random.randint(50, 800)
        stock_new = stock_old - qty
        acc_in    = random.randint(1000, 1099)
        acc_bal   = round(random.uniform(2000, 15000), 2)
        new_bal   = round(acc_bal - total, 2)
        tx_id     = random.randint(3000, 3999)
        po_id     = random.randint(4400, 4499)
        carrier   = random.choice(carriers)
        method    = random.choice(methods)
        tax       = round(total * 0.15, 2)
        batch_no  = random.randint(30, 99)
        matched   = random.randint(100, 200)
        eta_day   = random.randint(5, 28)
        weight    = round(random.uniform(0.5, 8.0), 1)

        return [
            ("transaction_begin",    f"BEGIN order_id={order_id} client={CLIENT_ID}"),
            ("order_insert",         f"INSERT orders id={order_id} sku={sku} qty={qty} unit_price=${price} total=${total}"),
            ("inventory_decrement",  f"UPDATE inventory sku={sku} stock {stock_old}→{stock_new} reserved=+{qty}"),
            ("ledger_debit",         f"UPDATE accounts acc=ACC-{acc_in} -{total} balance ${acc_bal}→${new_bal}"),
            ("shipment_create",      f"INSERT shipments order={order_id} carrier={carrier} weight={weight}kg eta=03-{eta_day:02d}"),
            ("audit_log",            f"INSERT audit event=order_confirmed order={order_id} amount=${total} tax=${tax}"),
            ("po_update",            f"UPDATE purchase_orders po={po_id} status=in_transit carrier={carrier}"),
            ("transaction_commit",   f"COMMIT order={order_id} {qty} units rows=6 ok"),
        ]

    current_batch = _batch()
    log.info("Starting DB write loop…")

    while not _stop_event.is_set():
        # Rotate through batch; regenerate when batch exhausted
        if counter >= len(current_batch):
            counter = 0
            current_batch = _batch()

        op_name, detail = current_batch[counter]
        operation = f"{op_name}: {detail}"
        counter += 1

        # Verify lock still held
        try:
            val, meta = _etcd.get(LOCK_KEY)
            if val is None or val.decode() != CLIENT_ID:
                log.warning("Lock lost! Stopping write loop.")
                break
        except Exception as exc:
            log.error("etcd get error in write loop: %s", exc)
            break

        # Write to PostgreSQL
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO distributed_ops (written_by, operation) VALUES (%s, %s)",
                    (CLIENT_ID, operation),
                )
            conn.commit()
            log.info("📝 DB WRITE  %-20s", op_name)
        except Exception as exc:
            log.error("DB write error: %s", exc)
            try:
                conn.rollback()
            except Exception:
                pass

        # Keepalive — refresh the lease TTL using the Lease object's own method
        try:
            _lease.refresh()
        except Exception as exc:
            log.warning("Lease keepalive failed: %s", exc)
            break

        _stop_event.wait(3)   # write every 3 seconds

    log.info("Exiting DB write loop.")


# ══════════════════════════════════════════════════════════════════════════════
# Follower watch loop — wait for lock to be released
# ══════════════════════════════════════════════════════════════════════════════

def follower_watch_loop() -> bool:
    """
    Watch the lock key. When it disappears (leader crash / lease expiry),
    return True so the main loop retries lock acquisition.
    """
    log.info("FOLLOWER: watching %s for leader departure…", LOCK_KEY)

    # CRITICAL: check if key is already gone before starting the watch.
    # If we lost the race between try_acquire_lock() and here, the key may
    # have been deleted already — in that case jump straight back to contesting.
    try:
        val, _ = _etcd.get(LOCK_KEY)
        if val is None:
            log.info("Lock key already vacant — re-contesting immediately")
            return True
    except Exception:
        pass

    lock_released = threading.Event()

    def _on_event(event):
        if hasattr(event, "events"):
            for e in event.events:
                if isinstance(e, etcd3.events.DeleteEvent):
                    log.info("Lock key deleted — attempting to become leader!")
                    lock_released.set()

    try:
        watch_id = _etcd.add_watch_callback(LOCK_KEY, _on_event)
        # Block until lock_released or stop_event
        while not _stop_event.is_set() and not lock_released.is_set():
            time.sleep(0.5)
        _etcd.cancel_watch(watch_id)
    except Exception as exc:
        log.error("Watch error: %s", exc)

    return lock_released.is_set()


# ══════════════════════════════════════════════════════════════════════════════
# Register presence in etcd (for visualiser discovery)
# ══════════════════════════════════════════════════════════════════════════════

def register_presence():
    """Write /clients/<CLIENT_ID>/status = 'alive' with a long lease."""
    try:
        presence_lease = _etcd.lease(60)
        _etcd.put(f"/clients/{CLIENT_ID}/status", "alive", lease=presence_lease)
        # Start a background thread to keep this lease alive
        def _keepalive():
            while not _stop_event.is_set():
                try:
                    presence_lease.refresh()   # Lease.refresh() is the correct etcd3 API
                except Exception:
                    pass
                _stop_event.wait(20)
        t = threading.Thread(target=_keepalive, daemon=True)
        t.start()
        log.info("Presence registered: /clients/%s/status", CLIENT_ID)
    except Exception as exc:
        log.warning("Could not register presence: %s", exc)


# ══════════════════════════════════════════════════════════════════════════════
# Signal handlers
# ══════════════════════════════════════════════════════════════════════════════

def _handle_signal(signum, frame):
    log.warning("Signal %d received — shutting down gracefully.", signum)
    _stop_event.set()

signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT,  _handle_signal)


# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════

def main():
    global _etcd, _db_conn

    log.info("═══════════════════════════════════════════")
    log.info("  etcd Leader Election Client  [%s]", CLIENT_ID)
    log.info("  etcd   → %s:%d", ETCD_HOST, ETCD_PORT)
    log.info("  DB     → PostgreSQL")
    log.info("  Lease  → %ds TTL", LEASE_TTL)
    log.info("═══════════════════════════════════════════")

    # ── Post-crash backoff ──────────────────────────────────────────────────
    # If we were just crash-signalled, wait CRASH_BACKOFF seconds before
    # re-contesting so the surviving client can take over first.
    try:
        if os.path.exists(_CRASH_STAMP):
            with open(_CRASH_STAMP) as f:
                crash_ts = float(f.read().strip())
            elapsed  = time.time() - crash_ts
            remaining = CRASH_BACKOFF - elapsed
            if remaining > 0:
                log.warning(
                    "⏳ Post-crash backoff: waiting %.0fs before re-contesting "
                    "(gives the other client time to become leader)…", remaining
                )
                time.sleep(remaining)
            os.remove(_CRASH_STAMP)
    except Exception:
        pass

    _etcd    = connect_etcd()
    _db_conn = connect_db()
    ensure_schema(_db_conn)
    register_presence()
    watch_crash_signal()

    while not _stop_event.is_set():
        log.info("--- Contesting for distributed lock ---")
        is_leader = try_acquire_lock()

        if is_leader:
            # Run leader work until lock is lost or crash signal
            leader_work_loop(_db_conn)
            release_lock()
            if _stop_event.is_set():
                break
            # Brief pause before re-contesting
            log.info("Re-contesting after losing leadership…")
            time.sleep(2)
        else:
            # Block until lock is free
            released = follower_watch_loop()
            if not released:
                break  # stop_event was set
            time.sleep(0.5)  # small delay before contesting

    log.info("Client %s shutting down.", CLIENT_ID)
    release_lock()
    if _db_conn:
        try:
            _db_conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
