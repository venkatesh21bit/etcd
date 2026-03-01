"""
app.py – Flask server for the etcd Leader Election Workflow Visualiser

Supports two modes controlled by LIVE_MODE env var:
  LIVE_MODE=false  (default) – in-memory simulation (no external deps)
  LIVE_MODE=true             – connects to real etcd + PostgreSQL
"""

import os
import json
import time
import logging
from flask import Flask, render_template, Response, jsonify, request
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s %(message)s")
log = logging.getLogger(__name__)

LIVE_MODE = os.environ.get("LIVE_MODE", "false").lower() == "true"

# ── Load the correct backend ──────────────────────────────────────────────────
if LIVE_MODE:
    import etcd_state
    etcd_state.start_background_poll(interval=2.0)
    log.info("▶  LIVE MODE – connected to real etcd + PostgreSQL")
    _event_queue = etcd_state.event_queue

    def _snapshot():
        return etcd_state.snapshot()
else:
    from simulation import sim
    log.info("▶  DEMO MODE – using in-memory simulation")
    _event_queue = sim.event_queue

    def _snapshot():
        return sim.snapshot()

app = Flask(__name__)
app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 0  # disable static file caching
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "etcd-case-study")


# ══════════════════════════════════════════════════════════════════════════════
# Routes
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/")
def index():
    return render_template("index.html", live_mode=LIVE_MODE)


@app.route("/api/state")
def state():
    """JSON snapshot of current state (live or simulated)."""
    return jsonify(_snapshot())


# ── Demo-only routes ──────────────────────────────────────────────────────────

@app.route("/api/start", methods=["POST"])
def start():
    """Start (or restart) the demo workflow simulation."""
    if LIVE_MODE:
        return jsonify({"status": "noop", "reason": "live mode — clients run independently"})
    sim.start()
    return jsonify({"status": "started"})


@app.route("/api/reset", methods=["POST"])
def reset():
    """Reset the demo simulation to idle state."""
    if LIVE_MODE:
        return jsonify({"status": "noop", "reason": "live mode"})
    sim.reset()
    return jsonify({"status": "reset"})


# ── Live-only routes ──────────────────────────────────────────────────────────

@app.route("/api/crash/<client_id>", methods=["POST"])
def crash_client(client_id: str):
    """
    Send a crash signal to the named client via etcd.
    The client watches /signal/crash/<client_id> and calls sys.exit() on receipt.
    """
    if not LIVE_MODE:
        # In demo mode: simulate the crash step
        sim._log(f"⚠ Manual crash triggered for {client_id}")
        sim.etcd_nodes.get("etcd-1") and setattr(sim.etcd_nodes["etcd-1"], "alive", False)
        sim.etcd_nodes.get("etcd-2") and setattr(sim.etcd_nodes["etcd-2"], "alive", False)
        sim._emit("node_crash", {"crashed": ["etcd-1", "etcd-2"], "system": "System1"})
        sim._emit("state_update", sim.snapshot())
        return jsonify({"status": "demo_crash_simulated"})

    ok = etcd_state.send_crash_signal(client_id)
    if ok:
        return jsonify({"status": "signal_sent", "target": client_id})
    return jsonify({"status": "error", "reason": "could not reach etcd"}), 503


@app.route("/api/clear_signal/<client_id>", methods=["POST"])
def clear_signal(client_id: str):
    """Delete the crash signal key so the client can be restarted cleanly."""
    if not LIVE_MODE:
        return jsonify({"status": "noop"})
    ok = etcd_state.clear_crash_signal(client_id)
    return jsonify({"status": "cleared" if ok else "error"})


# ── SSE stream ────────────────────────────────────────────────────────────────

@app.route("/api/stream")
def stream():
    """
    Server-Sent Events endpoint.
    Sends an immediate state snapshot then drains the event queue continuously.
    IMPORTANT: gunicorn must be started with a single worker (--workers 1)
    and threading enabled (--threads 4) for this to work correctly.
    """
    def event_generator():
        snap = _snapshot()
        yield f"data: {json.dumps({'type': 'state_update', **snap})}\n\n"
        while True:
            try:
                event = _event_queue.get(timeout=1.0)
                yield f"data: {json.dumps(event)}\n\n"
            except Exception:
                yield "data: {\"type\":\"heartbeat\"}\n\n"

    return Response(
        event_generator(),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ══════════════════════════════════════════════════════════════════════════════
# Entry point
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    mode_label = "LIVE (etcd + PostgreSQL)" if LIVE_MODE else "DEMO (in-memory simulation)"
    print(f"\n  etcd Leader Election Workflow Visualiser")
    print(f"  Mode: {mode_label}")
    print(f"  ─────────────────────────────────────────")
    print(f"  Open → http://127.0.0.1:5000\n")
    app.run(debug=not LIVE_MODE, threaded=True, use_reloader=False, port=int(os.environ.get("PORT", 5000)))
