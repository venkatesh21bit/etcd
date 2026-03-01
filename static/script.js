/* ═══════════════════════════════════════════════════════
   etcd Leader Election Visualiser – Frontend Logic
   ═══════════════════════════════════════════════════════ */

"use strict";

// LIVE_MODE is injected by Flask/Jinja2 in index.html as a const
// If the page was loaded without template rendering, default to false
const _LIVE = (typeof LIVE_MODE !== "undefined") ? LIVE_MODE : false;

// ─────────────────────────────────────────
const LEASE_TTL_MAX = 15;  // sync with LEASE_TTL in client.py

let evtSource  = null;
let logCount   = 0;
let _currentLeaderId = null;   // tracks who holds the lock right now
let _totalWrites     = 0;
let _sseRetries      = 0;

// ─────────────────────────────────────────
// SSE connection
// ─────────────────────────────────────────
function connectSSE() {
  if (evtSource) evtSource.close();
  evtSource = new EventSource("/api/stream");

  evtSource.onopen = () => {
    _sseRetries = 0;
    setConnStatus(true);
  };

  evtSource.onmessage = (e) => {
    const data = JSON.parse(e.data);
    handleEvent(data);
  };

  evtSource.onerror = () => {
    setConnStatus(false);
    appendLog("[SSE] Connection error – retrying…", "highlight-red");
    _sseRetries++;
    // Exponential backoff up to 10s
    const delay = Math.min(10000, 500 * Math.pow(2, _sseRetries));
    setTimeout(connectSSE, delay);
  };
}

// ─────────────────────────────────────────
// Event dispatcher
// ─────────────────────────────────────────
function handleEvent(data) {
  switch (data.type) {
    case "heartbeat":           onHeartbeat(); break;
    case "log":                 appendLog(data.message); break;
    case "state_update":        applyState(data);         break;
    case "phase_change":        setPhase(data);           break;
    case "raft_election":       onRaftElection(data);     break;
    case "raft_leader_elected": onRaftLeader(data);       break;
    case "lock_acquired":       onLockAcquired(data);     break;
    case "lock_released":       onLockReleased(data);     break;
    case "node_crash":          onCrash(data);            break;
    case "crash_signalled":     onCrashSignalled(data);   break;
    case "db_write":            onDbWrite(data);          break;
    case "workflow_complete":   onComplete(data);         break;
    case "reset":               onReset();                break;
  }
}

// ─────────────────────────────────────────
// Phase banner & progress steps
// ─────────────────────────────────────────
const phaseIcons = {
  idle:      "◎",
  init:      "⚙",
  election:  "⚡",
  lock:      "🔒",
  db_write:  "🐘",
  failure:   "⚠",
  recovery:  "♻",
  done:      "✓",
};

function setPhase(data) {
  const banner = document.getElementById("phase-banner");
  banner.className = `phase-banner ${data.phase}`;
  document.getElementById("phase-icon").textContent  = phaseIcons[data.phase] || "◎";
  document.getElementById("phase-title").textContent = data.title || data.phase;
  document.getElementById("phase-step").textContent  = data.step ? `Step ${data.step} / 4` : "";

  // Update progress circles
  const step = data.step || 0;
  for (let i = 1; i <= 4; i++) {
    const el = document.getElementById(`ps-${i}`);
    el.classList.remove("active", "done");
    if (i < step)  el.classList.add("done");
    if (i === step) el.classList.add("active");
  }
  // Connecting lines
  document.querySelectorAll(".ps-line").forEach((ln, idx) => {
    ln.classList.toggle("done", idx + 1 < step);
  });
}

// ─────────────────────────────────────────
// Full state snapshot → update all panels
// ─────────────────────────────────────────
function applyState(state) {
  if (state.nodes)   renderNodes(state.nodes);
  if (state.clients) renderClients(state.clients);
  if (state.lock)    renderLock(state.lock);
  if (state.db)      renderDB(state.db);
  if (state.logs)    renderAllLogs(state.logs);
  if (state.phase)   setPhase({ phase: state.phase, step: state.step, title: phaseTitleMap[state.phase] || state.phase });
}

const phaseTitleMap = {
  idle:      "Idle – Press \"Run Workflow\" to begin",
  init:      "Step 1 – Cluster Initialisation",
  election:  "Step 1 – Raft Internal Leader Election",
  lock:      "Step 2 – Application Leader Election (Distributed Lock)",
  db_write:  "Step 3 – Critical Database Operation",
  failure:   "Step 4 – Failure Scenario",
  recovery:  "Step 4 – Recovery & New Leader Election",
  done:      "Workflow Complete ✓",
};

// ─────────────────────────────────────────
// etcd Nodes table + arch chips
// ─────────────────────────────────────────
function renderNodes(nodes) {
  const tbody = document.getElementById("nodes-tbody");
  tbody.innerHTML = "";

  Object.values(nodes).forEach(n => {
    const alive     = n.alive;
    const roleClass = alive ? `role-${n.role}` : "role-crashed";
    const dotClass  = alive ? "dot-alive" : "dot-crashed";
    const roleLabel = alive ? n.role.toUpperCase() : "CRASHED";

    tbody.insertAdjacentHTML("beforeend", `
      <tr>
        <td><strong>${n.node_id}</strong></td>
        <td>${n.system}</td>
        <td><code>:${n.port}</code></td>
        <td>${n.term}</td>
        <td><span class="role-pill ${roleClass}">${roleLabel}</span></td>
        <td><span class="alive-dot ${dotClass}"></span>${alive ? "Running" : "Crashed"}</td>
      </tr>
    `);

    // Update arch chip
    const chipId  = `chip-${n.node_id}`;
    const badgeId = `badge-${n.node_id}`;
    const chip    = document.getElementById(chipId);
    const badge   = document.getElementById(badgeId);
    if (!chip) return;

    chip.className  = `node-chip etcd-chip chip-${alive ? n.role : "crashed"}`;
    badge.textContent = roleLabel;
  });
}

// ─────────────────────────────────────────
// Application clients
// ─────────────────────────────────────────
function renderClients(clients) {
  Object.values(clients).forEach(c => {
    const roleEl    = document.getElementById(`cc-role-${c.client_id}`);
    const lockEl    = document.getElementById(`cc-lock-${c.client_id}`);
    const actionsEl = document.getElementById(`cc-actions-${c.client_id}`);
    const card      = document.getElementById(`cc-${c.client_id}`);
    const chip      = document.getElementById(`chip-${c.client_id}`);
    const badge     = document.getElementById(`badge-${c.client_id}`);
    if (!roleEl) return;

    roleEl.className   = `cc-role ${c.role}`;
    roleEl.textContent = c.role.toUpperCase();
    lockEl.textContent = c.has_lock ? "🔒 Lock held" : "";

    card.classList.toggle("is-leader",   c.role === "leader");
    card.classList.toggle("is-follower", c.role === "follower");
    card.classList.toggle("is-contest",  c.role === "contesting");
    card.classList.toggle("is-offline",  c.alive === false);

    if (chip && badge) {
      chip.className    = `node-chip client-chip chip-${c.role === "leader" ? "app-leader" : c.role}`;
      badge.textContent = c.role.toUpperCase();
    }

    // Show crash button only in LIVE_MODE when client is the active leader
    if (actionsEl) {
      if (_LIVE && c.has_lock && c.role === "leader") {
        actionsEl.innerHTML = `<button class="btn btn-red btn-sm" onclick="crashLeader('${c.client_id}')">
          💥 Crash this leader
        </button>`;
      } else {
        actionsEl.innerHTML = "";
      }
    }
  });
}

// ─────────────────────────────────────────
// Distributed lock
// ─────────────────────────────────────────
function renderLock(lock) {
  const heldBy   = document.getElementById("ld-held-by");
  const ttlEl    = document.getElementById("ld-ttl");
  const revEl    = document.getElementById("ld-revision");
  const bar      = document.getElementById("ttl-bar");
  const widget   = document.getElementById("lock-widget");
  const archHeld = document.getElementById("lk-held");
  const archTTL  = document.getElementById("lk-ttl");
  const btnCrash = document.getElementById("btn-crash");

  _currentLeaderId = lock.held_by || null;
  const ttlMax     = LEASE_TTL_MAX;

  if (lock.held_by) {
    heldBy.textContent   = lock.held_by;
    ttlEl.textContent    = `${lock.ttl_remaining}s remaining`;
    archHeld.textContent = lock.held_by;
    archTTL.textContent  = `TTL: ${lock.ttl_remaining}s`;
    widget.classList.add("active");

    const pct = Math.min(100, (lock.ttl_remaining / ttlMax) * 100);
    bar.style.width = `${pct}%`;
    bar.classList.toggle("low", pct < 30);

    // Show header crash button in live mode
    if (btnCrash && _LIVE) {
      btnCrash.textContent = `💥 Crash ${lock.held_by}`;
      btnCrash.classList.remove("hidden");
      btnCrash.onclick = () => crashLeader(lock.held_by);
    }

    // Update write counter bar
    const leaderLabel = document.getElementById("active-leader-label");
    if (leaderLabel) leaderLabel.textContent = lock.held_by;
  } else {
    heldBy.textContent   = "— vacant —";
    ttlEl.textContent    = "—";
    archHeld.textContent = "— vacant —";
    archTTL.textContent  = "";
    widget.classList.remove("active");
    bar.style.width = "0%";
    if (btnCrash) btnCrash.classList.add("hidden");

    const leaderLabel = document.getElementById("active-leader-label");
    if (leaderLabel) leaderLabel.textContent = "—";
  }
  revEl.textContent = lock.revision;
}

// ─────────────────────────────────────────
// PostgreSQL write log
// ─────────────────────────────────────────
function renderDB(db) {
  const el = document.getElementById("db-write-log");
  if (!db.write_log || db.write_log.length === 0) {
    el.innerHTML = '<span class="empty-cell">No writes yet…</span>';
    return;
  }
  el.innerHTML = db.write_log.map(line => {
    const parts = line.match(/^\[(\S+)\]\s+(\S+)\s+→\s+(.+)$/);
    if (parts) {
      return `<div class="sql-entry">
        <span class="sql-ts">${parts[1]}</span>
        <span class="sql-writer">${parts[2]}</span>
        <span class="sql-cmd">${parts[3]}</span>
      </div>`;
    }
    return `<div class="sql-entry">${line}</div>`;
  }).join("");
  el.scrollTop = el.scrollHeight;

  // Update total write counter
  _totalWrites = db.records ? db.records.length : 0;
  const totalEl = document.getElementById("write-total");
  if (totalEl) totalEl.textContent = db.records ? db.records.length : 0;

  // Show write counter bar in live mode
  const counterBar = document.getElementById("write-counter-bar");
  if (counterBar && _LIVE) counterBar.classList.remove("hidden");
}

// ─────────────────────────────────────────
// Log panel helpers
// ─────────────────────────────────────────
function classifyLog(msg) {
  if (/leader|elected|wins|✓/i.test(msg))   return "highlight-green";
  if (/crash|fail|error|⚠/i.test(msg))      return "highlight-red";
  if (/lock|lease|ttl/i.test(msg))           return "highlight-yellow";
  if (/raft|vote|term|candidate/i.test(msg)) return "highlight-blue";
  if (/postgres|insert|update|commit/i.test(msg)) return "highlight-purple";
  return "";
}

function appendLog(msg, cls) {
  const el = document.getElementById("event-log");
  // Remove placeholder
  const placeholder = el.querySelector(".empty-cell");
  if (placeholder) placeholder.remove();

  const div = document.createElement("div");
  div.className = `log-entry ${cls || classifyLog(msg)}`;
  div.textContent = msg;
  el.appendChild(div);
  el.scrollTop = el.scrollHeight;

  logCount++;
  document.getElementById("log-count").textContent =
    `${logCount} entr${logCount === 1 ? "y" : "ies"}`;
}

function renderAllLogs(logs) {
  const el = document.getElementById("event-log");
  el.innerHTML = "";
  logCount = 0;
  logs.forEach(l => appendLog(l));
}

// ─────────────────────────────────────────
// Specific event handlers (visual effects)
// ─────────────────────────────────────────
function onRaftElection({ term }) {
  appendLog(`⚡ Raft election started – Term ${term}`, "highlight-blue");
  // Pulse sys1 box
  blinkBorder("sys1-box", "var(--orange)");
  blinkBorder("sys2-box", "var(--orange)");
}

function onRaftLeader({ leader, term }) {
  appendLog(`👑 Raft LEADER elected: ${leader} (Term ${term})`, "highlight-green");
  blinkBorder("sys1-box", "var(--green)");
}

function onLockAcquired({ winner, loser, lease_ttl }) {
  appendLog(`🔒 ${winner} acquired distributed lock (TTL ${lease_ttl}s)`, "highlight-yellow");
  appendLog(`   ${loser} is now PASSIVE`, "highlight-blue");

  const chip = document.getElementById(`chip-${winner}`);
  if (chip) {
    chip.classList.add("pulsing-green");
    setTimeout(() => chip.classList.remove("pulsing-green"), 4000);
  }

  showModal("🔒", "Distributed Lock Acquired",
    `${winner} won the etcd CAS race and holds the lock key with a ${lease_ttl}s lease. ` +
    `${loser} will monitor the key and wait for the lock to expire.`);
}

function onLockReleased({ previous_holder }) {
  appendLog(`🔓 Lock RELEASED — ${previous_holder} no longer the leader`, "highlight-orange");
  blinkBorder("lock-widget", "var(--orange)");
}

function onCrash({ crashed }) {
  appendLog(`💥 Crashed nodes: ${crashed.join(", ")}`, "highlight-red");
  document.getElementById("sys1-box").classList.add("crashed");
  blinkBorder("sys2-box", "var(--purple)");
}

function onCrashSignalled({ client_id, message }) {
  appendLog(`⚠️  ${message}`, "highlight-red");
  blinkBorder(`cc-${client_id}`, "var(--red)");

  const sysBox = client_id === "client-1" ? "sys1-box" : "sys2-box";
  document.getElementById(sysBox)?.classList.add("will-crash");

  showModal("💥", "Crash Signal Sent",
    `Signal delivered to ${client_id} via etcd. ` +
    `The process will call sys.exit() within seconds. ` +
    `The lock lease will expire in up to ${LEASE_TTL_MAX}s, then ${client_id === "client-1" ? "client-2" : "client-1"} will take over.`);

  // After TTL, remove crash styling
  setTimeout(() => {
    document.getElementById(sysBox)?.classList.remove("will-crash");
    document.getElementById(sysBox)?.classList.remove("crashed");
  }, (LEASE_TTL_MAX + 3) * 1000);
}

function onDbWrite(entry) {
  appendLog(`🐘 DB WRITE by ${entry.written_by}: ${entry.payload}`, "highlight-purple");
  flashElement("card-db");
}

function onComplete({ message }) {
  appendLog(`✅ ${message}`, "highlight-green");
  document.getElementById("sys1-box").classList.remove("crashed");
  setPhase({ phase: "done", step: 4, title: "Workflow Complete ✓" });
  // Mark all steps done
  for (let i = 1; i <= 4; i++) {
    document.getElementById(`ps-${i}`).className = "ps-step done";
  }
  document.querySelectorAll(".ps-line").forEach(l => l.classList.add("done"));
  showModal("✅", "Workflow Complete",
    "All four steps executed successfully. The system demonstrated self-healing: " +
    "after leader crash, the lock lease expired and client-2 took over as the new Application Leader.");
}

function onReset() {
  logCount = 0;
  document.getElementById("log-count").textContent = "0 entries";
  document.getElementById("event-log").innerHTML =
    '<span class="empty-cell">Waiting for events…</span>';
  document.getElementById("db-write-log").innerHTML =
    '<span class="empty-cell">No writes yet…</span>';
  document.getElementById("nodes-tbody").innerHTML =
    '<tr><td colspan="6" class="empty-cell">Waiting…</td></tr>';
  document.getElementById("sys1-box").classList.remove("crashed");
  // Reset progress
  for (let i = 1; i <= 4; i++) {
    document.getElementById(`ps-${i}`).className = "ps-step";
  }
  document.querySelectorAll(".ps-line").forEach(l => l.classList.remove("done"));
  setPhase({ phase: "idle", step: 0, title: 'Idle – Press "Run Workflow" to begin' });
}

// ─────────────────────────────────────────
// Connection status helpers
// ─────────────────────────────────────────
function setConnStatus(connected) {
  const badge = document.getElementById("conn-badge");
  if (!badge) return;
  if (connected) {
    badge.textContent = "● Connected";
    badge.className   = "conn-badge connected";
  } else {
    badge.textContent = "◌ Reconnecting…";
    badge.className   = "conn-badge disconnected";
  }
}

function onHeartbeat() { setConnStatus(true); }

// ─────────────────────────────────────────
// Crash leader controls
// ─────────────────────────────────────────
async function crashLeader(clientId) {
  const id = clientId || _currentLeaderId;
  if (!id) {
    alert("No active leader to crash.");
    return;
  }
  const btnCrash = document.getElementById("btn-crash");
  if (btnCrash) { btnCrash.disabled = true; btnCrash.textContent = "⏱ Sending signal…"; }

  try {
    const res = await fetch(`/api/crash/${id}`, { method: "POST" });
    const json = await res.json();
    if (json.status === "signal_sent" || json.status === "demo_crash_simulated") {
      appendLog(`⚠️  Crash signal sent to ${id}`, "highlight-red");
      // Visually mark the system box
      const sysBox = id === "client-1" ? "sys1-box" : "sys2-box";
      document.getElementById(sysBox)?.classList.add("will-crash");
    } else {
      appendLog(`[ERROR] Crash signal failed: ${JSON.stringify(json)}`, "highlight-red");
    }
  } catch (e) {
    appendLog(`[ERROR] Could not send crash signal: ${e}`, "highlight-red");
  } finally {
    setTimeout(() => {
      if (btnCrash) { btnCrash.disabled = false; }
    }, 5000);
  }
}
  const el = document.getElementById(id);
  if (!el) return;
  const orig = el.style.borderColor;
  el.style.transition = "border-color .1s";
  el.style.borderColor = colour;
  setTimeout(() => { el.style.borderColor = orig; }, 900);
}

function flashElement(id) {
  const el = document.getElementById(id);
  if (!el) return;
  el.style.transition = "border-color .1s";
  el.style.borderColor = "var(--purple)";
  setTimeout(() => { el.style.borderColor = ""; }, 700);
}

// ─────────────────────────────────────────
// Modal
// ─────────────────────────────────────────
let lastModal = null;

function showModal(icon, title, desc) {
  // Debounce – don't show same modal twice in a row
  const key = title;
  if (key === lastModal) return;
  lastModal = key;
  document.getElementById("modal-icon").textContent  = icon;
  document.getElementById("modal-title").textContent = title;
  document.getElementById("modal-desc").textContent  = desc;
  document.getElementById("step-modal").classList.remove("hidden");
}

function closeModal() {
  document.getElementById("step-modal").classList.add("hidden");
  lastModal = null;
}

// ─────────────────────────────────────────
// Controls
// ─────────────────────────────────────────
async function startWorkflow() {
  const btn = document.getElementById("btn-start");
  if (btn) btn.disabled = true;
  await fetch("/api/start", { method: "POST" });
  setTimeout(() => { if (btn) btn.disabled = false; }, 2000);
}

async function resetWorkflow() {
  await fetch("/api/reset", { method: "POST" });
}

// ─────────────────────────────────────────
// TTL bar + live state periodic refresh
// ─────────────────────────────────────────
function pollState() {
  fetch("/api/state")
    .then(r => r.json())
    .then(state => {
      if (state.lock)    renderLock(state.lock);
      if (state.clients) renderClients(state.clients);
      if (state.db && _LIVE) renderDB(state.db);
    })
    .catch(() => {});
}
setInterval(pollState, 2000);

// ─────────────────────────────────────────
// Boot
// ─────────────────────────────────────────
document.addEventListener("DOMContentLoaded", () => {
  connectSSE();
  // Load initial state
  fetch("/api/state").then(r => r.json()).then(applyState);
});
