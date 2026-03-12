/* ═══════════════════════════════════════════════════════
   etcd Leader Election Visualiser – Frontend Logic
   ═══════════════════════════════════════════════════════ */

"use strict";

// LIVE_MODE is injected by Flask/Jinja2 in index.html as a const
// If the page was loaded without template rendering, default to false
const _LIVE = (typeof LIVE_MODE !== "undefined") ? LIVE_MODE : false;

// ─────────────────────────────────────────
const LEASE_TTL_MAX = 15;  // sync with LEASE_TTL in client.py

let evtSource    = null;
let logCount     = 0;
let _currentLeaderId = null;   // tracks who holds the lock right now
let _totalWrites     = 0;
let _sseRetries      = 0;
let _pollTimer       = null;
let _lastLeader      = null;   // detect leader changes between polls
let _lastNodes       = {};     // latest etcd node state snapshot
let _hbCounts        = {};     // nodeId → heartbeats received/sent
let _selectedAlgorithm = "raft";
let _selectedUsecase   = "ecommerce_order";

// ─────────────────────────────────────────
// Polling (replaces SSE – Railway CDN buffers SSE streams)
// ─────────────────────────────────────────
function connectSSE() {
  // Use polling instead of SSE so Railway's Fastly CDN doesn't buffer events.
  // Poll /api/state every 2s; synthesise events from state diffs.
  if (_pollTimer) clearInterval(_pollTimer);
  pollOnce();  // immediate first fetch
  _pollTimer = setInterval(pollOnce, 2000);
}

function pollOnce() {
  fetch("/api/state")
    .then(r => { if (!r.ok) throw new Error(r.status); return r.json(); })
    .then(data => {
      setConnStatus(true);
      _sseRetries = 0;
      handleEvent({ type: "state_update", ...data });

      // Synthesise discrete events from state changes so the log stays lively
      const newLeader = data.lock && data.lock.held_by ? data.lock.held_by : null;
      if (newLeader !== _lastLeader) {
        if (newLeader) {
          appendLog(`Lock ACQUIRED by ${newLeader}`, "highlight-green");
          handleEvent({ type: "lock_acquired", client_id: newLeader });
        } else if (_lastLeader) {
          appendLog(`Lock RELEASED by ${_lastLeader}`, "highlight-orange");
          handleEvent({ type: "lock_released", client_id: _lastLeader });
        }
        _lastLeader = newLeader;
      }
    })
    .catch(() => {
      _sseRetries++;
      setConnStatus(false);
      if (_sseRetries === 1) appendLog("[Poll] Connection error – retrying…", "highlight-red");
    });
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
    case "raft_replicate":      onRaftReplicate(data);    break;
    case "raft_heartbeat":      onRaftHeartbeat(data);    break;
    case "bully_election":      onBullyElection(data);    break;
    case "bully_message":       onBullyMessage(data);     break;
    case "bully_leader_elected":onBullyLeader(data);      break;
    case "workflow_complete":   onComplete(data);         break;
    case "reset":               onReset();                break;
  }
}

// ─────────────────────────────────────────
// Phase banner & progress steps
// ─────────────────────────────────────────
const phaseIcons = {
  idle:        "◎",
  init:        "⚙",
  election:    "⚡",
  replication: "📋",
  heartbeat:   "💓",
  bully:       "👑",
  lock:        "🔒",
  db_write:    "🐘",
  failure:     "⚠",
  recovery:    "♻",
  done:        "✓",
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
  if (state.algorithm) syncAlgorithmUI(state.algorithm);
  if (state.usecase)   syncUsecaseUI(state.usecase);
  // Lock control panel while simulation is running
  const cp = document.getElementById("control-panel");
  if (cp) cp.classList.toggle("locked", state.running === true);
}

const phaseTitleMap = {
  idle:        "Idle – Press \"Run Workflow\" to begin",
  init:        "Step 1 – Cluster Initialisation",
  election:    "Step 1 – Leader Election",
  replication: "Step 1 – Raft Log Replication (AppendEntries)",
  heartbeat:   "Step 1 – Raft Heartbeat Monitoring",
  bully:       "Step 1 – Bully Algorithm Election",
  lock:        "Step 2 – Application Leader Election (Distributed Lock)",
  db_write:    "Step 3 – Critical Database Operation",
  failure:     "Step 4 – Failure Scenario",
  recovery:    "Step 4 – Recovery & New Leader Election",
  done:        "Workflow Complete ✓",
};

// ─────────────────────────────────────────
// etcd Nodes table + arch chips
// ─────────────────────────────────────────
function renderNodes(nodes) {
  const tbody = document.getElementById("nodes-tbody");
  tbody.innerHTML = "";

  _lastNodes = nodes;

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
    // Expected formats:
    //  Live:  "[04:14:19] client-1 → order_insert: qty=12 sku=SKU-409 total=$1079.88"
    //  Sim:   "[04:14:19] client-1 → order_insert  INSERT INTO orders... -- total=$1079.88"
    const parts = line.match(/^\[(\S+)\]\s+(\S+)\s+→\s+(.+)$/);
    if (!parts) return `<div class="sql-entry"><div class="sql-body">${line}</div></div>`;

    const full     = parts[3].trim();
    const colonIdx = full.indexOf(": ");
    let opName, detail, comment;

    if (colonIdx > 0 && colonIdx < 30) {
      // Live mode: "op_name: detail"
      opName  = full.slice(0, colonIdx);
      detail  = full.slice(colonIdx + 2);
      comment = "";
    } else {
      // Sim mode: "op_name  SQL text -- number comment"
      const spaceIdx = full.search(/\s/);
      opName  = spaceIdx > 0 ? full.slice(0, spaceIdx) : full;
      const rest     = full.slice(opName.length).trim();
      const cmtMatch = rest.match(/--\s*(.+)$/);
      comment = cmtMatch ? cmtMatch[1].trim() : "";
      detail  = rest.replace(/\s*--.*$/, "").trim();
    }

    return `<div class="sql-entry">
      <div class="sql-header">
        <span class="sql-ts">${parts[1]}</span>
        <span class="sql-writer">${parts[2]}</span>
        <span class="sql-cmd">${opName}</span>
      </div>
      ${detail  ? `<div class="sql-body">${detail}</div>`   : ""}
      ${comment ? `<div class="sql-comment">${comment}</div>` : ""}
    </div>`;
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
  blinkBorder("sys1-box", "var(--orange)");
  blinkBorder("sys2-box", "var(--orange)");
}

function onRaftLeader({ leader, term }) {
  appendLog(`👑 Raft LEADER elected: ${leader} (Term ${term})`, "highlight-green");
  blinkBorder("sys1-box", "var(--green)");
}

// ─────────────────────────────────────────
// Bully Algorithm
// ─────────────────────────────────────────
function onBullyElection({ term, nodes }) {
  appendLog(`👑 Bully election started – Term ${term}. Nodes: ${nodes.join(', ')}`, "highlight-blue");
  blinkBorder("sys1-box", "var(--orange)");
  blinkBorder("sys2-box", "var(--orange)");
}

function onBullyMessage({ from, to, msg_type }) {
  const el = document.getElementById("bully-messages");
  if (!el) return;
  const placeholder = el.querySelector(".empty-cell");
  if (placeholder) placeholder.remove();

  const typeClass = msg_type.toLowerCase();
  const div = document.createElement("div");
  div.className = "bully-msg";
  div.innerHTML = `
    <span class="bully-from">${from}</span>
    <span class="bully-arrow">&rarr;</span>
    <span class="bully-to">${to}</span>
    <span class="bully-msg-type ${typeClass}">${msg_type}</span>
  `;
  el.appendChild(div);
  el.scrollTop = el.scrollHeight;
  appendLog(`👑 Bully: ${from} → ${to} [${msg_type}]`, "highlight-blue");
  flashElement("card-bully");
}

function onBullyLeader({ leader, term, priority }) {
  appendLog(`👑 Bully LEADER elected: ${leader} (Priority ${priority}, Term ${term})`, "highlight-green");
  blinkBorder("sys1-box", "var(--green)");
  blinkBorder("sys2-box", "var(--green)");
  showModal("👑", "Bully Algorithm – Leader Elected",
    `${leader} has the highest priority (P${priority}) and broadcasts COORDINATOR to all nodes in Term ${term}.`);
}

// ─────────────────────────────────────────
// Algorithm & Use Case selectors
// ─────────────────────────────────────────
function setAlgorithm(algo) {
  _selectedAlgorithm = algo;
  syncAlgorithmUI(algo);
}

function syncAlgorithmUI(algo) {
  _selectedAlgorithm = algo;
  document.querySelectorAll(".algo-btn").forEach(btn => {
    btn.classList.toggle("active", btn.id === `algo-${algo}`);
  });
  // Show/hide Raft-specific panels
  const showRaft = algo === "raft";
  const cardRepl = document.getElementById("card-replication");
  const cardHb   = document.getElementById("card-heartbeat");
  const cardBully = document.getElementById("card-bully");
  if (cardRepl)  cardRepl.classList.toggle("hidden", !showRaft);
  if (cardHb)    cardHb.classList.toggle("hidden",   !showRaft);
  if (cardBully) cardBully.classList.toggle("hidden", showRaft);
}

function setUsecase(id) {
  _selectedUsecase = id;
  syncUsecaseUI(id);
}

function syncUsecaseUI(id) {
  _selectedUsecase = id;
  document.querySelectorAll(".uc-card").forEach(card => {
    card.classList.toggle("selected", card.dataset.id === id);
  });
}

async function loadUsecases() {
  const grid = document.getElementById("usecase-grid");
  if (!grid) return;
  try {
    const res  = await fetch("/api/usecases");
    const data = await res.json();
    grid.innerHTML = "";
    Object.values(data).forEach(uc => {
      const btn = document.createElement("button");
      btn.className   = `uc-card${uc.id === _selectedUsecase ? " selected" : ""}`;
      btn.dataset.id  = uc.id;
      btn.onclick     = () => setUsecase(uc.id);
      btn.innerHTML   = `<span class="uc-icon">${uc.icon}</span><span class="uc-name">${uc.name}</span>`;
      btn.title       = uc.description;
      grid.appendChild(btn);
    });
  } catch(e) {
    grid.textContent = "(could not load use cases)";
  }
}

// ─────────────────────────────────────────
// Raft Log Replication
// ─────────────────────────────────────────
function onRaftReplicate({ leader, entry, index, phase, acks, committed, nodes }) {
  const el = document.getElementById("repl-log");
  if (!el) return;
  const placeholder = el.querySelector(".empty-cell");
  if (placeholder) placeholder.remove();

  const entryId = `repl-entry-${index}`;
  let entryEl = document.getElementById(entryId);
  if (!entryEl) {
    entryEl = document.createElement("div");
    entryEl.id        = entryId;
    entryEl.className = "repl-entry";
    el.appendChild(entryEl);
  }

  const allNodes = (nodes && nodes.length) ? nodes :
    Object.keys(_lastNodes).filter(k => k.startsWith("etcd"));

  const acksHtml = allNodes.map(nid => {
    const isLeader = nid === leader;
    const hasAck   = isLeader || acks.includes(nid);
    return `<span class="repl-ack ${hasAck ? "acked" : "pending"}" title="${nid}">${nid.replace("etcd-", "e")}</span>`;
  }).join("");

  entryEl.innerHTML = `
    <span class="repl-idx">#${index}</span>
    <code class="repl-key">${entry}</code>
    <span class="repl-acks">${acksHtml}</span>
    <span class="repl-status ${committed ? "committed" : "pending"}">${committed ? "✓ COMMITTED" : "⏳ PENDING"}</span>
  `;

  if (committed) {
    entryEl.classList.add("is-committed");
    appendLog(`✓ Log entry #${index} committed (majority quorum reached)`, "highlight-green");
  }
  el.scrollTop = el.scrollHeight;
  flashElement("card-replication");
}

// ─────────────────────────────────────────
// Raft Heartbeat Monitor
// ─────────────────────────────────────────
function onRaftHeartbeat({ leader, term, round, nodes }) {
  const el = document.getElementById("hb-nodes");
  if (!el) return;
  const placeholder = el.querySelector(".empty-cell");
  if (placeholder) placeholder.remove();

  appendLog(`💓 Heartbeat #${round}: ${leader} → AppendEntries(empty) → followers`, "highlight-blue");

  nodes.forEach(nodeId => {
    _hbCounts[nodeId] = (_hbCounts[nodeId] || 0) + 1;
    const count    = _hbCounts[nodeId];
    const isLeader = nodeId === leader;

    let rowEl = document.getElementById(`hb-${nodeId}`);
    if (!rowEl) {
      rowEl = document.createElement("div");
      rowEl.id        = `hb-${nodeId}`;
      rowEl.className = "hb-row";
      el.appendChild(rowEl);
    }

    // Build beat track – last 5 beats, fading older ones
    const beats  = Math.min(count, 5);
    let beatHtml = "";
    for (let i = 0; i < beats; i++) {
      const age   = beats - 1 - i;
      const fresh = i === beats - 1;
      beatHtml += `<span class="hb-beat${fresh ? " hb-fresh" : ""}" style="opacity:${Math.max(0.15, 1 - age * 0.2)}">${fresh ? "♥" : "·"}</span>`;
    }

    rowEl.innerHTML = `
      <span class="hb-node-name">${nodeId}</span>
      <span class="hb-role-badge ${isLeader ? "hb-leader" : "hb-follower"}">${isLeader ? "LEADER" : "FOLLOWER"}</span>
      <span class="hb-pulse-track">${beatHtml}</span>
      <span class="hb-count">${isLeader ? `sent&nbsp;${count}` : `rcvd&nbsp;${count}`}</span>
    `;

    // Trigger the row glow animation
    rowEl.classList.remove("hb-active");
    void rowEl.offsetWidth;  // force reflow
    rowEl.classList.add("hb-active");
  });

  flashElement("card-heartbeat");
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
  _hbCounts = {};
  document.getElementById("log-count").textContent = "0 entries";
  document.getElementById("event-log").innerHTML =
    '<span class="empty-cell">Waiting for events…</span>';
  document.getElementById("db-write-log").innerHTML =
    '<span class="empty-cell">No writes yet…</span>';
  document.getElementById("nodes-tbody").innerHTML =
    '<tr><td colspan="6" class="empty-cell">Waiting…</td></tr>';
  document.getElementById("repl-log").innerHTML =
    '<span class="empty-cell">Waiting for election…</span>';
  document.getElementById("hb-nodes").innerHTML =
    '<span class="empty-cell">Waiting for election…</span>';
  const bullyEl = document.getElementById("bully-messages");
  if (bullyEl) bullyEl.innerHTML = '<span class="empty-cell">Waiting for election…</span>';
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

function blinkBorder(id, colour) {
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
  await fetch("/api/start", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ algorithm: _selectedAlgorithm, usecase: _selectedUsecase })
  });
  setTimeout(() => { if (btn) btn.disabled = false; }, 2000);
}

async function resetWorkflow() {
  await fetch("/api/reset", { method: "POST" });
}

/**
 * Trigger Step 4 (Failure & Recovery) from the header button.
 * Works whether or not a client currently holds the lock.
 */
async function triggerFailure() {
  const btn = document.getElementById("btn-trigger-failure");
  if (btn) { btn.disabled = true; btn.textContent = "⏱ Triggering…"; }

  try {
    const res  = await fetch("/api/trigger_failure", { method: "POST" });
    const json = await res.json();
    if (json.status === "noop") {
      alert(json.reason || "Start the simulation first (▶ Run Workflow).");
    } else {
      const target = json.target || json.crashed || "leader";
      appendLog(`⚡ Step 4 triggered — crashing ${target}`, "highlight-red");
      const sysBox = target === "client-1" ? "sys1-box" : "sys2-box";
      document.getElementById(sysBox)?.classList.add("will-crash");
      showModal("⚡", "Step 4 – Failure Triggered",
        `Crash signal sent to ${target}. The etcd lease will expire in up to ${LEASE_TTL_MAX}s, ` +
        `then the other client will acquire the lock and become the new Application Leader.`);
    }
  } catch (e) {
    appendLog(`[ERROR] trigger_failure: ${e}`, "highlight-red");
  } finally {
    setTimeout(() => {
      if (btn) { btn.disabled = false; btn.textContent = "⚡ Step 4: Crash Leader"; }
    }, 8000);
  }
}

// ─────────────────────────────────────────
// TTL bar + live state periodic refresh
// ─────────────────────────────────────────
function pollState() {
  fetch("/api/state")
    .then(r => r.json())
    .then(state => {
      setConnStatus(true);
      if (state.lock)    renderLock(state.lock);
      if (state.clients) renderClients(state.clients);
      if (state.db && _LIVE) renderDB(state.db);
    })
    .catch(() => { setConnStatus(false); });
}
setInterval(pollState, 2000);

// ─────────────────────────────────────────
// Boot – script is at bottom of <body>, DOM is already ready
// ─────────────────────────────────────────
connectSSE();
loadUsecases();
fetch("/api/state").then(r => r.json()).then(applyState).catch(() => {});
