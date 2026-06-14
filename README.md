# etcd Leader Election Workflow Visualiser

This project demonstrates **leader election and failover** in a distributed setup using:
- **etcd (Raft consensus)** for cluster leadership and distributed locking
- **Python clients** competing for a lock and performing critical writes
- **PostgreSQL** as the shared data store
- **Flask + frontend UI** to visualise system state and recovery behavior

## Project Analysis

### What the project does well
- Provides both a **demo mode** (fully in-memory simulation) and a **live mode** (real etcd + PostgreSQL).
- Shows the full lifecycle clearly: cluster init → election → lock ownership → database writes → crash → recovery.
- Uses practical distributed-system concepts:
  - CAS lock acquisition via etcd transactions
  - Lease-based lock expiry for automatic failover
  - Client presence registration and recovery signaling
- Includes a visual workflow that makes backend state easy to understand.

### Architecture Summary
- `Dockerfile.etcd`: Runs a 3-node etcd cluster in one container using goreman.
- `client.py`: Client process that contests `/db/critical_lock`, writes to DB when leader, and watches crash signals.
- `etcd_state.py`: Live-mode state reader that polls etcd + PostgreSQL and normalizes data for the UI.
- `simulation.py`: In-memory simulator with the same snapshot shape as live mode.
- `app.py`: Flask API + web server serving state and control endpoints.
- `templates/index.html`, `static/`: UI for cluster, lock, events, and DB activity.

### Operational Flow
1. etcd cluster starts and elects a Raft leader.
2. Clients attempt to acquire `/db/critical_lock`.
3. Winning client becomes application leader and writes critical DB operations.
4. Leader failure is triggered (manual or simulated).
5. Lease expires, lock is released automatically, and standby client takes over.

### Notable Design Choices
- **Single snapshot contract** across simulation and live modes keeps frontend logic simple.
- **Lease TTL + keepalive** provides a robust lock/failover mechanism.
- **Event queue + polling/SSE hooks** supports near-real-time UI updates.

## Running Locally

```bash
cp .env.example .env
docker compose up --build -d
```

Open: `http://localhost:8000`

To follow logs:
```bash
docker compose logs -f
```

## Repository Structure

```text
app.py                 Flask backend and API routes
client.py              etcd-backed distributed client
etcd_state.py          Live-state adapter for UI
simulation.py          In-memory workflow simulator
init.sql               PostgreSQL schema seed
docker-compose.yml     Full local stack
templates/             HTML templates
static/                Frontend JS/CSS
```
