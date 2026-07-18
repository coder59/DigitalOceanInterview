# go-ingestion-api

High-throughput prompt ingestion service with a **control plane / data plane** split.  
Clients upload a JSON array of prompts (up to 1000). The control plane acks immediately; the data plane rate-limits calls to a mock external API (base64 "inference"), retries on HTTP 429 with a leaky bucket + retry queue, and persists successful results to PostgreSQL as both rows and a compiled JSON document.

## Table of contents

- [Architecture overview](#architecture-overview)
- [Component map](#component-map)
- [Request lifecycle](#request-lifecycle)
- [Concurrency model](#concurrency-model)
- [Rate limiting & 429 backoff](#rate-limiting--429-backoff)
- [Persistence model](#persistence-model)
- [API](#api)
- [Public BASE_URL & testing](#public-base_url--testing)
- [Project layout](#project-layout)
- [Local development](#local-development)
- [Docker Compose](#docker-compose)
- [DigitalOcean deploy (GitHub Actions)](#digitalocean-deploy-github-actions)

---

## Architecture overview

```
+------------+     +-----------+     +--------------------------------------------------+
| HTTP Client|---->|   Nginx   |---->|         Single API process (:8080)               |
|   / curl   |     |  :80      |     |                                                  |
+------------+     +-----------+     |  +--------------------------------------------+  |
                                     |  | CONTROL PLANE (internal/controlplane)      |  |
                                     |  |                                            |  |
                                     |  |  Gin HTTP --> Validate --> Batch Tracker   |  |
                                     |  |                   |                        |  |
                                     |  |                   v                        |  |
                                     |  |              Dispatcher                    |  |
                                     |  +-------------------+------------------------+  |
                                     |                      | Dispatch WorkItem         |
                                     |                      v                           |
                                     |  +--------------------------------------------+  |
                                     |  | BOUNDARY (internal/plane)                  |  |
                                     |  |  MemoryQueue [buffered chan, cap 10000]    |  |
                                     |  +-------------------+------------------------+  |
                                     |                      |                           |
                                     |                      v                           |
                                     |  +--------------------------------------------+  |
                                     |  | DATA PLANE (internal/dataplane)            |  |
                                     |  |                                            |  |
                                     |  |  Workers x5 --> Leaky Bucket --> Mock API  |  |
                                     |  |      |         (burst 8 / leak 4/s)        |  |
                                     |  |      |                |                    |  |
                                     |  |      |         +------+------+             |  |
                                     |  |      |         |             |             |  |
                                     |  |      |       200 OK      429 / err         |  |
                                     |  |      |         |             |             |  |
                                     |  |      |         v             v             |  |
                                     |  |      |      Persist     Retry Queue        |  |
                                     |  |      |                      |              |  |
                                     |  |      |                      v              |  |
                                     |  |      |             Retry Dispatcher        |  |
                                     |  |      |           (sleep -> re-enqueue)     |  |
                                     |  +------+----------------------+------------+  |
                                     |         |                      |                 |
                                     |         v                      +--> job queue    |
                                     |  +--------------+                                |
                                     |  |  PostgreSQL  |  prompts + compilations      |
                                     |  +--------------+                                |
                                     +--------------------------------------------------+
```

### Plane responsibilities

| Plane | Owns | Does **not** |
|-------|------|----------------|
| **Control plane** | HTTP, validation, batch IDs, status APIs, dispatch | Inference, retries, DB writes of results |
| **Data plane** | Workers, leaky bucket, retry queue, mock API, save | Accepting public HTTP ingest |
| **plane package** | `WorkItem`, `MemoryQueue`, `Dispatcher` interface | Business logic |

---

## Component map

```
                         +-------------------+
                         |  cmd/api/main.go  |
                         |  wires CP + DP    |
                         +---------+---------+
                                   |
           +-----------+-----------+-----------+-----------+-----------+
           |           |           |           |           |           |
           v           v           v           v           v           v
    +------------+ +-------+ +-----------+ +----------+ +----+ +---------+
    |controlplane| | plane | | dataplane | | external | | db | |  batch  |
    +-----+------+ +---+---+ +-----+-----+ +----+-----+ +--+-+ +----+----+
          |            |           |            |          |        |
          |            |           |            |          |        |
          +------>-----+----->-----+----->------+          |        |
          |            |           |                       |        |
          |            |           +----------------------->        |
          |            |                                   |        |
          +------------+-----------------------------------+------->+
                       |
                       CP --> plane --> DP --> external / db
                       CP / DP --> batch tracker
```

| Package | Role |
|---------|------|
| `internal/controlplane` | Gin routes, parse prompts, ack `202`, async dispatch |
| `internal/plane` | Shared `WorkItem` + in-process work queue |
| `internal/dataplane` | Worker pool, leaky bucket, retry queue |
| `internal/external` | Mock inference API (base64, fixed-window 429) |
| `internal/db` | GORM Postgres: per-prompt rows + compiled JSON |
| `internal/batch` | In-memory batch progress for polling |

---

## Request lifecycle

### Sequence (ingest + poll)

```
 Client          Nginx         Control Plane      MemoryQueue     Data Plane      Mock API / DB
   |               |                 |                 |               |                |
   | POST /ingest  |                 |                 |               |                |
   |-------------->|---------------->|                 |               |                |
   |               |                 | validate <=1000 |               |                |
   |               |                 | create batch_id |               |                |
   | 202 accepted  |                 |                 |               |                |
   | {batch_id}    |                 |                 |               |                |
   |<--------------|<----------------|                 |               |                |
   |               |                 |                 |               |                |
   |               |                 | Dispatch items  |               |                |
   |               |                 | (background)    |               |                |
   |               |                 |---------------->|               |                |
   |               |                 |                 |  pull job     |                |
   |               |                 |                 |-------------->|                |
   |               |                 |                 |               | Wait(token)    |
   |               |                 |                 |               | Process()      |
   |               |                 |                 |               |--------------->|
   |               |                 |                 |               |                |
   |               |                 |                 |        +------+------+         |
   |               |                 |                 |        |             |         |
   |               |                 |                 |     200 OK       429/err       |
   |               |                 |                 |        |             |         |
   |               |                 |                 |        v             v         |
   |               |                 |                 |     save DB    retry queue     |
   |               |                 |                 |                   |            |
   |               |                 |                 |                   v sleep      |
   |               |                 |                 |<---------- re-enqueue          |
   |               |                 |                 |               |                |
   | GET /batches/id                 |                 |               |                |
   |-------------->|---------------->| status/results  |               |                |
   |<--------------|<----------------|<--------------------------------|---------------|
```

### Happy path (compressed)

```
  POST ingest --> validate --> 202 ack --> enqueue WorkItems
                                              |
                                              v
                                         worker pulls
                                              |
                                              v
                                      leaky bucket Wait
                                              |
                                              v
                                      mock API Process
                                              |
                         +--------------------+--------------------+
                         |                                         |
                         v                                         v
                    200 + base64                               429 / error
                         |                                         |
                         v                                         v
              SaveSuccessfulInferences                      scheduleRetry
              prompts + compiled JSON                     sleep --> re-enqueue
```

---

## Concurrency model

### Goroutine topology (default process)

```
  main
   |
   +-- net/http + Gin  (HTTP server)
   |        |
   |        +-- on each POST /ingest:
   |                 go dispatchBatch --sequential Enqueue--> job chan
   |
   +-- dataplane.Start(ctx)
            |
            +-- go worker x5  <--- pull ---  job chan
            |        |
            |        +-- on 429 / err --->  retry chan
            |
            +-- go retryDispatcher x1  <--- retry chan
                     |
                     +-- sleep until RetryAt ---> re-enqueue job chan
```

| Goroutine | Count | Blocking behavior |
|-----------|------:|-------------------|
| HTTP server | 1 (+ Go net/http pool) | Serves CP routes |
| `dispatchBatch` | 1 per ingest | Blocks on full job queue (never drops) |
| Workers | **5** | Pull jobs, batch up to **100** or flush every **2s** |
| Retry dispatcher | **1** | Sleeps per item, then re-queues |

### Shared state & synchronization

```
  +-----------------------------+     +--------------------------------+
  |  Lock-free (channels)       |     |  Mutex-protected               |
  |                             |     |                                |
  |  * job chan   (cap 10000)   |     |  * LeakyBucket.mu              |
  |  * retry chan (cap 10000)   |     |  * MockClient.mu               |
  |                             |     |  * Tracker.mu                  |
  +--------------+--------------+     |  * GormRepo.write (JSON merge) |
                 |                    +----------------+---------------+
                 |                                     |
                 v                                     v
        Workers / RetryDisp                   Workers / Saver / CP
```

**Guarantees**

- **No silent drops:** full job/retry channels **block** instead of discarding work.
- **Fair pacing:** all workers share one leaky bucket before calling the mock API.
- **Safe compile merges:** `GormRepo.write` mutex serializes appends into `inference_compilations` across workers.
- **Shutdown:** cancel stops accepting new worker loops; in-flight flush uses `context.Background()` so saves/retries are not abandoned mid-batch; retry queue is drained back onto the job queue.

### Worker flush concurrency

```
  job chan
     |
     v
  +----------------------------------------------+
  | Worker (local batch slice -- not shared)     |
  |                                              |
  |  1. receive WorkItem --> append              |
  |  2. flush when len >= 100  OR  ticker 2s     |
  |  3. for each item:                           |
  |        limiter.Wait --> api.Process          |
  |  4. successes --> saver([]WorkItem)          |
  |     failures  --> retry queue                |
  +----------------------------------------------+

  Workers run in parallel; one HTTP upload can be
  processed by several workers once on the job queue.
```

---

## Rate limiting & 429 backoff

```
                         +-------------+
                         | Dispatched  |<-----------------------------+
                         +------+------+                              |
                                | worker picks item                   |
                                v                                     |
                         +-------------+                              |
                         | TokenWait   |  leaky bucket                |
                         |             |  (burst 8 / leak 4/s)        |
                         +------+------+                              |
                                | token granted                       |
                                v                                     |
                         +-------------+                              |
                         |  CallAPI    |                              |
                         +------+------+                              |
                   +------------+------------+                        |
                   |            |            |                        |
                   v            v            v                        |
              +--------+   +--------+   +-----------+                 |
              | 200 OK |   |  429   |   | other err |                 |
              +---+----+   +---+----+   +-----+-----+                 |
                  |            |              |                       |
                  |            +------+-------+                       |
                  |                   v                               |
                  |            +-------------+                        |
                  |            | RetryPark   |  Attempts++            |
                  |            +------+------+                        |
                  |                   | RetryAt = now + delay         |
                  |                   v                               |
                  |            +-------------+                        |
                  |            |   Sleep     |                        |
                  |            +------+------+                        |
                  |                   | re-enqueue                    |
                  |                   +-------------------------------+
                  v
           +-------------+
           |  Persisted  |  prompts + inference_compilations
           +-------------+
```

### Leaky bucket (shared)

```
  tokens ########  capacity = 8 (burst)
           |
           |  leak / refill @ 4 tokens per second
           v
  worker Wait() -- consumes 1 token per API call
                   (blocks if empty until refill)
```

- Capacity (burst): **8** tokens  
- Leak rate: **4** tokens/second  
- Each API call consumes **1** token; if empty, worker blocks until refill  

### Mock external API

- Fixed window: **5** requests / second, then returns **429** with `Retry-After`  
- Successful body: `base64(prompt)`  

### Retry delay

Exponential backoff (capped), preferring a larger `Retry-After` from the mock API when present:

```
  delay = min(2s, 100ms * 2^(attempts-1))
  if Retry-After > delay --> use Retry-After
```

| Attempt | Exponential floor |
|--------:|-------------------|
| 1 | 100ms |
| 2 | 200ms |
| 3 | 400ms |
| 4 | 800ms |
| 5 | 1.6s |
| 6+ | capped at 2s |

---

## Persistence model

```
  successful flush (one or more WorkItems)
              |
              +------------------------------+
              |                              |
              v                              v
     INSERT prompts rows              MERGE inference_compilations
     (id, batch_id, prompt,           JSON per batch_id
      processed_b64, ...)
```

```json
{
  "batch_id": "...",
  "count": 2,
  "inferences": [
    {"id":"...","prompt":"hello","inference":"aGVsbG8=","attempts":0},
    {"id":"...","prompt":"world","inference":"d29ybGQ=","attempts":1}
  ],
  "updated_at": "..."
}
```

---

## API

| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/health` | Liveness (`plane: control`) |
| `POST` | `/api/v1/ingest` | Accept prompt array → `202` + `batch_id` |
| `GET` | `/api/v1/ingest/batches/:batch_id` | Poll batch status |
| `GET` | `/api/v1/ingest/batches/:batch_id/results` | Compiled inference JSON |
| `GET` | `/api/v1/pool` | Job / retry queue depths (`plane: data`) |

**Ingest body examples**

```json
[{"prompt":"hello"},{"prompt":"world"}]
```

```json
["hello","world"]
```

```json
{"prompts":[{"prompt":"hello","id":"550e8400-e29b-41d4-a716-446655440000"}]}
```

**Immediate ack**

```json
{"status":"accepted","batch_id":"...","total":2,"plane":"control"}
```

---

## Public BASE_URL & testing

Live DigitalOcean droplet (nginx on port 80):

```bash
BASE_URL=http://157.245.246.228
```

| | |
|--|--|
| Public IP | `157.245.246.228` |
| Private IP | `10.116.0.2` (VPC only; not used by clients) |
| Edge | Nginx → API `:8080` |

### Curl examples

```bash
# Health
curl -sS "$BASE_URL/health"

# Pool stats (job / retry queue depths)
curl -sS "$BASE_URL/api/v1/pool"

# Ingest prompts (immediate 202 + batch_id)
curl -sS -X POST "$BASE_URL/api/v1/ingest" \
  -H 'Content-Type: application/json' \
  -d '["hello","world"]'

# Same ingest as objects
curl -sS -X POST "$BASE_URL/api/v1/ingest" \
  -H 'Content-Type: application/json' \
  -d '[{"prompt":"hello"},{"prompt":"world"}]'

# Poll batch status (replace BATCH_ID)
curl -sS "$BASE_URL/api/v1/ingest/batches/BATCH_ID"

# Compiled results
curl -sS "$BASE_URL/api/v1/ingest/batches/BATCH_ID/results"
```

### Ingest → wait → results (one-liner)

```bash
BASE_URL=http://157.245.246.228
BID=$(curl -sS -X POST "$BASE_URL/api/v1/ingest" \
  -H 'Content-Type: application/json' \
  -d '["hello","world"]' | python3 -c 'import sys,json; print(json.load(sys.stdin)["batch_id"])')
echo "batch_id=$BID"
until curl -sS "$BASE_URL/api/v1/ingest/batches/$BID" | grep -q '"status":"completed"'; do sleep 1; done
curl -sS "$BASE_URL/api/v1/ingest/batches/$BID/results"; echo
```

Expected shapes:

- Health: `{"plane":"control","status":"healthy"}`
- Ingest: `{"status":"accepted","batch_id":"…","total":2,"plane":"control"}`
- Results: `{"batch_id":"…","count":2,"inferences":[…],"updated_at":"…"}`

---

## Project layout

- `cmd/api/main.go` — wires control plane + data plane + HTTP server
- `internal/controlplane/` — HTTP + dispatch only
- `internal/dataplane/` — workers, leaky bucket, retry queue
- `internal/plane/` — `WorkItem` + `MemoryQueue`
- `internal/external/` — mock API (base64 + 429)
- `internal/db/` — Postgres rows + compiled inference JSON
- `internal/batch/` — in-memory batch tracker
- `internal/e2e/` — embedded-Postgres end-to-end test
- `.github/workflows/ci.yml` — unit tests on PR/push
- `.github/workflows/deploy.yml` — SSH deploy to DigitalOcean droplet
- `docker-compose.yaml` — postgres + api + nginx
- `Dockerfile` / `nginx.conf` / `Makefile`
- `scripts/compose-smoke.sh` / `scripts/deploy-droplet.sh`

---

## Local development

```bash
make test          # unit tests
make test-e2e      # full API + embedded Postgres
make build-local   # ./bin/api-service
```

Requires `DATABASE_URL` (or default local Postgres URL in `main.go`) to run the binary outside e2e.

---

## Docker Compose

```
  +----------+     +-----------+     +----------+
  |  Client  |---->|  nginx:80 |---->| api:8080 |
  +----------+     +-----------+     |  CP + DP |
                                     +----+-----+
                                          |
                                          v
                                     +----------+
                                     |postgres:16|
                                     +----------+
```

```bash
make deploy   # docker compose up --build -d
make smoke    # health + ingest + results via nginx :80
make logs-api
make down
```

---

## DigitalOcean deploy (GitHub Actions)

```
  push to main
       |
       v
  +--------------------+
  | GitHub Actions     |
  | 1. test + build    |
  | 2. rsync over SSH  |
  | 3. deploy-droplet  |
  | 4. optional smoke  |
  +---------+----------+
            |
            v
  +--------------------+
  | DigitalOcean       |
  | droplet            |
  | docker compose up  |
  +--------------------+
```

On push to `main`, [`.github/workflows/deploy.yml`](.github/workflows/deploy.yml):

1. Run unit tests + build  
2. `rsync` project to the droplet over SSH  
3. Run `scripts/deploy-droplet.sh` → `docker compose up --build -d`  
4. Optional smoke against `DROPLET_URL`  

**Required GitHub Actions secrets**

| Secret | Meaning |
|--------|---------|
| `DROPLET_HOST` | Droplet IP / DNS |
| `DROPLET_USER` | SSH user (`root` or docker-capable user) |
| `DROPLET_SSH_KEY` | Private key for that user |

Optional: `DROPLET_URL`, `DROPLET_APP_DIR` (default `/opt/go-ingestion-api`), `DROPLET_PORT` (default `22`).

One-time droplet bootstrap:

```bash
make droplet-bootstrap
```

---

## Design principles

1. **Ack fast, process async** — CP never waits on inference.  
2. **Never drop prompts** — backpressure via blocking channels; retries park and return.  
3. **Clear plane boundary** — `Dispatcher` / `MemoryQueue` is the only CP→DP seam (ready to split into separate services later).  
4. **Observability** — batch polling + `/api/v1/pool` queue depths.
