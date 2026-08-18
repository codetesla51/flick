# flick

**Postgres-native feature flags, live-synced to flagd — no polling.**

flick stores feature flags in Postgres and streams every change to [flagd](https://flagd.dev) over its native gRPC sync protocol. Change a flag from your terminal or SQL and flagd clients see it within milliseconds — with **transactional guarantees** most flag systems don't have: the flag update and its change event commit atomically, so no change is ever lost or half-applied.

Your app never talks to flick. It talks to flagd, which evaluates from its own in-memory copy — sub-millisecond reads, no per-request database hits. flick is the *source*; flagd is the *evaluator*.

---

## Why flick

- **Flags are data.** They live in Postgres — SQL, joins, audit, backups, the same operational muscle you already have. No new platform, no vendor lock-in.
- **No polling anywhere.** Changes are pushed end-to-end via a **transactional outbox** + **logical replication (CDC)**. Write a flag and an outbox event in one transaction; a WAL consumer streams the event to connected flagd instances.
- **No missed updates.** A subscribe-first streaming design guarantees a new client gets the full snapshot *and* any changes that land while the snapshot is being built — nothing slips through the gap.
- **Survives restarts.** The replication slot resumes from its saved position and replays undelivered events (at-least-once), so changes made while flick is down still reach flagd when it comes back.
- **Small and testable.** One modular library (`package flick`) + one CLI entrypoint, with `-race`-clean tests against a real Postgres.

---

## How it works

```mermaid
flowchart LR
    CLI["flick CLI / SetFlag"] -->|"flag + outbox event (1 tx)"| DB[("Postgres<br/>flags + outbox")]
    DB -->|"logical replication (WAL)"| Phylax["phylax (CDC)"]
    Phylax -->|"delivers outbox rows"| Hub{{"Hub<br/>pub/sub"}}
    Hub -->|"broadcasts deltas"| Sync["SyncService<br/>gRPC :8015<br/>flagd.sync.v1"]
    Sync <-->|"snapshot + live stream"| Flagd["flagd"]
    Flagd -->|"evaluates flags"| Users["Your app / users"]
    style DB fill:#2d4,color:#111
    style Flagd fill:#49c,color:#fff
```

**Division of labor:** flick *stores and pushes* config; flagd *evaluates* it. flick never answers "what should this user see?" — only "here's the config."

### The write path (per flag change)

1. `SetFlag` (or `flick set`) upserts the `flags` row and appends an outbox event — **one transaction, atomic commit**.
2. phylax picks the event up off the WAL via logical replication, acks it (`delivered_at`), and publishes it to the Hub.
3. Every connected `SyncFlags` stream merges the delta into its in-memory state and resends the full config to flagd.

### The read path (per evaluation)

Your app asks flagd → flagd reads **its own in-memory copy** → answer. Zero network, zero database. flick is only involved when something *changes*.

### Why subscribe-first?

```mermaid
sequenceDiagram
    participant C as Client (flagd)
    participant S as SyncFlags
    participant H as Hub
    participant DB as Postgres
    C->>S: connect (SyncFlags)
    S->>H: 1. subscribe — tune in NOW
    Note over S,DB: 2. build snapshot (slow DB read)
    H--)S: changes arriving here get buffered (not missed)
    S->>C: 3. send full snapshot
    S->>C: 4. flush buffered deltas (in order)
    H--)S: 5. live deltas forwarded instantly
```

Subscribe **before** the snapshot work: nothing between "started listening" and "snapshot sent" is lost. The Hub uses buffered, drop-on-full channels — a slow subscriber can never stall the WAL consumer; a dropped delta self-heals on reconnect with a fresh snapshot.

---

## Quick start

### 1. Postgres with logical replication

```sh
docker run -d --name flick-pg \
  -e POSTGRES_USER=flick -e POSTGRES_PASSWORD=flick -e POSTGRES_DB=flick \
  -p 5432:5432 postgres:16

docker exec flick-pg psql -U flick -d flick -c "ALTER SYSTEM SET wal_level = 'logical'"
docker restart flick-pg
```

### 2. Set up and run flick

```sh
go run ./cmd/flick init      # migrations + wal_level check
go run ./cmd/flick serve     # sync gRPC server on :8015
```

DSN resolution: `--dsn` flag → `FLICK_DSN` env → `postgres://user:pass@localhost:5432/flick?sslmode=disable`.

### 3. Point flagd at it

```sh
flagd start --sources '[{"uri":"localhost:8015","provider":"grpc"}]'
```

flagd connects to flick as a gRPC sync source, receives the full config, and stays live-synced. Its evaluation API is on `:8013`.

### 4. Manage flags from the terminal

```sh
flick set show-banner --default-variant on --variants '{"on":true,"off":false}'
flick get show-banner
flick list
flick delete show-banner
```

flagd clients see each change within milliseconds — no restart, no reload.

---

## CLI reference

| Command | What it does |
|---|---|
| `flick init` | Apply migrations + verify `wal_level=logical` readiness |
| `flick serve` | Run the flagd sync gRPC server (`--addr`, env `FLICK_SYNC_ADDR`, default `:8015`) |
| `flick set <key>` | Create/update a flag: `--state` (default `ENABLED`), `--default-variant`, `--variants`, `--targeting`, `--metadata` (JSON) |
| `flick get <key>` | Show one flag's full detail |
| `flick list` | Table of all flags + pending outbox count |
| `flick delete <key>` | Delete a flag (absent keys are an error) |
| `flick version` | Print version (settable via `-ldflags "-X main.version=..."`) |

All database commands accept the global `--dsn`.

## Using it from your app (any language)

Your app uses the standard [OpenFeature](https://openfeature.dev) SDK with the flagd provider — no flick-specific code. Go example:

```go
import (
    "github.com/open-feature/go-sdk/openfeature"
    flagd "github.com/open-feature/go-sdk-contrib/providers/flagd/pkg"
)

provider, _ := flagd.NewProvider() // connects to flagd evaluation on :8013
openfeature.SetProvider(provider)
client := openfeature.NewClient("my-app")

enabled, err := client.BooleanValue(
    ctx, "show-banner", false,
    openfeature.NewEvaluationContext("user-1", map[string]any{"country": "NG"}),
)
```

## Live metrics & console

`flick serve` also runs an HTTP server (`--metrics-addr`, env `FLICK_METRICS_ADDR`, default `:8016`) with:

- `/metrics/stream` — SSE: an in-memory metrics snapshot every second (`changes_processed`, `changes_dropped`, `subscribers`, `replication_lag_bytes`, `outbox_delivered`, `outbox_inflight`, `outbox_failed`) — no DB access
- `/events` — SSE: every decoded WAL change, live
- `/dashboard` — the embedded Phylax Console

```sh
curl -sN localhost:8016/metrics/stream   # one JSON frame per second
```

## Serve / sync guarantees

- **One contract:** every flag change goes through the outbox pair (`SetFlag` / `DeleteFlag` / `flick set`). Bare `UPDATE flags` SQL is visible only after a client reconnects.
- **At-least-once:** the replication slot replays undelivered events across restarts — consumers must be idempotent.
- **Ordered per topic:** outbox events within a topic are delivered strictly in order, so a rapid on/off toggle always converges on the *final* value, never a stale intermediate.
- **Self-healing:** drop-on-full deltas resolve on reconnect; hard server crashes trigger flagd's own retry + resync.

## Scope

**In scope:** Postgres-backed flag storage with transactional eventing, live gRPC sync to flagd, CLI management, delete support, e2e-tested against a real database.

**Out of scope (deliberately):** flag *evaluation* (that's flagd's job), targeting rule authoring, auth/multi-tenancy, a web console, and dead-lettering of dropped deltas. The delivery handler currently logs and fans out; the wire format is flagd's full-config-per-message semantics (not minimal diffs).

## Layout

```
cmd/flick/          CLI: init, serve, set, get, list, delete, version
flags.go            SetFlag / DeleteFlag / TranslateFlag / ApplyDelta / snapshot builders
outbox.go           phylax wiring; delivery handler → Hub
hub.go              pub/sub: Subscribe / Unsubscribe / Publish (drop-on-full)
sync.go             SyncService: FetchAllFlags + SyncFlags (subscribe-first)
migrate.go          embedded goose migrations
db/goose_migrations/  flags + outbox DDL
gen/flagd/sync/v1/  generated flagd.sync.v1 Go bindings
```

## Development

```sh
go test -race ./...     # unit + e2e (needs the Postgres from quick start)
go vet ./...
```

Verified end-to-end against real flagd v0.15 and the OpenFeature Go SDK: live updates, 100-toggle ordering convergence, and kill/restart reconnect with WAL replay.