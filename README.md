# flick

**Postgres-native feature flags, live-synced to flagd — no polling.**

[![CI](https://github.com/codetesla51/flick/actions/workflows/ci.yml/badge.svg)](https://github.com/codetesla51/flick/actions/workflows/ci.yml)
[![Go version](https://img.shields.io/github/go-mod/go-version/codetesla51/flick)](https://go.dev)
[![Release](https://img.shields.io/github/v/release/codetesla51/flick)](https://github.com/codetesla51/flick/releases)

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

## Install

```sh
go install github.com/codetesla51/flick/cmd/flick@latest
```

> [!NOTE]
> `@latest` resolves to the newest tagged release (see the Releases page for version history). To build from a checkout instead: `go run ./cmd/flick` (or `go build -o flick ./cmd/flick`).

---

## Quick start

> [!TIP]
> Any Postgres with `wal_level = logical` works. The examples below use a docker container; `FLICK_DSN` or `--dsn` points flick at whatever Postgres you have.

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
flick init      # migrations + replication probe (proves streaming works)
flick serve     # sync gRPC server on :8015, console on :8016
```

`flick init` doesn't just check settings — it runs a live end-to-end probe: it creates the `flick_slot` slot and `flick_pub` publication (the same ones serve uses), writes a probe row to the outbox table, and confirms the stream delivers it. If anything is broken (permissions, wal_level pending restart, another process holding the slot), it says exactly what and how to fix it.

DSN resolution: `--dsn` flag → `FLICK_DSN` env → `postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable`.

### 3. Point flagd at it

> [!NOTE]
> Don't have flagd yet? Download the latest binary from the [flagd releases](https://github.com/open-feature/flagd/releases) page (or use Docker: `docker pull ghcr.io/open-feature/flagd:latest`). Note that `go install github.com/open-feature/flagd/flagd@latest` is **not** supported — flagd's module depends on unpublished local workspace modules, so use a versioned release binary instead.

```sh
flagd start --sources '[{"uri":"localhost:8015","provider":"grpc"}]'
```

flagd connects to flick as a gRPC sync source, receives the full config, and stays live-synced. It opens its evaluation API on `:8013`.

> [!TIP]
> flagd's *own* sync service also defaults to port 8015. It's not needed when flick is the source, so if flagd complains the port is taken, relocate it: `flagd start --sync-port 18015 --sources '[{"uri":"localhost:8015","provider":"grpc"}]'`.

### 4. Connect your app

```go
provider, _ := flagd.NewProvider()          // flagd on localhost:8013
openfeature.SetProvider(provider)
client := openfeature.NewClient("my-app")

showBanner, _ := client.BooleanValue(ctx, "show-banner", false, evalCtx)
```

Your app talks to **flagd, not flick** — standard OpenFeature SDK with the flagd provider. Full examples in [Using it from your app](#using-it-from-your-app-any-language).

### 5. Manage flags from the terminal

```sh
flick set show-banner --default-variant on --variants '{"on":true,"off":false}'
flick get show-banner
flick list
flick delete show-banner
```

flagd clients see each change within milliseconds — no restart, no reload.

### What's running where

| Process | Opens | Purpose |
|---|---|---|
| `flick serve` | `:8015` | sync gRPC server — **flagd** connects here |
| `flick serve` | `:8016` | web console — **you** connect here |
| `flagd` | `:8013` | evaluation gRPC — **your app's SDK** connects here |

Three hops, two long-running processes: **flick** (source of truth) → **flagd** (in-memory evaluator) → **your app**. flick and your app never talk to each other — flagd sits between them, and flick is only involved when a flag *changes*.

---

## CLI reference

| Command | What it does |
|---|---|
| `flick init` | Apply migrations, verify `wal_level=logical`, run a live replication probe (slot + publication + probe event delivered end-to-end) |
| `flick serve` | Run the flagd sync gRPC server (`--addr`, env `FLICK_SYNC_ADDR`, default `:8015`) + console (`--metrics-addr`, env `FLICK_METRICS_ADDR`, default `:8016`) |
| `flick set <key>` | Create/update a flag: `--state` (default `ENABLED`), `--default-variant`, `--variants`, `--targeting`, `--metadata` (JSON) |
| `flick get <key>` | Show one flag's full detail |
| `flick list` | Table of all flags + pending outbox count |
| `flick delete <key>` | Delete a flag (absent keys are an error) |
| `flick export` | Export every flag as pretty-printed JSON (backups, migrations) |
| `flick import` | Import flags from a JSON array on stdin, as produced by `flick export` |
| `flick version` | Print version (settable via `-ldflags "-X main.version=..."`) |

All database commands accept the global `--dsn`.

## Targeting rules

`--targeting` stores arbitrary JSON and passes it through to flagd's flag definition unchanged — so it must be **flagd's targeting schema** (a [JsonLogic-style rule](https://flagd.dev/reference/flag-definitions/)), not a plain attribute map. Supported operators: `if`, `and`, `or`, `==`, `===`, `!=`, `in`, `var`, `fractional`, `starts_with`, `ends_with`, `sem_ver`. A rule whose top-level key isn't an operator evaluates falsy, and the flag silently falls back to its default variant.

Country targeting (only NG users get the `on` variant):

```json
{ "if": [ { "in": [ { "var": "country" }, ["NG"] ] }, "on", "off" ] }
```

20% rollout via the `fractional` operator — deterministic per user (bucketed on `targetingKey`), so the same user always gets the same variant:

```json
{ "fractional": [ { "var": "targetingKey" }, ["on", 20], ["off", 80] ] }
```

`fractional` weights can be any relative numbers; the A/B example in the demo data is a 50/30/20 split across `v2`/`v3`/`v1`.

The console's visual targeting editor ([see below](#console--live-metrics)) covers a practical operator subset — `in`, `nin`, `==`, `!=`, `exists`, `starts_with`, `ends_with`, `>`, `>=`, `<`, `<=` — plus a raw JSON fallback for anything more exotic.

## Console & live metrics

`flick serve` runs a web console at `:8016` — flag CRUD, targeting authoring, and live telemetry in one page, with light/dark themes. It's a single embedded HTML page; rebuild flick to pick up UI changes.

### What it does

- **Live stream** — every outbox event as it flows through (`#320 flags · checkout-v2 DISABLED`), live over SSE.
- **Metrics** — headline counters for Flags, Delivered, WAL lag, and Changes processed, plus subscribers / dropped / inflight / failed — fed by `/metrics/stream` (in-memory, no DB access).
- **Graphs** — 60-second rolling history of outbox delivered, changes processed, and WAL lag.
- **Flags ledger** — list, filter (enabled / disabled / targeted), search, and paginate (6 per page); edit, delete, toggle state, and switch a flag's default variant inline. Any change from any client — CLI, API, another browser — updates the list live via SSE.
- **Flag editor** — opens on **New flag** or a row's edit action:
  - **General** — key and state.
  - **Variants** — a JSON object of `variant → value` (strings, numbers, booleans, nested objects); invalid JSON is rejected inline.
  - **Targeting** — a visual editor for flagd JsonLogic targeting, three modes:
    - **Rules** — rows of `field → operator → values → variant`, compiled to `{"if":[…]}` (first match wins, else the default). Operators: `in`, `nin`, `==`, `!=`, `exists`, `starts_with`, `ends_with`, `>`, `>=`, `<`, `<=`.
    - **Split** — a percentage per variant, bucketed on `targetingKey` (deterministic per user); totals auto-normalize to exactly 100%.
    - **Raw JSON** — synced both ways with the rules/split views; paste any valid flagd targeting blob and the form repaints, or get an inline error.
  - **Metadata** — arbitrary JSON stored on the flag definition.

### HTTP API

| Endpoint | What it is |
|---|---|
| `/` | the console page (embedded `dashboard.html`) |
| `GET /api/flags` | list all flags |
| `POST /api/flags` | create/update a flag (full flag object; the console always sends the complete flag) |
| `DELETE /api/flags/{key}` | delete a flag |
| `/metrics/stream` | SSE: in-memory metrics snapshot every second (`changes_processed`, `changes_dropped`, `subscribers`, `replication_lag_bytes`, `outbox_delivered`, `outbox_inflight`, `outbox_failed`) — no DB access |
| `/events` | SSE: every decoded WAL change, live |

```sh
open http://localhost:8016          # the console
curl -sN localhost:8016/metrics/stream
```

**Keyboard shortcuts:** `/` focuses search · `Esc` closes the editor.

## Using it from your app (any language)

Your app talks to flagd through the standard [OpenFeature](https://openfeature.dev) SDK plus a flagd provider — the same pattern in every language: register the provider once, then evaluate a flag with its **key**, a **fallback**, and an **evaluation context** (user id, country, env…). flagd pushes updates to connected SDKs, so toggling a flag in flick reaches your app without a restart or reload.

| Language | Install |
|---|---|
| Go | `go get github.com/open-feature/go-sdk github.com/open-feature/go-sdk-contrib/providers/flagd/pkg` |
| Python | `pip install openfeature-provider-flagd` |
| Node.js / TS | `npm install @openfeature/server-sdk @openfeature/flagd-provider` |
| Java | `dev.openfeature.contrib.providers:flagd` (Maven) |
| .NET | `dotnet add package OpenFeature.Providers.Flagd` |
| Rust / PHP / Ruby / … | see [flagd providers](https://flagd.dev/providers/) |

### Go

```go
provider, _ := flagd.NewProvider() // connects to flagd evaluation on :8013
openfeature.SetProvider(provider)
client := openfeature.NewClient("my-app")

ctx := openfeature.NewEvaluationContext("user-1", map[string]any{"country": "NG"})

checkoutV2, _ := client.BooleanValue(ctx, "checkout-v2", false, ctx)
currency, _  := client.StringValue(ctx, "checkout-currency", "$", ctx)
```

### Python

```python
from openfeature import api
from openfeature.contrib.provider.flagd import FlagdProvider

api.set_provider(FlagdProvider())          # flagd on localhost:8013
client = api.get_client(name="my-app")

ctx = {"targetingKey": "user-1", "country": "NG"}
checkout_v2 = client.get_boolean_value("checkout-v2", False, ctx)
currency = client.get_string_value("checkout-currency", "$", ctx)
```

### Node.js (TypeScript)

```ts
import { OpenFeature } from '@openfeature/server-sdk';
import { FlagdProvider } from '@openfeature/flagd-provider';

await OpenFeature.setProvider(new FlagdProvider());   // flagd on localhost:8013
const client = OpenFeature.getClient('my-app');

const ctx = { targetingKey: 'user-1', country: 'NG' };
const checkoutV2 = await client.getBooleanValue('checkout-v2', false, ctx);
const currency = await client.getStringValue('checkout-currency', '$', ctx);
```

### Java

```java
import dev.openfeature.contrib.providers.flagd.FlagdProvider;
import dev.openfeature.sdk.*;

OpenFeatureAPI.getInstance().setProvider(new FlagdProvider()); // flagd on localhost:8013
Client client = OpenFeatureAPI.getInstance().getClient("my-app");

EvaluationContext ctx = new ImmutableContext(Map.of("targetingKey", "user-1", "country", "NG"));
boolean checkoutV2 = client.getBooleanValue("checkout-v2", false, ctx);
String currency = client.getStringValue("checkout-currency", "$", ctx);
```

### .NET

```csharp
using OpenFeature;
using OpenFeature.Providers.Flagd;

OpenFeature.Api.Instance.SetProvider(new FlagdProvider()); // flagd on localhost:8013
var client = OpenFeature.Api.Instance.GetClient("my-app");

var ctx = new EvaluationContextBuilder()
    .Set("targetingKey", "user-1")
    .Set("country", "NG")
    .Build();
var checkoutV2 = await client.GetBooleanValueAsync("checkout-v2", false, ctx);
var currency = await client.GetStringValueAsync("checkout-currency", "$", ctx);
```

The evaluation context is how targeting works: a `country=NG` user gets `checkout-v2`, the `₦` currency, and the NG banner — while a US user falls back to defaults. Same flags, different answers, all evaluated by flagd.

## Serve / sync guarantees

- **One contract:** every flag change goes through the outbox pair (`SetFlag` / `DeleteFlag` / `flick set`). Bare `UPDATE flags` SQL is visible only after a client reconnects.

> **Do NOT `UPDATE flags` directly.** If you write to the `flags` table without writing a matching row to the `outbox` table in the same transaction, flagd never sees the change. The outbox is how changes propagate — no outbox event, no sync. Always use `flick set`, the console, or the `SetFlag` / `DeleteFlag` Go functions.
- **At-least-once:** the replication slot replays undelivered events across restarts — consumers must be idempotent.
- **Ordered per topic:** outbox events within a topic are delivered strictly in order, so a rapid on/off toggle always converges on the *final* value, never a stale intermediate.
- **Self-healing:** drop-on-full deltas resolve on reconnect; hard server crashes trigger flagd's own retry + resync.

## Scope

**In scope:** Postgres-backed flag storage with transactional eventing, live gRPC sync to flagd, CLI management, a full web console with live metrics, delete support, an end-to-end init probe, and e2e-tested against a real database.

**Out of scope (deliberately):** flag *evaluation* and targeting *decisions* (that's flagd's job — flick only stores and ships the rules), auth/multi-tenancy, and dead-lettering of dropped deltas. The delivery handler fans out to the Hub and logs; the wire format is flagd's full-config-per-message semantics (not minimal diffs).

## Layout

```
flick
├── cmd/flick/                the CLI (init, serve, set, get, list, delete, export, import, version)
│   ├── main.go               command wiring, version, DSN resolution
│   ├── init.go               migrations + wal_level check → replication probe
│   ├── probe.go              end-to-end probe: slot, publication, probe event
│   ├── serve.go              sync gRPC server + console + outbox consumer
│   ├── flags_cmd.go          set / get / list / delete / export / import
│   ├── dashboard.go          console: embedded HTML+CSS+JS, /api/flags, /events, /metrics/stream
│   └── *_test.go             unit tests; *_e2e_test.go behind `-tags e2e`
├── flags.go                  SetFlag / DeleteFlag / TranslateFlag / ApplyDelta / snapshot builders
├── outbox.go                 phylax wiring; delivery handler → Hub
├── hub.go                    pub/sub: Subscribe / Unsubscribe / Publish (drop-on-full)
├── sync.go                   SyncService: FetchAllFlags + SyncFlags (subscribe-first)
├── migrate.go                embedded goose migrations
├── db/goose_migrations/      flags + outbox DDL
└── gen/flagd/sync/v1/        generated flagd.sync.v1 Go bindings
```

## Development

```sh
go test ./...                # unit tests — no database required
FLICK_DSN=<postgres-dsn> go test -tags e2e ./...   # e2e — needs Postgres
```

Tests are split into two buckets:

- **Unit** (`go test ./...`) — hub, translation, snapshot, delta, and CLI-wiring tests. Pure in-memory; runs anywhere with no database.
- **E2E** (`go test -tags e2e ./...`) — real Postgres round-trips: `SetFlag`/`DeleteFlag` transactions, the CLI (set/get/list/delete), and the console HTTP API. The e2e build tag activates a `TestMain` that self-applies the embedded migrations against the database from `FLICK_DSN`.

E2E tests take their database entirely from the `FLICK_DSN` env var — there is no hardcoded fallback. If `FLICK_DSN` is unset (or the database is unreachable), e2e tests skip cleanly instead of failing, so `-tags e2e` is safe to run anywhere.
