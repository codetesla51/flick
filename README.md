# flick

**Postgres-native feature flags, live-synced to flagd — no polling.**

[![CI](https://github.com/codetesla51/flick/actions/workflows/ci.yml/badge.svg)](https://github.com/codetesla51/flick/actions/workflows/ci.yml)
[![Go version](https://img.shields.io/github/go-mod/go-version/codetesla51/flick)](https://go.dev)
[![Release](https://img.shields.io/github/v/release/codetesla51/flick)](https://github.com/codetesla51/flick/releases)

flick stores feature flags in Postgres and streams every change to [flagd](https://flagd.dev) over its native gRPC sync protocol. Change a flag from your terminal or SQL and flagd clients see it within milliseconds — with **transactional guarantees** most flag systems don't have: the flag update and its change event commit atomically, so a change is never half-applied, and undelivered events replay on restart.

Your app never talks to flick. It talks to flagd, which evaluates from its own in-memory copy — sub-millisecond reads, no per-request database hits. flick is the *source*; flagd is the *evaluator*.

---

## Live demo

No install, no Postgres, no setup — this is the full stack from [Quick start](#quick-start), already running against a real Postgres on a public VM. The storefront reads **nine real flags** through flagd, and flipping one in the console sends it through the entire pipeline — Postgres → outbox → LISTEN/NOTIFY → flagd → back to the page — in under a second:

- **Demo storefront** — https://owner-reliable-closure-interactions.trycloudflare.com
- **flick console** (manage flags, watch live metrics) — https://attending-crowd-particles-dramatically.trycloudflare.com

> [!WARNING]
> The demo is served through a Cloudflare **quick tunnel** on a free-tier VM — it is **not permanent**. The URLs are temporary and will change if the tunnel restarts, and the box may be offline at any time. Treat it as a preview: the [Quick start](#quick-start) below runs the same stack on your own machine with stable `localhost` addresses.

---

## Why flick

- **Flags are data.** They live in Postgres — SQL, joins, audit, backups, the same operational muscle you already have. No new platform, no vendor lock-in.
- **No polling anywhere.** Changes are pushed end-to-end via a **transactional outbox** + **Postgres LISTEN/NOTIFY**. Write a flag and an outbox event in one transaction; a trigger fires a NOTIFY, and the flick sync server streams the event to connected flagd instances.
- **No missed updates.** A subscribe-first streaming design guarantees a new client gets the full snapshot *and* any changes that land while the snapshot is being built — nothing slips through the gap.
- **Survives restarts.** On boot, flick replays outbox events that were never delivered (at-least-once across restarts), so changes made while flick was down still reach flagd when it comes back. flagd additionally resyncs a full snapshot whenever a client reconnects.
- **Zero replication setup.** No `wal_level=logical`, no replication slots, no publications, no special privileges — LISTEN/NOTIFY works on **any** Postgres, including managed ones.
- **Small and testable.** One modular library (`package flick`) + one CLI entrypoint, with `-race`-clean tests against a real Postgres.

---

## How it works

```mermaid
flowchart LR
    CLI["flick CLI / SetFlag"] -->|"flag + outbox event (1 tx)"| DB[("Postgres<br/>flags + outbox")]
    DB -->|"trigger fires pg_notify"| Notify["notify stream<br/>LISTEN/NOTIFY"]
    Notify -->|"reads outbox row, delivers"| Hub{{"Hub<br/>pub/sub"}}
    Hub -->|"broadcasts deltas"| Sync["SyncService<br/>gRPC :8015<br/>flagd.sync.v1"]
    Sync <-->|"snapshot + live stream"| Flagd["flagd"]
    Flagd -->|"evaluates flags"| Users["Your app / users"]
    style DB fill:#2d4,color:#111
    style Flagd fill:#49c,color:#fff
```

**Division of labor:** flick *stores and pushes* config; flagd *evaluates* it. flick never answers "what should this user see?" — only "here's the config."

### The write path (per flag change)

1. `SetFlag` (or `flick set`) upserts the `flags` row and appends an outbox event — **one transaction, atomic commit**.
2. The `outbox_flags_notify` trigger fires `pg_notify('flick_flags', id)` when that transaction commits.
3. The notify layer (`LISTEN flick_flags`) re-reads the outbox row by id — sidestepping the 8 KB NOTIFY payload limit — marks it delivered, and publishes the flag delta to the Hub.
4. Every connected `SyncFlags` stream merges the delta into its in-memory state and resends the full config to flagd.

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

Subscribe **before** the snapshot work: nothing between "started listening" and "snapshot sent" is lost. The Hub uses buffered, drop-on-full channels — a slow subscriber can never stall the stream consumer; a dropped delta self-heals on reconnect with a fresh snapshot.

## Install

```sh
go install github.com/codetesla51/flick/cmd/flick@latest
```

> [!NOTE]
> `@latest` resolves to the newest tagged release (see the Releases page for version history). To build from a checkout instead: `go run ./cmd/flick` (or `go build -o flick ./cmd/flick`).

---

## Quick start

> [!TIP]
> **Any Postgres works** — no `wal_level` changes, no replication privileges. The examples below use a docker container; `FLICK_DSN` or `--dsn` points flick at whatever Postgres you have.

### 1. Postgres

```sh
docker run -d --name flick-pg \
  -e POSTGRES_USER=flick -e POSTGRES_PASSWORD=flick -e POSTGRES_DB=flick \
  -p 5432:5432 postgres:16
```

That's it — no `ALTER SYSTEM SET wal_level`, no restart, no slot or publication setup.

### 2. Set up and run flick

```sh
flick init      # migrations + notify probe (proves streaming works)
flick serve     # sync gRPC server on :8015, console on :8016
```

`flick init` doesn't just check settings — it runs a live probe: it confirms the `outbox_flags_notify` trigger is installed, then verifies LISTEN/NOTIFY end-to-end by listening on a test channel from one connection and sending a notification from a second. If anything is broken, it says exactly what and how to fix it.

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
| `flick init` | Apply migrations, verify the outbox notify trigger, run a live LISTEN/NOTIFY probe (round-trip confirmed end-to-end) |
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
- **Metrics** — headline counters for Flags, Delivered, Pending, and Changes processed, plus subscribers / dropped / replayed / failed — fed by `/metrics/stream`.
- **Graphs** — 60-second rolling history of outbox delivered, changes processed, and pending events.
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
| `/metrics/stream` | SSE: counters every second (`changes_processed`, `changes_dropped`, `subscribers`, `outbox_delivered`, `outbox_inflight` = pending outbox rows, `outbox_failed`, `replayed`) |
| `/events` | SSE: every outbox event, live |

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
- **Atomic write + event:** the flag row and its outbox event commit in one transaction, so a change is never half-applied.
- **At-least-once across restarts:** on boot, flick replays outbox events that were never delivered and marks them delivered, so changes made while flick was down still reach flagd. Live delivery is at-most-once — a NOTIFY sent while flick is disconnected is lost — but flagd resyncs a full snapshot whenever a client (re)connects, which bounds staleness regardless. (A row can be delivered twice in the tiny startup overlap; flagd applies deltas idempotently.)
- **Ordered:** a single consumer reads outbox rows in id order, so a rapid on/off toggle always converges on the *final* value, never a stale intermediate.
- **Self-healing:** drop-on-full deltas resolve on reconnect; hard server crashes trigger flagd's own retry + resync.

## Scope

**In scope:** Postgres-backed flag storage with transactional eventing, live gRPC sync to flagd, CLI management, a full web console with live metrics, delete support, an end-to-end init probe, and e2e-tested against a real database.

**Out of scope (deliberately):** flag *evaluation* and targeting *decisions* (that's flagd's job — flick only stores and ships the rules), auth/multi-tenancy, and dead-lettering of dropped deltas. The delivery handler fans out to the Hub and logs; the wire format is flagd's full-config-per-message semantics (not minimal diffs).

## Layout

```
flick
├── cmd/flick/                the CLI (init, serve, set, get, list, delete, export, import, version)
│   ├── main.go               command wiring, version, DSN resolution
│   ├── init.go               migrations + trigger check → LISTEN/NOTIFY probe
│   ├── notify_probe.go       live probe: LISTEN from one conn, NOTIFY from another
│   ├── serve.go              sync gRPC server + console + notify stream
│   ├── flags_cmd.go          set / get / list / delete / export / import
│   ├── dashboard.go          console: embedded HTML+CSS+JS, /api/flags, /events, /metrics/stream
│   └── *_test.go             unit tests; *_e2e_test.go behind `-tags e2e`
├── flags.go                  SetFlag / DeleteFlag / TranslateFlag / ApplyDelta / snapshot builders
├── notify.go                 LISTEN/NOTIFY stream layer: replay + live delivery → Hub
├── hub.go                    pub/sub: Subscribe / Unsubscribe / Publish (drop-on-full)
├── sync.go                   SyncService: FetchAllFlags + SyncFlags (subscribe-first)
├── migrate.go                embedded goose migrations
├── db/goose_migrations/      flags + outbox DDL + outbox_flags_notify trigger
└── gen/flagd/sync/v1/        generated flagd.sync.v1 Go bindings
```

## Development

```sh
go test ./...                # unit tests — no database required
FLICK_DSN=<postgres-dsn> go test -tags e2e ./...   # e2e — needs Postgres
```

Tests are split into two buckets:

- **Unit** (`go test ./...`) — hub, translation, snapshot, delta, and CLI-wiring tests. Pure in-memory; runs anywhere with no database.
- **E2E** (`go test -tags e2e ./...`) — real Postgres round-trips: `SetFlag`/`DeleteFlag` transactions, the CLI (set/get/list/delete), the console HTTP API, and the notify layer (live LISTEN/NOTIFY delivery, delete events, startup replay, metrics). The e2e build tag activates a `TestMain` that self-applies the embedded migrations against the database from `FLICK_DSN`; the notify tests run against their own scratch database so the live consumer never hears other tests' writes.

E2E tests take their database entirely from the `FLICK_DSN` env var — there is no hardcoded fallback. If `FLICK_DSN` is unset (or the database is unreachable), e2e tests skip cleanly instead of failing, so `-tags e2e` is safe to run anywhere.

---

## Design history: how flick used logical replication (CDC)

Until v0.3, flick streamed outbox events with **logical replication** instead of LISTEN/NOTIFY. This section documents that design for readers of older versions or older blog posts.

### The CDC architecture (v0.1–v0.2)

```mermaid
flowchart LR
    CLI["flick CLI / SetFlag"] -->|"flag + outbox event (1 tx)"| DB[("Postgres<br/>flags + outbox")]
    DB -->|"logical replication (WAL)"| Phylax["phylax (CDC)"]
    Phylax -->|"delivers outbox rows"| Hub{{"Hub<br/>pub/sub"}}
    Hub -->|"broadcasts deltas"| Sync["SyncService<br/>gRPC :8015"]
    Sync <-->|"snapshot + live stream"| Flagd["flagd"]
    style DB fill:#2d4,color:#111
    style Flagd fill:#49c,color:#fff
```

- **Transport:** [phylax](https://github.com/codetesla51/phylax), a logical-replication client, consumed `outbox` inserts from the WAL via the `flick_slot` replication slot and `flick_pub` publication, acked each row (`delivered_at`), and published flag deltas to the Hub.
- **Requirements:** `wal_level = logical`, a replication slot + publication, the `REPLICATION` privilege, and pg_hba rules permitting replication connections — verified by an end-to-end probe (`flick init`) that created the slot/publication and streamed a probe row to prove delivery.
- **Guarantees:** the slot made delivery **at-least-once even while disconnected** — undelivered events replayed from the slot's saved position after any downtime.

### Why it changed

Feature flags change a handful of times a day, and the CDC setup tax was disproportionate to that workload: every environment needed WAL-level changes, slot and publication management (slots can go stale, conflict across databases, or block `VACUUM`), replication privileges, and a probe elaborate enough to diagnose all of it. LISTEN/NOTIFY is Postgres's built-in mechanism for exactly this job — infrequent change notifications with zero configuration — and works on **any** Postgres, including managed providers that restrict or charge extra for logical replication.

**What was traded away:** live delivery is now at-most-once (a NOTIFY sent while flick is disconnected is lost), and there is no slot to replay from. Both are covered in practice — flick replays undelivered outbox events on startup, and flagd resyncs a full snapshot whenever a client reconnects — but the strict at-least-once-per-event guarantee of the slot era is gone.

**What was gained:** no `wal_level` changes, no slots or publications, no replication privileges, a 10-line probe instead of a 200-line one, one less dependency, and a codebase that works against a stock Postgres out of the box.

For migrations from v0.2: the `flags` and `outbox` tables are unchanged; run `flick init` (now the LISTEN/NOTIFY probe) after upgrading, and drop the old `flick_slot` slot and `flick_pub` publication if you no longer need them.
