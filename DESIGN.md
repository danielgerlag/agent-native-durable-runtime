# durable_session v0

Crate `durable_session`. Rust 2021. Sync. Dual-licensed Apache-2.0 OR MIT.

This is the implementation contract. Fill the bodies. Do not grow the public surface.

## Problem

Agent sessions stream, mutate a workspace, and call tools with irreversible side effects. Restore must reload transcript, workspace, and the side-effect ledger as one object. It must not replay orchestration code. A completed or partially persisted model turn is never sampled again. An `Applied` idempotent tool is never invoked again. A `Pending` at-most-once tool is inspected, never silently retried.

## Usage (caller's view)

```rust
use std::time::Duration;
use durable_session::{Input, OpenOptions, OpId, Session, SessionId, WorkerId};

let mut session = Session::open(OpenOptions {
    store_dir: "./.durable".into(),
    session_id: SessionId::parse("sess_demo")?,
    worker: WorkerId::this_process(),
    workspace: "./work".into(),
    ttl: Duration::from_secs(60),
    filter: Default::default(),
})?;

session.append(OpId::from_static("req-1"), Input::user("summarize README"))?;
```

Create and restore are the same call. After `kill -9`, pass the same `OpenOptions`. If another worker holds an unexpired lease, `open` returns `Error::LeaseHeld`.

The agent thread owns `Session` (`!Sync`). UI and SSE reconnect use `SessionView` on a second connection.

`begin_tool` is the policy gate. The harness does not query a ledger and then decide. `run_tool` is the begin/invoke/complete bracket.

`Session::open` auto-seals a trailing unsealed assistant turn as `FinishReason::TruncatedCrash`. The prefix stays in the transcript. The next model call is a new turn that sees that prefix as history.

## Shape

**Base.** Red (Session as the deep module). Cross-judge total 37 vs Blue 31 vs Green 29.

**Grafted from Green.** Private `apply` + typed `Reject` behind every persist. Continuation rule: a new assistant turn cannot start while one is unsealed. Import ignores derived checkpoint views. `Drop` does not release the lease; panic mid-tool must not open a steal window. `close()` releases. TTL is the death protocol. Same `WorkerId` re-attaches without waiting.

**Grafted from Blue.** Conformance child aborts, it does not unwind. `PersistOp` covers blob fsync, head swap, tool pending/applied, and assistant chunks. Partial unique index on idempotent `(session_id, args_hash)` where status is `applied`.

**Rejected.** Public event-write API and caller-held `Lease` (Green). Worker actor, `Host` trait, mailbox, `idempotent: bool`, dual-truth `side_effects` in the checkpoint (Blue).

**Single source of truth.** The event log. Tool ledger rows and workspace head pointers are same-transaction projections. On disagreement the log wins and projections rebuild.

**Write path.** Blob bytes via tmp + fsync + rename. Then `BEGIN IMMEDIATE`, lease fence (generation CAS), `apply`, insert event, upsert projection, `COMMIT`. Crash after blob rename and before SQL leaves an orphan blob. Crash after SQL and before bytes is forbidden by ordering.

**Workspace.** Content-addressed trees, not git, not tar. `begin_tool` snapshots the before-image. `complete_tool` snapshots the after-image in the same SQLite transaction as `ToolApplied`. Live working copy is a cache. Restore uses a journaled sibling materialize then rename. A crash mid-tool restores the before-image before any re-invoke.

**Streaming.** Each assistant chunk is an event. `ResumeToken` is opaque (`session` + `seq`). `SessionView::replay` is `seq > token.seq`. Persist the chunk before the caller is allowed to send it.

**Shared state.** One writer per session. Serialized by lease CAS plus a fencing generation on every write. Readers open `SessionView`. After a steal, the old handle's next mutation returns `Error::Fenced` and poisons the handle.

## Public API

Branded ids with private fields: `SessionId`, `WorkerId`, `ResumeToken`, `EventSeq`, `CallId`, `ArgsHash`, `BlobRef`, `OpId`, `TurnId`, `SnapshotRev`.

`ResumeToken` has `encode` / `decode` only. No public constructor from a raw seq.

Enums, not booleans:

- `ToolPolicy::{Idempotent, AtMostOnce}`
- `SideEffectStatus::{Pending, Applied, Failed, Abandoned}`
- `FinishReason::{Stop, Length, ToolCalls, TruncatedCrash, Error}`
- `LeaseState::{Vacant, Held { worker, last_heartbeat, ttl }, Expired { last_worker, last_heartbeat, ttl }}`
- `Recovery::{Clean, Truncated { prefix, model }, PendingTool(PendingTool)}`
- `Input::{User, System}` (no assistant)

`ToolDisposition` from `begin_tool`:

- `Run` — caller must invoke, then `complete_tool` / `fail_tool`
- `AlreadyApplied(AppliedTool)`
- `Inspect(PendingTool)` — `Pending` + `AtMostOnce` after a crash

`run_tool` returns `ToolRun::{AlreadyApplied, Inspect, Completed}` so `Run` is not overloaded.

`Session` methods: `open`, `open_with_hooks`, `id`, `worker`, `workspace_path`, `resume_token`, `recovery`, `append`, `append_assistant_chunk`, `seal_assistant`, `begin_tool`, `complete_tool`, `fail_tool`, `abandon_tool`, `run_tool`, `snapshot_workspace`, `restore_workspace`, `heartbeat`, `messages`, `events`, `replay`, `export_bundle`, `close`.

`SessionView`: `open`, `messages`, `events`, `replay`, `resume_token`, `recovery`, `lease_state`, `export_bundle`.

`import_bundle(store_dir, src) -> SessionId`.

`Error` never contains a `rusqlite` type. Variants include `LeaseHeld`, `Fenced`, `InvalidState`, `Corrupt`, `Injected`, `Store` (opaque), `Io`, `Bundle`.

`semconv` module exports OTel attribute *name* constants. It does not store `gen_ai.client.inference.operation.details`.

## Private reducer

`pub(crate) fn apply(state: &SessionState, event: &Event) -> Result<SessionState, Reject>`

`SessionState` is an immutable value. `Turn` is derived, never stored.

Rejects (typed): session not open, already closed, unexpected turn, unknown/duplicate call, not pending, empty chunk, two pending tools, assistant input while a tool is pending, `complete_tool` with a different outcome than a recorded `Applied`.

`open` folds events, then if the tail is an unsealed assistant, persists `AssistantSealed { TruncatedCrash }` through `apply` before returning.

`messages()` folds sealed events only. Unsealed deltas are available via `recovery()` / `Truncated.prefix` after auto-seal they are part of the sealed truncated message.

## Modules

| File | Owns |
| --- | --- |
| `src/lib.rs` | Re-exports. No policy. |
| `src/ids.rs` | Branded ids. |
| `src/event.rs` | `Event`, `Input`, `Message`, `AssistantDelta`, projections. |
| `src/reducer.rs` | `apply`, `Reject`, `SessionState`. No IO. |
| `src/tool.rs` | Spec, policy, disposition, `ToolCtx`. |
| `src/session.rs` | Public mutating handle. Persist-before-return. Poison. |
| `src/lease.rs` | CAS SQL, generation fence, `LeaseState`. |
| `src/store.rs` | SQLite, WAL, event insert, blob put/get. The only `rusqlite` import. |
| `src/workspace.rs` | Tree walk, snapshot, restore journal. |
| `src/bundle.rs` | Checkpoint v0 pack/unpack. Parses JSON into domain events. |
| `src/fault.rs` | `PersistOp`, `Fault`, `CrashAtNth`. |
| `src/error.rs` | Public `Error`. |
| `src/semconv.rs` | OTel name constants. |
| `examples/react.rs` | ~200-line ReAct demo with fake model and tools. |
| `tests/conformance.rs` | Crash-at-N runner. |

Call chain on write: `session.rs` → `store.rs` (and `workspace.rs` or `lease.rs`). Reducer is called from store inside the transaction.

## SQLite

`<store_dir>/durable.sqlite` plus `<store_dir>/blobs/<aa>/<sha256>`.

Pragmas: `WAL`, `foreign_keys=ON`, `busy_timeout=5000`. Tests that abort a child use `synchronous=FULL`.

Tables:

- `meta(schema_version)`
- `sessions(id PK, created_at, workspace_path, workspace_head_rev, closed_at)`
- `leases(session_id PK, worker_id, generation, heartbeat_ms, ttl_ms)`
- `events(session_id, seq, t_ms, write_id, kind, body_json, blob_ref, PRIMARY KEY(session_id, seq), UNIQUE(session_id, write_id))`
- `ops(session_id, op_id, seq, PRIMARY KEY(session_id, op_id))` — user/system append idempotency
- `tool_calls(...)` — projection, rebuildable
- `snapshots(session_id, rev, tree_blob, event_seq, created_at)`
- `restore_journal(session_id PK, rev, phase, scratch_path, bak_path)`

Partial unique index:

```sql
CREATE UNIQUE INDEX tool_calls_idempotent_applied
  ON tool_calls(session_id, name, args_hash)
  WHERE status = 'applied' AND policy = 'idempotent';
```

Lease acquire: `INSERT ... ON CONFLICT DO UPDATE` where owner is this worker or heartbeat+ttl is in the past. `changes()==1` or `LeaseHeld`. Every write updates heartbeat and checks `generation`. Zero rows is `Fenced`.

Internal `WriteId` on every event insert. Retry after a committed-but-unseen append returns the original seq.

## Checkpoint v0

See `spec/checkpoint-v0.md`. JSON directory. `events` (ndjson) is the source of truth. `views/` and a materialized `workspace/` are derived. Import ignores derived files and refolds events.

Lease state is not in the bundle.

## Workspace crash protocol

Blob: write `.part`, fsync, rename. Readers never open `.part`.

Snapshot: hash files, put blobs, put tree blob, then SQL insert of `WorkspaceSnapshotted` + snapshot row + session head. Crash before SQL: next snapshot reuses hashes.

Restore: journal `Copying` into scratch; `Swapping` rename live aside then scratch into place; `Done` delete backup. `open` reconciles the journal before other work.

Default exclude: `.git`, `node_modules`, `target`, the store dir if it sits inside the workspace. UTF-8 paths only. Do not follow symlinks. Skip empty dirs.

## Conformance lever

`Fault::before_commit(PersistOp)` fires immediately before each durability boundary.

`PersistOp`: `OpenLease`, `Append`, `AppendAssistantChunk`, `SealAssistant`, `BeginTool`, `CompleteTool`, `SnapshotStage`, `SnapshotCommit`, `RestoreStage`, `RestoreCommit`, `BlobRename`, `Heartbeat`.

`CrashAtNth { n, abort_process }`. In-process tests return `Error::Injected`. The conformance child calls `std::process::abort()`.

Oracle: fake tools append `{tool, args_hash, call_id}` to a JSONL file *outside* the store, with tmp+rename, so a crash cannot roll the oracle back.

Scan: dry-run counts persist ops; for each `n` in `1..=count`, crash then resume to completion.

Assert:

1. An `Idempotent` `(name, args_hash)` that reached `Applied` is not invoked again. A legal re-invoke happens only from `Pending`. After completion there is one `Applied` row per such hash.
2. `AtMostOnce` tools are invoked at most once. `Pending` after crash becomes `Inspect` / `abandon_tool`, never a second oracle line.
3. Sealed assistant turns are not replaced by a new sample of the same turn. Truncated seals use `TruncatedCrash`.
4. Workspace head after a mid-snapshot crash is the old tree or the new tree, never a mix.

## Tradeoffs accepted

- We accept a commit per assistant chunk in exchange for reconnect that cannot see a prefix the log does not have.
- We accept sealing truncated turns instead of resuming a provider HTTP stream.
- We accept best-effort side effects with an explicit ledger, not distributed transactions.
- We accept sequential tools in v0 (at most one `Pending`).
- We accept polling `SessionView` instead of an async fan-out.
- We accept a large `Session` impl in exchange for one atomic object.
- We accept content-addressed trees without git.

## Alternatives considered

- Public `Store` + `Lease` + `apply` as the write API. Lost. Harness authors would coordinate crash rules the types already know.
- In-process worker actor + `Host`. Lost. That is a framework. v0 is a library the harness loop calls.
- Dual ledger table as a source of truth. Lost. It diverges from the log.
- RAII lease release on `Drop`. Lost. Panic mid-tool would invite a steal.

## Open questions

None that block v0. Auto-seal on open is decided (yes). `force_reinvoke` for at-most-once is omitted.

## Next implementation step

Crate skeleton, branded ids, `Event`, `apply`, table-driven reducer tests. Then store+lease, then Session, then conformance, then `examples/react.rs`.
