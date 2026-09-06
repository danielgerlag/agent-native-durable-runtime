# Implementation contract for durable_session v0

Read `DESIGN.md` and `spec/checkpoint-v0.md` first. They win if this file is silent. Do not expand the public API past `DESIGN.md`.

## Constraints

- Rust 2021 crate named `durable_session` at the repo root.
- `license = "Apache-2.0 OR MIT"` in Cargo.toml. Edition 2021.
- Sync only. No tokio. No async traits.
- `rusqlite` with `bundled`. It is imported only in `src/store.rs` and `src/lease.rs` (lease SQL may live in `lease.rs` if it takes `&Connection`; do not re-export the type).
- Comments only for a non-obvious *why*. No phase narration. No `// Create the session` above the code that creates it.
- `Error` never carries `rusqlite::Error` in a public variant.
- Tests live under `tests/` and in `#[cfg(test)]` modules next to the reducer.
- Feature `fault-injection` is not required. `open_with_hooks` is always available. Production callers use `open`.

## Dependencies (do not add more without need)

```
rusqlite = { version = "0.32", features = ["bundled"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
sha2 = "0.10"
thiserror = "2"
uuid = { version = "1", features = ["v4"] }
hex = "0.4"
walkdir = "2"
```

Dev: `tempfile = "3"`.

## Sequence. Verify each unit before the next.

1. `cargo init --lib` is already not done. Create Cargo.toml and modules. Branded ids compile. `apply` + table tests for illegal turns (assistant while pending, two pendings, complete unknown call, empty chunk, user after close). `cargo test reducer` green.
2. Store open, schema, blob put/get, append event, reload events. Unit test roundtrip. Lease acquire, same-worker reattach, foreign worker `LeaseHeld`, steal after TTL (inject clock), fence after steal.
3. Workspace snapshot + restore + journal reconcile. Test that a crash during `Copying` retries; a restored tree matches hashes.
4. `Session` public methods. Tests: append idempotent on `OpId`; `begin_tool` returns `AlreadyApplied` on second call with same name+args_hash after `Applied`; `Pending`+`AtMostOnce` returns `Inspect`; `Pending`+`Idempotent` returns `Run` and restores before-image; `open` auto-seals truncated assistant; `SessionView::replay` from a token; `export_bundle` / `import_bundle`.
5. `tests/conformance.rs` crash-at-N with in-process `Error::Injected` and a subprocess abort lane for at least one N. Oracle JSONL outside the store. Assertions from DESIGN.md.
6. `examples/react.rs`. Fake model emits one user-visible text turn that calls `idempotent_write` then `at_most_once_email` then stops. Survives crash mid-tool in the conformance runner. Keep it near 200 lines.

## `apply` table (must hold)

Start at origin. `SessionOpened` is implicit in `Session::open` (first persist may be a workspace snapshot or the first user event; encode an internal `SessionOpened` event if that keeps the reducer total).

- After close, every event rejects.
- `User`/`System` allowed when no pending tool and no unsealed assistant.
- First `AssistantDelta` opens a turn. Further deltas only while that turn is unsealed.
- `AssistantSealed` only while unsealed. Then messages include the folded assistant.
- `ToolPending` only when assistant is sealed, zero pending tools, and `applied_by_hash` does not already map this `(name, args_hash)`.
- `ToolApplied`/`ToolFailed`/`ToolAbandoned` require `Pending` for that `CallId`.
- At most one `Pending`.
- `WorkspaceSnapshotted` always allowed while open, except you should still fence the lease.

## Demo tools

- `idempotent_write`: args `{path, contents}`, writes that file under the workspace. Policy `Idempotent`.
- `at_most_once_email`: args `{to, body}`, appends one line to an oracle file *outside* store and workspace. Policy `AtMostOnce`.

## Do not

- Add Python/TS bindings, a daemon, tokio, CBOR, Temporal names, or a GUI.
- Put `rusqlite` in `lib.rs` re-exports.
- Make `ResumeToken` constructible from a raw u64.
- Release the lease in `Drop`.
- Snapshot by shelling out to `git`.
- Use `boolean idempotent` on `ToolSpec`.
