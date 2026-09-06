# durable_session

A Rust library that checkpoints an agent session as one object: the conversation and tool transcript, a workspace snapshot, and a tool side-effect ledger. Restore reloads that object. It does not replay your agent loop.

Use it from a local harness (Hivemind OS, a Claude Code-style loop, a DIY ReAct). You do not run a Temporal cluster.

License: Apache-2.0 OR MIT.

## Add the crate

The package lives in this repository. In a sibling Cargo project:

```toml
[dependencies]
durable_session = { path = "../agent-native-durable-runtime" }
```

## Open or resume a session

Create and restore are the same call. After a crash, pass the same `store_dir`, `session_id`, and `workspace`.

```rust
use std::time::Duration;
use durable_session::{Input, OpenOptions, OpId, Session, SessionId, WorkerId};

fn main() -> durable_session::Result<()> {
    let mut session = Session::open(OpenOptions {
        store_dir: "./.durable".into(),
        session_id: SessionId::parse("sess_demo")?,
        worker: WorkerId::this_process(),
        workspace: "./work".into(),
        ttl: Duration::from_secs(60),
        filter: Default::default(),
    })?;

    session.append(OpId::from_static("req-1"), Input::user("summarize README"))?;
    Ok(())
}
```

If another live worker holds the lease, `open` returns `Error::LeaseHeld`. The same `WorkerId` re-attaches immediately. A `WorkerId::this_process()` value from a process that has exited is treated as dead, so `kill -9` resume does not wait for the TTL.

The agent thread owns `Session`. A UI thread that only reads uses `SessionView`.

## Call tools without duplicating side effects

`begin_tool` (or `run_tool`) is the policy gate. Do not query a ledger yourself.

- `AlreadyApplied` means this tool name and args hash already completed. Skip the invoke.
- `Inspect` means a non-idempotent tool was `Pending` when the process died. Do not retry it silently. `abandon_tool` or ask a human.
- `Run` means invoke the tool, then `complete_tool` or `fail_tool`.

Mark filesystem writes `ToolPolicy::Idempotent`. Mark email, PRs, and other one-shot APIs `ToolPolicy::AtMostOnce`.

## Stream and reconnect

Each assistant chunk is persisted before `append_assistant_chunk` returns. Give the client the `ResumeToken`. On reconnect, `SessionView::replay` yields events after that token. The model is not prompted again for a prefix that is already in the log.

If the worker died mid-stream, the next `Session::open` seals the turn as `FinishReason::TruncatedCrash`. The prefix stays in `messages()`. The next model call is a new turn.

## Run the demo

```bash
cargo run --example react -- /tmp/durable-demo
# crash it, then run the same command again
```

The demo writes `out.txt` with an idempotent tool and records a fake email with an at-most-once tool. The second run must not send a second email.

```bash
cargo test
```

`tests/conformance.rs` crashes at every persist point, resumes, and checks those rules.

## Layout

- `DESIGN.md` explains the types and why Session is the public handle.
- `spec/checkpoint-v0.md` is the portable JSON bundle.
- `src/` is the library. SQLite and blobs live under the `store_dir` you pass to `open`.

Non-goals for v0: multi-region replication, a Temporal bridge, a GUI, language bindings, and exactly-once delivery for APIs that ignore the ledger.
