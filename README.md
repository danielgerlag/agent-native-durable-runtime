# durable_session

The product is a directory format for an agent session: transcript, workspace tree, and tool side-effect ledger. Restore is loading that directory. It is not replaying a program.

Rust, Python, and TypeScript all read the same files. JSON Schema under `spec/schema/` is the contract. Golden bundles live in `fixtures/v0/`.

License: Apache-2.0 OR MIT.

## Load a checkpoint

Rust:

```rust
use durable_session::Checkpoint;

let cp = Checkpoint::load("./fixtures/v0/tools")?;
println!("{}", cp.view());
```

Python:

```python
from durable_session import load

cp = load("fixtures/v0/tools")
print(cp.view())
```

TypeScript:

```ts
import { load } from "durable-session";

const cp = load("fixtures/v0/tools");
console.log(cp.view());
```

`view()` is a JSON object. All three languages emit the same object for a given bundle. That is the conformance check.

## Format

A bundle is a directory:

```
manifest.json
transcript.ndjson
trees/<sha256>.json
blobs/<aa>/<sha256>
views/ledger.json
workspace/
```

`transcript.ndjson` is the source of truth. Paths listed in `manifest.derived` are ignored on load.

Readable spec: `spec/checkpoint-v0.md`. Normative schemas: `spec/schema/`.

```bash
./scripts/conformance.sh
```

That runs the Rust, Python, and TypeScript golden tests.

## Rust runtime

The crate also persists live sessions to SQLite with a sticky worker lease. That is how a harness survives `kill -9`. It is one implementation of the format, not the format itself.

```toml
[dependencies]
durable_session = { path = "." }
```

See `DESIGN.md` for the session handle, tool policy, and lease.

```bash
cargo run --example react -- /tmp/durable-demo
cargo test
```
