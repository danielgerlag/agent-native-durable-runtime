# Session checkpoint spec v0

Format name: `durable_session.checkpoint`. Encoding: JSON. A checkpoint is a directory.

The JSON Schema files in `spec/schema/` are normative. This page is the readable companion. If prose and schema disagree, the schema wins.

Readers that do not run the Rust crate parse these files. Python and TypeScript packages in this repository do that. They do not open SQLite and they do not take a lease.

## Layout

```
manifest.json
transcript.ndjson
trees/<sha256>.json
blobs/<aa>/<sha256>
views/ledger.json
workspace/
```

`transcript.ndjson` is the source of truth for conversation, tool calls, and workspace revisions. Paths listed in `manifest.derived` are convenience materializations. Importers ignore them and rebuild from the transcript plus blobs.

Lease state is not in the bundle. Ownership is a process fact.

## Schemas

| File | Validates |
| --- | --- |
| `spec/schema/manifest.schema.json` | `manifest.json` |
| `spec/schema/event.schema.json` | each line of `transcript.ndjson` |
| `spec/schema/tree.schema.json` | `trees/<sha256>.json` |
| `spec/schema/view.schema.json` | the canonical projection, not a file in the bundle |

The canonical view is what every language must emit from `load(path).view()`. Golden output lives next to the fixture as `fixtures/v0/<name>.view.json`.

## `manifest.json`

Required: `format` = `durable_session.checkpoint`, `checkpoint_version` = `0`, `session_id`, `event_head`, `files.transcript`, `derived`.

`event_head` is the `seq` of the last transcript line. `files.transcript` is a relative path with no `..` segments. `workspace_head` in `view()` is the last `workspace_snapshot` in the transcript. If the manifest also has a head, it must match.

`gen_ai.conversation.id` is the same string as `session_id`. That is an OpenTelemetry attribute name reused as a label.

## `transcript.ndjson`

One event per line, `seq` starting at 1 with no gaps. Discriminator is `type`. Envelope fields are `seq` and `t` (RFC3339).

| `type` | Required fields |
| --- | --- |
| `system` | `role`=`system`, `content`, `op` |
| `user` | `role`=`user`, `content`, `op` |
| `assistant_delta` | `turn`, and `text` and/or `tool_call` `{id, name, args_delta}` |
| `assistant_sealed` | `turn`, `finish_reason`, optional `gen_ai.request.model`, optional usage ints |
| `workspace_snapshot` | `rev`, `tree` (`sha256:<hex>`) |
| `tool_pending` | `gen_ai.tool.call.id`, `gen_ai.tool.name`, `args_hash`, `args`, `policy`, `workspace_rev` |
| `tool_applied` | `gen_ai.tool.call.id`, `workspace_rev`, optional `result_ref` or `result_text` |
| `tool_failed` | `gen_ai.tool.call.id`, `error` |
| `tool_abandoned` | `gen_ai.tool.call.id`, `reason` |

`finish_reason` is `stop` | `length` | `tool_calls` | `truncated_crash` | `error`.

`policy` is `idempotent` | `at_most_once`.

`args_hash` is lowercase hex SHA-256 of canonical JSON args (object keys sorted, no insignificant whitespace).

Unknown `type` is an error. A seq gap is an error.

## Trees and blobs

`trees/<sha256>.json` matches `tree.schema.json`. File bytes live at `blobs/<first two hex chars>/<full hex>`. The tree object is also a blob whose hash matches the filename (without `.json`).

## Import rules

1. Reject `checkpoint_version` other than `0`.
2. Read `transcript.ndjson` in order. Fail on an unknown `type` or a seq gap.
3. Ignore `derived` paths.
4. Do not take a lease.
5. The canonical view is folded from the transcript. Do not trust `views/`.
6. A `workspace_snapshot` or `result_ref` whose blob is missing is corrupt.

## Golden fixtures

`fixtures/v0/tools/` is a valid bundle. `fixtures/v0/tools.view.json` is the required `view()` output. `fixtures/v0/invalid-seq-gap/` and `fixtures/v0/invalid-version/` must fail to load.
