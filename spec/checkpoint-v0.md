# Session checkpoint spec v0

Format name: `durable_session.checkpoint`. Encoding: JSON. This document is reference. It describes the on-disk bundle, not how to write a harness.

A checkpoint is a directory. Zip it if you need a single file. Readers that do not run this crate can still parse the files.

## Layout

```
manifest.json
transcript.ndjson
trees/<sha256>.json
blobs/<aa>/<sha256>
views/ledger.json
workspace/
```

`transcript.ndjson` is the source of truth for conversation, tool calls, and workspace revisions. `views/` and `workspace/` are derived. Importers ignore derived files and rebuild them from the transcript plus blobs.

Lease state is not part of the bundle. Ownership is a process fact.

## `manifest.json`

| Field | Type | Meaning |
| --- | --- | --- |
| `format` | string | `durable_session.checkpoint` |
| `checkpoint_version` | u32 | `0` |
| `session_id` | string | Session id |
| `gen_ai.conversation.id` | string | Same value as `session_id`. OTel attribute name reused as a label. |
| `created_at` | string | RFC3339 of session creation |
| `exported_at` | string | RFC3339 of this export |
| `event_head` | u64 | Seq of the last event in `transcript.ndjson` |
| `workspace_head` | object or null | `{ "rev": u64, "tree": "sha256:<hex>" }` |
| `files` | object | Relative paths of the other files |
| `derived` | string[] | Paths the importer must ignore |

Example:

```json
{
  "format": "durable_session.checkpoint",
  "checkpoint_version": 0,
  "session_id": "sess_demo",
  "gen_ai.conversation.id": "sess_demo",
  "created_at": "2026-09-05T12:00:00.000Z",
  "exported_at": "2026-09-05T12:05:00.000Z",
  "event_head": 8,
  "workspace_head": { "rev": 2, "tree": "sha256:ab" },
  "files": {
    "transcript": "transcript.ndjson",
    "trees": "trees/",
    "blobs": "blobs/",
    "workspace": "workspace/",
    "ledger_view": "views/ledger.json"
  },
  "derived": ["workspace/", "views/ledger.json"]
}
```

## `transcript.ndjson`

One event per line, seq order, discriminator `type`. Field names reuse OpenTelemetry GenAI attribute names where they name the same fact. The transcript is not an OTel `gen_ai.client.inference.operation.details` payload. That event is one inference operation. This file is the session log.

Common envelope: `seq` (u64, starts at 1), `t` (RFC3339).

| `type` | Fields |
| --- | --- |
| `system` | `role`=`system`, `content`, `op` |
| `user` | `role`=`user`, `content`, `op` |
| `assistant_delta` | `turn`, `text` and/or `tool_call` `{id, name, args_delta}` |
| `assistant_sealed` | `turn`, `finish_reason`, optional `gen_ai.request.model`, optional usage ints |
| `workspace_snapshot` | `rev`, `tree` (`sha256:<hex>`) |
| `tool_pending` | `gen_ai.tool.call.id`, `gen_ai.tool.name`, `args_hash`, `args`, `policy`, `workspace_rev` |
| `tool_applied` | `gen_ai.tool.call.id`, `result_ref` or `result_text`, `workspace_rev` |
| `tool_failed` | `gen_ai.tool.call.id`, `error` |
| `tool_abandoned` | `gen_ai.tool.call.id`, `reason` |

`finish_reason` is `stop` | `length` | `tool_calls` | `truncated_crash` | `error`.

`policy` is `idempotent` | `at_most_once`.

`args_hash` is lowercase hex SHA-256 of canonical JSON args (object keys sorted, no insignificant whitespace).

## Trees and blobs

`trees/<sha256>.json`:

```json
{
  "v": 0,
  "entries": [
    { "path": "README.md", "type": "file", "blob": "sha256:…", "mode": 33188, "size": 120 }
  ]
}
```

`type` is `file` or `symlink`. File bytes live at `blobs/<first two hex chars>/<full hex>`. The tree object itself is also a blob whose hash matches the filename.

## `views/ledger.json`

Convenience projection. Importers ignore it.

```json
[
  {
    "tool": "write_file",
    "args_hash": "…",
    "result_ref": "sha256:…",
    "status": "applied",
    "gen_ai.tool.call.id": "c1",
    "policy": "idempotent"
  }
]
```

`status` is `pending` | `applied` | `failed` | `abandoned`.

## Import rules

1. Reject `checkpoint_version` other than `0`.
2. Read `transcript.ndjson` in order. Parse each line into a domain event. Fail the import on an unknown `type` or a seq gap.
3. Copy blobs by hash. A referenced hash that is missing is corrupt.
4. Ignore `derived` paths.
5. Do not take a lease. The next `Session::open` for that `session_id` is resume.
