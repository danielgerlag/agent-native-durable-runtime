# Agent-native durable runtime

## The wedge (1–2 paragraphs)

Agent sessions are not DAGs. They are long-lived, streaming, non-deterministic loops that mutate a filesystem workspace, call tools with irreversible side effects, and expect a human (or another agent) to resume mid-turn. Classic durable-execution engines—Temporal foremost—can host agents, but only after you quarantine every LLM and tool call into Activities, keep Workflow code deterministic, and fight history/blob-size limits as transcripts grow. That tax is why Failproof’s Exosphere and similar agent-first runtimes exist: graph/state models that persist agent progress without replaying orchestration code like a Temporal Workflow.

The open wedge is a **small, harness-embeddable runtime** that checkpoints the three things agents actually need—(1) conversation/tool transcript, (2) workspace filesystem snapshot or diff, (3) tool side-effect ledger—then resumes on sticky workers with streaming continuity. Ship it as a library + optional local daemon that Hivemind OS, Claude Code-style harnesses, and DIY agents can adopt without operating a Temporal cluster.

## Problem

When an agent crashes mid-session today, recovery is usually one of:

- **Restart from scratch** — wastes tokens, duplicates side effects (duplicate PRs, double emails, re-run migrations).
- **Ad-hoc checkpoint JSON** — teams invent their own “save state” blobs; none are portable across harnesses.
- **Framework checkpointers** (e.g. LangGraph thread checkpoints) — good for graph state, weak on FS workspace + streaming resume + sticky placement.
- **Temporal / Restate / DBOS** — strong exactly-once story if you accept deterministic Workflows, Activity wrapping, Continue-as-New for long histories, and claim-check patterns for large payloads.

Agents break Temporal’s mental model in practice: LLM outputs are non-deterministic (replay trap if put in Workflow code), tool transcripts balloon past history/gRPC limits, and “resume streaming the same SSE turn after a worker death” is not a first-class primitive. Exosphere documents a pull-based state manager + runtimes with graph templates, retries, and native state persistence aimed at agentic flows—evidence that builders are already rejecting “just use Temporal” for this niche.

Missing piece: an **agent-native** durability contract that treats transcript + workspace + side effects as one atomic session object, with sticky workers and stream resume, designed to embed in local harnesses.

## Why now (2025–2026)

- Coding agents and desktop harnesses run multi-hour sessions with large workspaces; crash recovery is a product feature, not ops trivia.
- Temporal’s own guidance and community write-ups emphasize Activities for LLM calls and External Storage / Continue-as-New for growing histories—friction that OSS harness authors feel immediately.
- Exosphere (FailproofAI/runtime) and peer “reliability runtimes for AI agents” validate demand for agent-shaped durable execution outside enterprise workflow platforms.
- Local-first harnesses (Hivemind OS and peers) need durability that works offline with SQLite/object store on disk—not a multi-service Temporal cluster.
- Streaming UX is table stakes: users expect the same turn to continue after a reconnect, not a silent full replay.

## Who it's for

- OSS harness authors (Hivemind OS, Goose, custom agent loops) who want crash-safe sessions without adopting a full workflow platform.
- Teams running many concurrent coding agents who need sticky workers (same machine/workspace affinity) and clean resume after OOM/kill.
- Plugin/MCP tool authors who want a standard side-effect ledger so tools can declare compensations or “already applied” markers.
- Researchers comparing harnesses who need reproducible session artifacts (checkpoint bundles) rather than opaque vendor logs.

## Differentiation vs existing options

| Option | Fit for agents | Gap |
| --- | --- | --- |
| Temporal | Production-proven; Codex-scale usage claimed | Determinism tax, history size, ops weight, weak streaming-resume UX |
| LangGraph checkpointers | Easy in-graph | Not harness-portable; FS/side-effect story incomplete |
| Exosphere | Agent-oriented graph runtime | Full product/runtime; not a minimal embeddable session library |
| DIY SQLite session tables | Common in local apps | No shared schema, no sticky workers, no stream protocol |

**Differentiation:** publish a **Session Checkpoint Spec** + reference runtime (Rust or Go + thin Python/TS bindings) focused on local/dev harnesses: checkpoint bundles, sticky lease, streaming resume tokens—not another general workflow engine.

## Crowding / difficulty

- **Crowding:** Medium. Temporal owns “durable execution” mindshare; LangGraph owns framework-local checkpoints; Exosphere occupies “agent reliability runtime.” Few projects own the *minimal embeddable* niche for local harnesses.
- **Difficulty:** High for correctness (exactly-once tool semantics, partial FS writes, stream idempotency). Medium for an MVP that is “best-effort resume with explicit side-effect log” rather than distributed transactions.
- **Moat:** Spec adoption + reference integrations into 2–3 popular harnesses beats feature breadth.

## Suggested MVP (concrete scope for a first OSS release)

1. **Checkpoint format (v0):** JSON/CBOR manifest with transcript events (OpenTelemetry GenAI-aligned where possible), workspace diff (git-style or tar snapshot), and side-effect entries `{tool, args_hash, result_ref, status}`.
2. **Local store:** SQLite + filesystem blob dir; `session.save()` / `session.restore(id)`.
3. **Sticky worker lease:** single-node lease file or SQLite row; heartbeats; steal only after TTL.
4. **Streaming resume:** opaque `resume_token` so a client can reconnect mid-assistant-message without re-prompting the model for completed prefixes (store partial assistant chunks in the transcript).
5. **Reference loop:** 200-line ReAct demo that survives `kill -9` mid-tool and resumes without re-calling completed tools.
6. **Non-goal for MVP:** multi-region replication, Temporal compatibility bridge, GUI.

Ship under Apache-2.0/MIT with a conformance test suite (“crash at N random points, assert no duplicate side effects for marked-idempotent tools”).

## Fit for Hivemind OS / local harnesses

Hivemind OS already runs a local Rust daemon with chat sessions, tools/MCP, and SQLite-backed memory. An agent-native durable runtime maps cleanly to:

- Persisting daemon agent loops across app restarts and OS sleep.
- Resuming background bots without re-planning from scratch.
- Exporting portable checkpoint bundles for debugging and community evals.
- Keeping durability **local-first** (no Temporal dependency)—aligned with Hivemind’s privacy and install story.

Strategic options: (a) extract Hivemind’s session persistence into this shared crate and dogfood it; (b) adopt an external OSS runtime early and contribute the FS+privacy hooks others lack.

## Risks & non-goals

- **Risks:** Competing with Temporal’s brand; under-specifying side-effect semantics leading to unsafe “resume”; scope creep into full workflow product.
- **Non-goals:** Replacing Temporal for enterprise saga orchestration; guaranteeing exactly-once for non-cooperative third-party APIs; cloud multi-tenant hosting as the primary delivery model.
- **Mitigation:** Position as “ulimit/checkpoint layer for agent sessions,” integrate *with* Temporal optionally later via an adapter—not as a Temporal clone.

## Sources

- Failproof Exosphere docs: https://docs.exosphere.host/
- FailproofAI/runtime (Exosphere): https://github.com/FailproofAI/runtime
- Exosphere architecture: https://docs.exosphere.host/exosphere/architecture/
- Temporal on dynamic AI agents: https://temporal.io/blog/of-course-you-can-build-dynamic-ai-agents-with-temporal
- Temporal External Storage (large agent payloads): https://docs.temporal.io/external-storage
- Temporal blob size limits: https://docs.temporal.io/troubleshooting/blob-size-limit-error
- Replay trap analysis: https://dreaming.press/posts/resume-crashed-ai-agent-durable-execution-replay-trap.html
- Durable execution comparison (Temporal / Restate / DBOS): https://alatirok.com/durable-execution-ai-agents-compared/
- Hivemind OS: https://hivemind-os.io/
- Hivemind OS how it works: https://hivemind-os.io/concepts/how-it-works
- hivemind-os/hivemind: https://github.com/hivemind-os/hivemind
