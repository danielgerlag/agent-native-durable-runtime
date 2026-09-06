from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Union

FORMAT = "durable_session.checkpoint"
_SESSION_ID = re.compile(r"^[A-Za-z0-9_-]+$")
GEN_AI_TOOL_CALL_ID = "gen_ai.tool.call.id"
GEN_AI_TOOL_NAME = "gen_ai.tool.name"
GEN_AI_REQUEST_MODEL = "gen_ai.request.model"
GEN_AI_USAGE_INPUT_TOKENS = "gen_ai.usage.input_tokens"
GEN_AI_USAGE_OUTPUT_TOKENS = "gen_ai.usage.output_tokens"

_FINISH_REASONS = frozenset({"stop", "length", "tool_calls", "truncated_crash", "error"})
_POLICIES = frozenset({"idempotent", "at_most_once"})
_HEX = frozenset("0123456789abcdefABCDEF")


class CheckpointError(Exception):
    """The bundle is not a valid durable_session checkpoint v0."""


@dataclass(frozen=True)
class ToolCallDelta:
    id: str
    name: str
    args_delta: str = ""


@dataclass(frozen=True)
class ToolCall:
    id: str
    name: str
    arguments: str


@dataclass(frozen=True)
class SystemEvent:
    seq: int
    t: str
    content: str
    op: str
    type: str = "system"
    role: str = "system"


@dataclass(frozen=True)
class UserEvent:
    seq: int
    t: str
    content: str
    op: str
    type: str = "user"
    role: str = "user"


@dataclass(frozen=True)
class AssistantDeltaEvent:
    seq: int
    t: str
    turn: str
    text: str | None = None
    tool_call: ToolCallDelta | None = None
    type: str = "assistant_delta"


@dataclass(frozen=True)
class AssistantSealedEvent:
    seq: int
    t: str
    turn: str
    finish_reason: str
    model: str | None = None
    input_tokens: int | None = None
    output_tokens: int | None = None
    type: str = "assistant_sealed"


@dataclass(frozen=True)
class WorkspaceSnapshotEvent:
    seq: int
    t: str
    rev: int
    tree: str
    type: str = "workspace_snapshot"


@dataclass(frozen=True)
class ToolPendingEvent:
    seq: int
    t: str
    call_id: str
    name: str
    args_hash: str
    args: Any
    policy: str
    workspace_rev: int
    type: str = "tool_pending"


@dataclass(frozen=True)
class ToolAppliedEvent:
    seq: int
    t: str
    call_id: str
    workspace_rev: int
    result_ref: str | None = None
    result_text: str | None = None
    type: str = "tool_applied"


@dataclass(frozen=True)
class ToolFailedEvent:
    seq: int
    t: str
    call_id: str
    error: str
    type: str = "tool_failed"


@dataclass(frozen=True)
class ToolAbandonedEvent:
    seq: int
    t: str
    call_id: str
    reason: str
    type: str = "tool_abandoned"


Event = Union[
    SystemEvent,
    UserEvent,
    AssistantDeltaEvent,
    AssistantSealedEvent,
    WorkspaceSnapshotEvent,
    ToolPendingEvent,
    ToolAppliedEvent,
    ToolFailedEvent,
    ToolAbandonedEvent,
]


@dataclass(frozen=True)
class SystemMessage:
    content: str
    op: str
    role: str = "system"


@dataclass(frozen=True)
class UserMessage:
    content: str
    op: str
    role: str = "user"


@dataclass(frozen=True)
class AssistantMessage:
    content: str
    turn: str
    finish_reason: str
    tool_calls: tuple[ToolCall, ...] = field(default_factory=tuple)
    model: str | None = None
    role: str = "assistant"


@dataclass(frozen=True)
class ToolMessage:
    call_id: str
    name: str
    status: str
    result_text: str | None = None
    error: str | None = None
    role: str = "tool"


Message = Union[SystemMessage, UserMessage, AssistantMessage, ToolMessage]


@dataclass
class Checkpoint:
    session_id: str
    events: list[Event]
    messages: list[Message]
    event_head: int
    workspace_head: dict[str, Any] | None

    @classmethod
    def load(cls, path: Path | str) -> Checkpoint:
        root = Path(path)
        try:
            raw = (root / "manifest.json").read_text(encoding="utf-8")
        except OSError as e:
            raise CheckpointError(f"cannot read manifest.json: {e}") from e
        try:
            manifest = json.loads(raw)
        except json.JSONDecodeError as e:
            raise CheckpointError(f"invalid JSON: {e}") from e
        if not isinstance(manifest, dict):
            raise CheckpointError("manifest must be an object")

        fmt = manifest.get("format")
        if not isinstance(fmt, str):
            fmt = ""
        if fmt != FORMAT:
            raise CheckpointError(f"unknown format {fmt}")

        version = manifest.get("checkpoint_version")
        if not _is_int(version) or version != 0:
            raise CheckpointError(f"unsupported checkpoint_version {version}")

        sid = manifest.get("session_id")
        if not isinstance(sid, str) or not _SESSION_ID.fullmatch(sid):
            raise CheckpointError("invalid session id")

        transcript_name = "transcript.ndjson"
        files = manifest.get("files")
        if isinstance(files, dict):
            tname = files.get("transcript")
            if isinstance(tname, str) and tname:
                transcript_name = tname

        events = _read_transcript(_bundle_path(root, transcript_name))
        messages = _fold_messages(events)

        last = events[-1].seq if events else 0
        event_head = manifest.get("event_head")
        if event_head is None:
            event_head = last
        elif not _is_int(event_head):
            raise CheckpointError("manifest missing event_head")
        if event_head != last:
            raise CheckpointError(
                f"event_head {event_head} does not match transcript"
            )

        manifest_head = _workspace_head(manifest.get("workspace_head"))
        folded_head = _folded_workspace_head(events)
        if manifest_head is None:
            workspace_head = folded_head
        elif folded_head == manifest_head:
            workspace_head = folded_head
        else:
            raise CheckpointError("workspace_head does not match transcript")

        for ev in events:
            _ensure_referenced_blob(root, ev)

        return cls(
            session_id=sid,
            events=events,
            messages=messages,
            event_head=event_head,
            workspace_head=workspace_head,
        )

    def view(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "event_head": self.event_head,
            "workspace_head": None
            if self.workspace_head is None
            else {
                "rev": self.workspace_head["rev"],
                "tree": self.workspace_head["tree"],
            },
            "messages": [_message_view(m) for m in self.messages],
            "ledger": _ledger_view(self.events),
        }


def load(path: Path | str) -> Checkpoint:
    return Checkpoint.load(path)


def _is_int(v: Any) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


def _req_str(obj: dict[str, Any], key: str) -> str:
    v = obj.get(key)
    if not isinstance(v, str):
        raise CheckpointError(f"missing field {key}")
    return v


def _req_hex64(obj: dict[str, Any], key: str) -> str:
    v = _req_str(obj, key)
    if len(v) != 64 or any(c not in _HEX for c in v):
        raise CheckpointError(f"missing field {key}")
    return v.lower()


def _req_int(obj: dict[str, Any], key: str) -> int:
    v = obj.get(key)
    if not _is_int(v):
        raise CheckpointError(f"missing field {key}")
    return v


def _opt_str(obj: dict[str, Any], key: str) -> str | None:
    v = obj.get(key)
    if v is None:
        return None
    if not isinstance(v, str):
        raise CheckpointError(f"missing field {key}")
    return v


def _opt_int(obj: dict[str, Any], key: str) -> int | None:
    if key not in obj or obj[key] is None:
        return None
    return _req_int(obj, key)


def _blob_ref(s: str) -> str:
    hexpart = s[7:] if s.startswith("sha256:") else s
    if len(hexpart) != 64 or any(c not in _HEX for c in hexpart):
        raise CheckpointError(f"invalid blob ref {s}")
    return f"sha256:{hexpart.lower()}"


def _workspace_head(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise CheckpointError("workspace_head missing rev")
    rev = value.get("rev")
    tree = value.get("tree")
    if not _is_int(rev):
        raise CheckpointError("workspace_head missing rev")
    if not isinstance(tree, str):
        raise CheckpointError("workspace_head missing tree")
    _blob_ref(tree)
    return {"rev": rev, "tree": tree}


def _read_transcript(path: Path) -> list[Event]:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as e:
        raise CheckpointError(f"cannot read transcript: {e}") from e
    events: list[Event] = []
    expect = 1
    for line in text.splitlines():
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError as e:
            raise CheckpointError(f"invalid JSON: {e}") from e
        if not isinstance(obj, dict):
            raise CheckpointError("event must be an object")
        seq = obj.get("seq")
        if not _is_int(seq):
            raise CheckpointError("event missing seq")
        if seq != expect:
            raise CheckpointError(f"seq gap: expected {expect}, got {seq}")
        expect += 1
        t = obj.get("t")
        if not isinstance(t, str) or "T" not in t.upper():
            raise CheckpointError("event missing t")
        events.append(_wire_to_event(obj))
    return events


def _wire_to_event(obj: dict[str, Any]) -> Event:
    typ = obj.get("type")
    if not isinstance(typ, str):
        raise CheckpointError("event missing type")
    seq = obj["seq"]
    t = obj["t"]
    if typ == "user":
        return UserEvent(
            seq=seq,
            t=t,
            content=_req_str(obj, "content"),
            op=_req_str(obj, "op"),
        )
    if typ == "system":
        return SystemEvent(
            seq=seq,
            t=t,
            content=_req_str(obj, "content"),
            op=_req_str(obj, "op"),
        )
    if typ == "assistant_delta":
        text = obj["text"] if isinstance(obj.get("text"), str) else None
        tool_call = _tool_call_delta(obj.get("tool_call"))
        if text is None and tool_call is None:
            raise CheckpointError("assistant_delta requires text and/or tool_call")
        return AssistantDeltaEvent(
            seq=seq,
            t=t,
            turn=_req_str(obj, "turn"),
            text=text,
            tool_call=tool_call,
        )
    if typ == "assistant_sealed":
        reason = _req_str(obj, "finish_reason")
        if reason not in _FINISH_REASONS:
            raise CheckpointError(f"unknown finish reason {reason}")
        return AssistantSealedEvent(
            seq=seq,
            t=t,
            turn=_req_str(obj, "turn"),
            finish_reason=reason,
            model=_opt_str(obj, GEN_AI_REQUEST_MODEL),
            input_tokens=_opt_int(obj, GEN_AI_USAGE_INPUT_TOKENS),
            output_tokens=_opt_int(obj, GEN_AI_USAGE_OUTPUT_TOKENS),
        )
    if typ == "workspace_snapshot":
        return WorkspaceSnapshotEvent(
            seq=seq,
            t=t,
            rev=_req_int(obj, "rev"),
            tree=_blob_ref(_req_str(obj, "tree")),
        )
    if typ == "tool_pending":
        policy = _req_str(obj, "policy")
        if policy not in _POLICIES:
            raise CheckpointError(f"unknown tool policy {policy}")
        return ToolPendingEvent(
            seq=seq,
            t=t,
            call_id=_req_str(obj, GEN_AI_TOOL_CALL_ID),
            name=_req_str(obj, GEN_AI_TOOL_NAME),
            args_hash=_req_hex64(obj, "args_hash"),
            args=obj.get("args"),
            policy=policy,
            workspace_rev=_req_int(obj, "workspace_rev"),
        )
    if typ == "tool_applied":
        result_ref = _opt_str(obj, "result_ref")
        if result_ref is not None:
            result_ref = _blob_ref(result_ref)
        return ToolAppliedEvent(
            seq=seq,
            t=t,
            call_id=_req_str(obj, GEN_AI_TOOL_CALL_ID),
            workspace_rev=_req_int(obj, "workspace_rev"),
            result_ref=result_ref,
            result_text=_opt_str(obj, "result_text"),
        )
    if typ == "tool_failed":
        return ToolFailedEvent(
            seq=seq,
            t=t,
            call_id=_req_str(obj, GEN_AI_TOOL_CALL_ID),
            error=_req_str(obj, "error"),
        )
    if typ == "tool_abandoned":
        return ToolAbandonedEvent(
            seq=seq,
            t=t,
            call_id=_req_str(obj, GEN_AI_TOOL_CALL_ID),
            reason=_req_str(obj, "reason"),
        )
    raise CheckpointError(f"unknown event type {typ}")


def _tool_call_delta(value: Any) -> ToolCallDelta | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise CheckpointError("missing field tool_call")
    args = value.get("args_delta")
    return ToolCallDelta(
        id=_req_str(value, "id"),
        name=_req_str(value, "name"),
        args_delta=args if isinstance(args, str) else "",
    )


@dataclass
class _CallAcc:
    id: str
    name: str
    arguments: str


@dataclass
class _Unsealed:
    turn: str
    text: str
    tool_calls: list[_CallAcc]
    model: str | None = None


def _bundle_path(root: Path, rel: str) -> Path:
    rel_path = Path(rel)
    if rel_path.is_absolute() or any(part == ".." for part in rel_path.parts):
        raise CheckpointError("path escapes bundle")
    return root / rel_path


def _folded_workspace_head(events: list[Event]) -> dict[str, Any] | None:
    for ev in reversed(events):
        if isinstance(ev, WorkspaceSnapshotEvent):
            return {"rev": ev.rev, "tree": ev.tree}
    return None


def _ensure_referenced_blob(root: Path, ev: Event) -> None:
    uri: str | None = None
    if isinstance(ev, WorkspaceSnapshotEvent):
        uri = ev.tree
    elif isinstance(ev, ToolAppliedEvent):
        uri = ev.result_ref
    if uri is None:
        return
    hex_part = uri[7:] if uri.startswith("sha256:") else uri
    if len(hex_part) < 2:
        raise CheckpointError(f"invalid blob ref {uri}")
    path = _bundle_path(root, f"blobs/{hex_part[:2]}/{hex_part}")
    if not path.is_file():
        raise CheckpointError(f"missing blob {uri}")


def _fold_messages(events: list[Event]) -> list[Message]:
    messages: list[Message] = []
    unsealed: _Unsealed | None = None
    pending: ToolPendingEvent | None = None
    calls: set[str] = set()
    applied_by_hash: set[tuple[str, str]] = set()
    for ev in events:
        if isinstance(ev, (SystemEvent, UserEvent)):
            if pending is not None:
                raise CheckpointError("assistant input while a tool is pending")
            if unsealed is not None:
                raise CheckpointError("unexpected turn")
            if isinstance(ev, SystemEvent):
                messages.append(SystemMessage(content=ev.content, op=ev.op))
            else:
                messages.append(UserMessage(content=ev.content, op=ev.op))
        elif isinstance(ev, AssistantDeltaEvent):
            if pending is not None:
                raise CheckpointError("assistant input while a tool is pending")
            text_empty = ev.text is None or ev.text == ""
            if text_empty and ev.tool_call is None:
                raise CheckpointError("empty assistant chunk")
            unsealed = _apply_delta(unsealed, ev)
        elif isinstance(ev, AssistantSealedEvent):
            if unsealed is None or unsealed.turn != ev.turn:
                raise CheckpointError("unexpected turn")
            model = ev.model if ev.model is not None else unsealed.model
            messages.append(
                AssistantMessage(
                    content=unsealed.text,
                    turn=unsealed.turn,
                    finish_reason=ev.finish_reason,
                    model=model,
                    tool_calls=tuple(
                        ToolCall(id=c.id, name=c.name, arguments=c.arguments)
                        for c in unsealed.tool_calls
                    ),
                )
            )
            unsealed = None
        elif isinstance(ev, ToolPendingEvent):
            if unsealed is not None:
                raise CheckpointError("unexpected turn")
            if pending is not None:
                raise CheckpointError("two pending tools")
            if ev.call_id in calls:
                raise CheckpointError("duplicate tool call")
            if (ev.name, ev.args_hash) in applied_by_hash:
                raise CheckpointError("duplicate tool call")
            pending = ev
            calls.add(ev.call_id)
        elif isinstance(ev, ToolAppliedEvent):
            _apply_terminal(
                messages,
                pending,
                applied_by_hash,
                ev.call_id,
                "applied",
                ev.result_text,
                None,
            )
            pending = None
        elif isinstance(ev, ToolFailedEvent):
            _apply_terminal(
                messages,
                pending,
                applied_by_hash,
                ev.call_id,
                "failed",
                None,
                ev.error,
            )
            pending = None
        elif isinstance(ev, ToolAbandonedEvent):
            _apply_terminal(
                messages,
                pending,
                applied_by_hash,
                ev.call_id,
                "abandoned",
                None,
                ev.reason,
            )
            pending = None
        elif isinstance(ev, WorkspaceSnapshotEvent):
            continue
    return messages


def _apply_terminal(
    messages: list[Message],
    pending: ToolPendingEvent | None,
    applied_by_hash: set[tuple[str, str]],
    call_id: str,
    status: str,
    result_text: str | None,
    error: str | None,
) -> None:
    if pending is None or pending.call_id != call_id:
        raise CheckpointError("unknown tool call")
    if status == "applied":
        applied_by_hash.add((pending.name, pending.args_hash))
    messages.append(
        ToolMessage(
            call_id=call_id,
            name=pending.name,
            status=status,
            result_text=result_text,
            error=error,
        )
    )


def _apply_delta(unsealed: _Unsealed | None, ev: AssistantDeltaEvent) -> _Unsealed:
    if unsealed is None:
        calls: list[_CallAcc] = []
        if ev.tool_call is not None:
            calls.append(
                _CallAcc(
                    id=ev.tool_call.id,
                    name=ev.tool_call.name,
                    arguments=ev.tool_call.args_delta,
                )
            )
        return _Unsealed(turn=ev.turn, text=ev.text or "", tool_calls=calls)
    if unsealed.turn != ev.turn:
        raise CheckpointError("unexpected turn")
    if ev.text:
        unsealed.text += ev.text
    if ev.tool_call is not None:
        existing = next(
            (c for c in unsealed.tool_calls if c.id == ev.tool_call.id), None
        )
        if existing is not None:
            existing.arguments += ev.tool_call.args_delta
            if ev.tool_call.name:
                existing.name = ev.tool_call.name
        else:
            unsealed.tool_calls.append(
                _CallAcc(
                    id=ev.tool_call.id,
                    name=ev.tool_call.name,
                    arguments=ev.tool_call.args_delta,
                )
            )
    return unsealed


def _message_view(message: Message) -> dict[str, Any]:
    if isinstance(message, SystemMessage):
        return {"role": "system", "content": message.content, "op": message.op}
    if isinstance(message, UserMessage):
        return {"role": "user", "content": message.content, "op": message.op}
    if isinstance(message, AssistantMessage):
        return {
            "role": "assistant",
            "content": message.content,
            "turn": message.turn,
            "finish_reason": message.finish_reason,
            "model": message.model,
            "tool_calls": [
                {"id": c.id, "name": c.name, "arguments": c.arguments}
                for c in message.tool_calls
            ],
        }
    return {
        "role": "tool",
        "call_id": message.call_id,
        "name": message.name,
        "status": message.status,
        "result_text": message.result_text,
        "error": message.error,
    }


def _ledger_view(events: list[Event]) -> list[dict[str, Any]]:
    # Fold from the transcript. Do not read views/ledger.json.
    rows: list[dict[str, Any]] = []
    for ev in events:
        if isinstance(ev, ToolPendingEvent):
            rows.append(
                {
                    "call_id": ev.call_id,
                    "tool": ev.name,
                    "args_hash": ev.args_hash,
                    "status": "pending",
                    "policy": ev.policy,
                    "result_ref": None,
                }
            )
        elif isinstance(ev, ToolAppliedEvent):
            row = _find_row(rows, ev.call_id)
            if row is not None:
                row["status"] = "applied"
                row["result_ref"] = ev.result_ref
        elif isinstance(ev, ToolFailedEvent):
            row = _find_row(rows, ev.call_id)
            if row is not None:
                row["status"] = "failed"
        elif isinstance(ev, ToolAbandonedEvent):
            row = _find_row(rows, ev.call_id)
            if row is not None:
                row["status"] = "abandoned"
    return rows


def _find_row(rows: list[dict[str, Any]], call_id: str) -> dict[str, Any] | None:
    for row in rows:
        if row.get("call_id") == call_id:
            return row
    return None
