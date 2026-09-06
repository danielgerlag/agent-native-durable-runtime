import {
  assertNever,
  type AssistantToolCall,
  type CheckpointView,
  type Event,
  type LedgerRow,
  type Message,
  type SideEffectStatus,
  type WorkspaceHead,
} from "./types";

type UnsealedTurn = {
  turn: string;
  text: string;
  toolCalls: AssistantToolCall[];
  model: string | null;
};

type PendingTool = {
  callId: string;
  name: string;
  argsHash: string;
  policy: LedgerRow["policy"];
};

type CallRecord = {
  status: SideEffectStatus;
  resultRef: string | null;
  resultText: string | null;
  error: string | null;
};

type FoldState = {
  unsealed: UnsealedTurn | null;
  pending: PendingTool | null;
  messages: Message[];
  appliedByHash: Set<string>;
  calls: Map<string, CallRecord>;
};

function hashKey(name: string, argsHash: string): string {
  return `${name}\0${argsHash}`;
}

function chunkIsEmpty(event: Extract<Event, { type: "assistant_delta" }>): boolean {
  const textEmpty = event.text === undefined || event.text.length === 0;
  return textEmpty && event.tool_call === undefined;
}

function apply(state: FoldState, event: Event): void {
  switch (event.type) {
    case "user": {
      if (state.pending !== null) {
        throw new Error("assistant input while a tool is pending");
      }
      if (state.unsealed !== null) {
        throw new Error("unexpected turn");
      }
      state.messages.push({
        role: "user",
        content: event.content,
        op: event.op,
      });
      return;
    }
    case "system": {
      if (state.pending !== null) {
        throw new Error("assistant input while a tool is pending");
      }
      if (state.unsealed !== null) {
        throw new Error("unexpected turn");
      }
      state.messages.push({
        role: "system",
        content: event.content,
        op: event.op,
      });
      return;
    }
    case "assistant_delta": {
      if (state.pending !== null) {
        throw new Error("assistant input while a tool is pending");
      }
      if (chunkIsEmpty(event)) {
        throw new Error("empty assistant chunk");
      }
      if (state.unsealed === null) {
        const toolCalls: AssistantToolCall[] = [];
        if (event.tool_call !== undefined) {
          toolCalls.push({
            id: event.tool_call.id,
            name: event.tool_call.name,
            arguments: event.tool_call.args_delta,
          });
        }
        state.unsealed = {
          turn: event.turn,
          text: event.text ?? "",
          toolCalls,
          model: null,
        };
        return;
      }
      if (state.unsealed.turn !== event.turn) {
        throw new Error("unexpected turn");
      }
      if (event.text !== undefined) {
        state.unsealed.text += event.text;
      }
      if (event.tool_call !== undefined) {
        const delta = event.tool_call;
        const existing = state.unsealed.toolCalls.find((c) => c.id === delta.id);
        if (existing !== undefined) {
          existing.arguments += delta.args_delta;
          if (delta.name.length > 0) {
            existing.name = delta.name;
          }
        } else {
          state.unsealed.toolCalls.push({
            id: delta.id,
            name: delta.name,
            arguments: delta.args_delta,
          });
        }
      }
      return;
    }
    case "assistant_sealed": {
      if (state.unsealed === null || state.unsealed.turn !== event.turn) {
        throw new Error("unexpected turn");
      }
      const unsealed = state.unsealed;
      state.unsealed = null;
      state.messages.push({
        role: "assistant",
        content: unsealed.text,
        turn: unsealed.turn,
        finish_reason: event.finish_reason,
        model: event["gen_ai.request.model"] ?? unsealed.model,
        tool_calls: unsealed.toolCalls,
      });
      return;
    }
    case "workspace_snapshot":
      return;
    case "tool_pending": {
      if (state.unsealed !== null) {
        throw new Error("unexpected turn");
      }
      if (state.pending !== null) {
        throw new Error("two pending tools");
      }
      const callId = event["gen_ai.tool.call.id"];
      const name = event["gen_ai.tool.name"];
      if (state.calls.has(callId)) {
        throw new Error("duplicate tool call");
      }
      if (state.appliedByHash.has(hashKey(name, event.args_hash))) {
        throw new Error("duplicate tool call");
      }
      state.calls.set(callId, {
        status: "pending",
        resultRef: null,
        resultText: null,
        error: null,
      });
      state.pending = {
        callId,
        name,
        argsHash: event.args_hash,
        policy: event.policy,
      };
      return;
    }
    case "tool_applied":
      applyTerminal(state, event["gen_ai.tool.call.id"], "applied", {
        resultRef: event.result_ref ?? null,
        resultText: event.result_text ?? null,
        error: null,
      });
      return;
    case "tool_failed":
      applyTerminal(state, event["gen_ai.tool.call.id"], "failed", {
        resultRef: null,
        resultText: null,
        error: event.error,
      });
      return;
    case "tool_abandoned":
      applyTerminal(state, event["gen_ai.tool.call.id"], "abandoned", {
        resultRef: null,
        resultText: null,
        error: event.reason,
      });
      return;
    default:
      assertNever(event);
  }
}

function applyTerminal(
  state: FoldState,
  callId: string,
  status: SideEffectStatus,
  outcome: {
    resultRef: string | null;
    resultText: string | null;
    error: string | null;
  },
): void {
  if (state.pending !== null && state.pending.callId === callId) {
    const pending = state.pending;
    state.pending = null;
    const record = state.calls.get(callId);
    if (record !== undefined) {
      record.status = status;
      record.resultRef = outcome.resultRef;
      record.resultText = outcome.resultText;
      record.error = outcome.error;
    }
    if (status === "applied") {
      state.appliedByHash.add(hashKey(pending.name, pending.argsHash));
    }
    state.messages.push({
      role: "tool",
      call_id: callId,
      name: pending.name,
      status,
      result_text: outcome.resultText,
      error: outcome.error,
    });
    return;
  }
  if (state.pending !== null) {
    throw new Error("unknown tool call");
  }
  const existing = state.calls.get(callId);
  if (existing !== undefined) {
    if (existing.status === "applied" && status === "applied") {
      const same =
        existing.resultRef === outcome.resultRef &&
        existing.resultText === outcome.resultText;
      if (same) {
        return;
      }
      throw new Error("complete_tool outcome does not match recorded Applied");
    }
    throw new Error("tool call is not pending");
  }
  throw new Error("unknown tool call");
}

export function foldMessages(events: Event[]): Message[] {
  const state: FoldState = {
    unsealed: null,
    pending: null,
    messages: [],
    appliedByHash: new Set(),
    calls: new Map(),
  };
  for (const event of events) {
    apply(state, event);
  }
  return state.messages;
}

export function foldLedger(events: Event[]): LedgerRow[] {
  const rows: LedgerRow[] = [];
  for (const event of events) {
    switch (event.type) {
      case "tool_pending":
        rows.push({
          call_id: event["gen_ai.tool.call.id"],
          tool: event["gen_ai.tool.name"],
          args_hash: event.args_hash,
          status: "pending",
          policy: event.policy,
          result_ref: null,
        });
        break;
      case "tool_applied": {
        const row = rows.find(
          (r) => r.call_id === event["gen_ai.tool.call.id"],
        );
        if (row !== undefined) {
          row.status = "applied";
          row.result_ref = event.result_ref ?? null;
        }
        break;
      }
      case "tool_failed": {
        const row = rows.find(
          (r) => r.call_id === event["gen_ai.tool.call.id"],
        );
        if (row !== undefined) {
          row.status = "failed";
        }
        break;
      }
      case "tool_abandoned": {
        const row = rows.find(
          (r) => r.call_id === event["gen_ai.tool.call.id"],
        );
        if (row !== undefined) {
          row.status = "abandoned";
        }
        break;
      }
      case "system":
      case "user":
      case "assistant_delta":
      case "assistant_sealed":
      case "workspace_snapshot":
        break;
      default:
        assertNever(event);
    }
  }
  return rows;
}

function messageView(message: Message): Message {
  switch (message.role) {
    case "system":
      return {
        role: "system",
        content: message.content,
        op: message.op,
      };
    case "user":
      return {
        role: "user",
        content: message.content,
        op: message.op,
      };
    case "assistant":
      return {
        role: "assistant",
        content: message.content,
        turn: message.turn,
        finish_reason: message.finish_reason,
        model: message.model,
        tool_calls: message.tool_calls.map((c) => ({
          id: c.id,
          name: c.name,
          arguments: c.arguments,
        })),
      };
    case "tool":
      return {
        role: "tool",
        call_id: message.call_id,
        name: message.name,
        status: message.status,
        result_text: message.result_text,
        error: message.error,
      };
    default:
      return assertNever(message);
  }
}

export function foldView(
  sessionId: string,
  eventHead: number,
  workspaceHead: WorkspaceHead | null,
  messages: Message[],
  events: Event[],
): CheckpointView {
  return {
    session_id: sessionId,
    event_head: eventHead,
    workspace_head:
      workspaceHead === null
        ? null
        : { rev: workspaceHead.rev, tree: workspaceHead.tree },
    messages: messages.map(messageView),
    ledger: foldLedger(events),
  };
}
