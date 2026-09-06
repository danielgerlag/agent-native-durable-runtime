export type JsonValue =
  | null
  | boolean
  | number
  | string
  | JsonValue[]
  | { [key: string]: JsonValue };

export const FINISH_REASONS = [
  "stop",
  "length",
  "tool_calls",
  "truncated_crash",
  "error",
] as const;
export type FinishReason = (typeof FINISH_REASONS)[number];

export const TOOL_POLICIES = ["idempotent", "at_most_once"] as const;
export type ToolPolicy = (typeof TOOL_POLICIES)[number];

export const SIDE_EFFECT_STATUSES = [
  "pending",
  "applied",
  "failed",
  "abandoned",
] as const;
export type SideEffectStatus = (typeof SIDE_EFFECT_STATUSES)[number];

export type Envelope = {
  seq: number;
  t: string;
};

export type SystemEvent = Envelope & {
  type: "system";
  role: "system";
  content: string;
  op: string;
};

export type UserEvent = Envelope & {
  type: "user";
  role: "user";
  content: string;
  op: string;
};

export type ToolCallDelta = {
  id: string;
  name: string;
  args_delta: string;
};

export type AssistantDeltaEvent = Envelope & {
  type: "assistant_delta";
  turn: string;
  text?: string;
  tool_call?: ToolCallDelta;
};

export type AssistantSealedEvent = Envelope & {
  type: "assistant_sealed";
  turn: string;
  finish_reason: FinishReason;
  "gen_ai.request.model"?: string;
  "gen_ai.usage.input_tokens"?: number;
  "gen_ai.usage.output_tokens"?: number;
};

export type WorkspaceSnapshotEvent = Envelope & {
  type: "workspace_snapshot";
  rev: number;
  tree: string;
};

export type ToolPendingEvent = Envelope & {
  type: "tool_pending";
  "gen_ai.tool.call.id": string;
  "gen_ai.tool.name": string;
  args_hash: string;
  args: JsonValue;
  policy: ToolPolicy;
  workspace_rev: number;
};

export type ToolAppliedEvent = Envelope & {
  type: "tool_applied";
  "gen_ai.tool.call.id": string;
  workspace_rev: number;
  result_ref?: string;
  result_text?: string;
};

export type ToolFailedEvent = Envelope & {
  type: "tool_failed";
  "gen_ai.tool.call.id": string;
  error: string;
};

export type ToolAbandonedEvent = Envelope & {
  type: "tool_abandoned";
  "gen_ai.tool.call.id": string;
  reason: string;
};

export type Event =
  | SystemEvent
  | UserEvent
  | AssistantDeltaEvent
  | AssistantSealedEvent
  | WorkspaceSnapshotEvent
  | ToolPendingEvent
  | ToolAppliedEvent
  | ToolFailedEvent
  | ToolAbandonedEvent;

export type SystemMessage = {
  role: "system";
  content: string;
  op: string;
};

export type UserMessage = {
  role: "user";
  content: string;
  op: string;
};

export type AssistantToolCall = {
  id: string;
  name: string;
  arguments: string;
};

export type AssistantMessage = {
  role: "assistant";
  content: string;
  turn: string;
  finish_reason: FinishReason;
  model: string | null;
  tool_calls: AssistantToolCall[];
};

export type ToolMessage = {
  role: "tool";
  call_id: string;
  name: string;
  status: SideEffectStatus;
  result_text: string | null;
  error: string | null;
};

export type Message =
  | SystemMessage
  | UserMessage
  | AssistantMessage
  | ToolMessage;

export type WorkspaceHead = {
  rev: number;
  tree: string;
};

export type LedgerRow = {
  call_id: string;
  tool: string;
  args_hash: string;
  status: SideEffectStatus;
  policy: ToolPolicy;
  result_ref: string | null;
};

export type CheckpointView = {
  session_id: string;
  event_head: number;
  workspace_head: WorkspaceHead | null;
  messages: Message[];
  ledger: LedgerRow[];
};

export function assertNever(value: never): never {
  throw new Error(`unexpected value ${JSON.stringify(value)}`);
}
