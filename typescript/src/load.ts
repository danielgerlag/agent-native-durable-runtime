import fs from "node:fs";
import path from "node:path";
import { foldMessages, foldView } from "./fold";
import {
  FINISH_REASONS,
  TOOL_POLICIES,
  type CheckpointView,
  type Event,
  type FinishReason,
  type JsonValue,
  type Message,
  type ToolCallDelta,
  type ToolPolicy,
  type WorkspaceHead,
} from "./types";

const FORMAT = "durable_session.checkpoint";
const ARGS_HASH = /^[0-9a-f]{64}$/;
const SESSION_ID = /^[A-Za-z0-9_-]+$/;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isJsonValue(value: unknown): value is JsonValue {
  if (
    value === null ||
    typeof value === "boolean" ||
    typeof value === "number" ||
    typeof value === "string"
  ) {
    return true;
  }
  if (Array.isArray(value)) {
    return value.every(isJsonValue);
  }
  if (isRecord(value)) {
    return Object.values(value).every(isJsonValue);
  }
  return false;
}

function reqString(obj: Record<string, unknown>, key: string): string {
  const value = obj[key];
  if (typeof value !== "string") {
    throw new Error(`missing field ${key}`);
  }
  return value;
}

function reqNonEmpty(obj: Record<string, unknown>, key: string): string {
  const value = reqString(obj, key);
  if (value.length === 0) {
    throw new Error(`missing field ${key}`);
  }
  return value;
}

function optString(obj: Record<string, unknown>, key: string): string | undefined {
  if (!(key in obj) || obj[key] === undefined || obj[key] === null) {
    return undefined;
  }
  if (typeof obj[key] !== "string") {
    throw new Error(`missing field ${key}`);
  }
  return obj[key];
}

function reqUint(obj: Record<string, unknown>, key: string): number {
  const value = obj[key];
  if (typeof value !== "number" || !Number.isInteger(value) || value < 0) {
    throw new Error(`missing field ${key}`);
  }
  return value;
}

function optInt(obj: Record<string, unknown>, key: string): number | undefined {
  if (!(key in obj) || obj[key] === undefined || obj[key] === null) {
    return undefined;
  }
  const value = obj[key];
  if (typeof value !== "number" || !Number.isInteger(value)) {
    throw new Error(`missing field ${key}`);
  }
  return value;
}

function isOneOf<T extends string>(
  value: string,
  allowed: readonly T[],
): value is T {
  return allowed.some((item) => item === value);
}

function parseFinishReason(value: string): FinishReason {
  if (!isOneOf(value, FINISH_REASONS)) {
    throw new Error(`unknown finish reason ${value}`);
  }
  return value;
}

function parsePolicy(value: string): ToolPolicy {
  if (!isOneOf(value, TOOL_POLICIES)) {
    throw new Error(`unknown tool policy ${value}`);
  }
  return value;
}

function parseBlobUri(value: string): string {
  const hex = value.startsWith("sha256:") ? value.slice("sha256:".length) : value;
  if (hex.length !== 64 || !/^[0-9a-fA-F]+$/.test(hex)) {
    throw new Error(`invalid blob ref ${value}`);
  }
  return value;
}

function parseToolCallDelta(value: unknown): ToolCallDelta | undefined {
  if (value === undefined || value === null) {
    return undefined;
  }
  if (!isRecord(value)) {
    throw new Error("missing field tool_call");
  }
  return {
    id: reqNonEmpty(value, "id"),
    name: reqString(value, "name"),
    args_delta: optString(value, "args_delta") ?? "",
  };
}

function parseEvent(raw: unknown): Event {
  if (!isRecord(raw)) {
    throw new Error("event is not an object");
  }
  const seq = reqUint(raw, "seq");
  if (seq < 1) {
    throw new Error("event missing seq");
  }
  const t = reqNonEmpty(raw, "t");
  const type = reqString(raw, "type");
  switch (type) {
    case "system": {
      const role = reqString(raw, "role");
      if (role !== "system") {
        throw new Error("missing field role");
      }
      return {
        seq,
        t,
        type: "system",
        role: "system",
        content: reqString(raw, "content"),
        op: reqNonEmpty(raw, "op"),
      };
    }
    case "user": {
      const role = reqString(raw, "role");
      if (role !== "user") {
        throw new Error("missing field role");
      }
      return {
        seq,
        t,
        type: "user",
        role: "user",
        content: reqString(raw, "content"),
        op: reqNonEmpty(raw, "op"),
      };
    }
    case "assistant_delta": {
      const text = optString(raw, "text");
      const toolCall = parseToolCallDelta(raw.tool_call);
      if (text === undefined && toolCall === undefined) {
        throw new Error("assistant_delta requires text and/or tool_call");
      }
      return {
        seq,
        t,
        type: "assistant_delta",
        turn: reqNonEmpty(raw, "turn"),
        ...(text !== undefined ? { text } : {}),
        ...(toolCall !== undefined ? { tool_call: toolCall } : {}),
      };
    }
    case "assistant_sealed": {
      const model = optString(raw, "gen_ai.request.model");
      const inputTokens = optInt(raw, "gen_ai.usage.input_tokens");
      const outputTokens = optInt(raw, "gen_ai.usage.output_tokens");
      return {
        seq,
        t,
        type: "assistant_sealed",
        turn: reqNonEmpty(raw, "turn"),
        finish_reason: parseFinishReason(reqString(raw, "finish_reason")),
        ...(model !== undefined ? { "gen_ai.request.model": model } : {}),
        ...(inputTokens !== undefined
          ? { "gen_ai.usage.input_tokens": inputTokens }
          : {}),
        ...(outputTokens !== undefined
          ? { "gen_ai.usage.output_tokens": outputTokens }
          : {}),
      };
    }
    case "workspace_snapshot": {
      const tree = parseBlobUri(reqString(raw, "tree"));
      const rev = reqUint(raw, "rev");
      if (rev < 1) {
        throw new Error("snapshot missing rev");
      }
      return {
        seq,
        t,
        type: "workspace_snapshot",
        rev,
        tree,
      };
    }
    case "tool_pending": {
      const argsRaw = raw.args === undefined ? null : raw.args;
      if (!isJsonValue(argsRaw)) {
        throw new Error("missing field args");
      }
      const argsHash = reqString(raw, "args_hash");
      if (!ARGS_HASH.test(argsHash)) {
        throw new Error("missing field args_hash");
      }
      return {
        seq,
        t,
        type: "tool_pending",
        "gen_ai.tool.call.id": reqNonEmpty(raw, "gen_ai.tool.call.id"),
        "gen_ai.tool.name": reqNonEmpty(raw, "gen_ai.tool.name"),
        args_hash: argsHash,
        args: argsRaw,
        policy: parsePolicy(reqString(raw, "policy")),
        workspace_rev: "workspace_rev" in raw ? reqUint(raw, "workspace_rev") : 0,
      };
    }
    case "tool_applied": {
      const resultRef = optString(raw, "result_ref");
      if (resultRef !== undefined) {
        parseBlobUri(resultRef);
      }
      const resultText = optString(raw, "result_text");
      return {
        seq,
        t,
        type: "tool_applied",
        "gen_ai.tool.call.id": reqNonEmpty(raw, "gen_ai.tool.call.id"),
        workspace_rev: "workspace_rev" in raw ? reqUint(raw, "workspace_rev") : 0,
        ...(resultRef !== undefined ? { result_ref: resultRef } : {}),
        ...(resultText !== undefined ? { result_text: resultText } : {}),
      };
    }
    case "tool_failed":
      return {
        seq,
        t,
        type: "tool_failed",
        "gen_ai.tool.call.id": reqNonEmpty(raw, "gen_ai.tool.call.id"),
        error: reqString(raw, "error"),
      };
    case "tool_abandoned":
      return {
        seq,
        t,
        type: "tool_abandoned",
        "gen_ai.tool.call.id": reqNonEmpty(raw, "gen_ai.tool.call.id"),
        reason: reqString(raw, "reason"),
      };
    default:
      throw new Error(`unknown event type ${type}`);
  }
}

function parseWorkspaceHead(value: unknown): WorkspaceHead | null {
  if (value === undefined || value === null) {
    return null;
  }
  if (!isRecord(value)) {
    throw new Error("workspace_head missing rev");
  }
  const rev = reqUint(value, "rev");
  const tree = reqString(value, "tree");
  parseBlobUri(tree);
  return { rev, tree };
}

function resolveInBundle(dir: string, rel: string): string {
  if (path.isAbsolute(rel) || rel.split(/[\\/]/).includes("..")) {
    throw new Error("path escapes bundle");
  }
  return path.join(dir, rel);
}

function foldedWorkspaceHead(events: Event[]): WorkspaceHead | null {
  for (let i = events.length - 1; i >= 0; i -= 1) {
    const event = events[i];
    if (event !== undefined && event.type === "workspace_snapshot") {
      return { rev: event.rev, tree: event.tree };
    }
  }
  return null;
}

function ensureReferencedBlob(dir: string, event: Event): void {
  let uri: string | undefined;
  if (event.type === "workspace_snapshot") {
    uri = event.tree;
  } else if (event.type === "tool_applied") {
    uri = event.result_ref;
  }
  if (uri === undefined) {
    return;
  }
  const hex = uri.startsWith("sha256:") ? uri.slice("sha256:".length) : uri;
  if (hex.length < 2) {
    throw new Error(`invalid blob ref ${uri}`);
  }
  const blobPath = resolveInBundle(dir, path.posix.join("blobs", hex.slice(0, 2), hex));
  if (!fs.existsSync(blobPath) || !fs.statSync(blobPath).isFile()) {
    throw new Error(`missing blob ${uri}`);
  }
}

function parseTranscript(filePath: string): Event[] {
  const body = fs.readFileSync(filePath, "utf8");
  const events: Event[] = [];
  let expect = 1;
  for (const line of body.split(/\r?\n/)) {
    if (line.trim() === "") {
      continue;
    }
    let raw: unknown;
    try {
      raw = JSON.parse(line);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      throw new Error(msg);
    }
    if (!isRecord(raw)) {
      throw new Error("event is not an object");
    }
    const seq = reqUint(raw, "seq");
    if (seq !== expect) {
      throw new Error(`seq gap: expected ${expect}, got ${seq}`);
    }
    expect += 1;
    if (!("t" in raw) || typeof raw.t !== "string" || !raw.t.toUpperCase().includes("T")) {
      throw new Error("event missing t");
    }
    events.push(parseEvent(raw));
  }
  return events;
}

export class Checkpoint {
  readonly sessionId: string;
  readonly events: Event[];
  readonly messages: Message[];
  private readonly eventHead: number;
  private readonly workspaceHead: WorkspaceHead | null;

  private constructor(
    sessionId: string,
    eventHead: number,
    workspaceHead: WorkspaceHead | null,
    events: Event[],
    messages: Message[],
  ) {
    this.sessionId = sessionId;
    this.eventHead = eventHead;
    this.workspaceHead = workspaceHead;
    this.events = events;
    this.messages = messages;
  }

  static load(dir: string): Checkpoint {
    const manifestRaw: unknown = JSON.parse(
      fs.readFileSync(path.join(dir, "manifest.json"), "utf8"),
    );
    if (!isRecord(manifestRaw)) {
      throw new Error("manifest is not an object");
    }
    const format =
      typeof manifestRaw.format === "string" ? manifestRaw.format : "";
    if (format !== FORMAT) {
      throw new Error(`unknown format ${format}`);
    }
    const version =
      typeof manifestRaw.checkpoint_version === "number" &&
      Number.isInteger(manifestRaw.checkpoint_version)
        ? manifestRaw.checkpoint_version
        : Number.MAX_SAFE_INTEGER;
    if (version !== 0) {
      throw new Error(`unsupported checkpoint_version ${version}`);
    }
    const sessionId = reqNonEmpty(manifestRaw, "session_id");
    if (!SESSION_ID.test(sessionId)) {
      throw new Error("invalid session id");
    }
    const files = manifestRaw.files;
    const transcriptName =
      isRecord(files) && typeof files.transcript === "string"
        ? files.transcript
        : "transcript.ndjson";
    const events = parseTranscript(resolveInBundle(dir, transcriptName));
    const last = events.at(-1);
    const lastSeq = last === undefined ? 0 : last.seq;
    const eventHead =
      typeof manifestRaw.event_head === "number" &&
      Number.isInteger(manifestRaw.event_head)
        ? manifestRaw.event_head
        : lastSeq;
    if (eventHead !== lastSeq) {
      throw new Error(`event_head ${eventHead} does not match transcript`);
    }
    const manifestHead = parseWorkspaceHead(manifestRaw.workspace_head);
    const foldedHead = foldedWorkspaceHead(events);
    let workspaceHead: WorkspaceHead | null;
    if (manifestHead === null) {
      workspaceHead = foldedHead;
    } else if (
      foldedHead !== null &&
      foldedHead.rev === manifestHead.rev &&
      foldedHead.tree === manifestHead.tree
    ) {
      workspaceHead = foldedHead;
    } else {
      throw new Error("workspace_head does not match transcript");
    }
    for (const event of events) {
      ensureReferencedBlob(dir, event);
    }
    const messages = foldMessages(events);
    return new Checkpoint(
      sessionId,
      eventHead,
      workspaceHead,
      events,
      messages,
    );
  }

  view(): CheckpointView {
    return foldView(
      this.sessionId,
      this.eventHead,
      this.workspaceHead,
      this.messages,
      this.events,
    );
  }
}

export function load(dir: string): Checkpoint {
  return Checkpoint.load(dir);
}
