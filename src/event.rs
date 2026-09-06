use serde::{Deserialize, Serialize};

use crate::ids::{ArgsHash, BlobRef, CallId, EventSeq, OpId, SnapshotRev, TurnId};
use crate::tool::{FinishReason, SideEffectStatus, ToolPolicy};

#[derive(Clone, Debug, PartialEq)]
pub enum Input {
    User { content: String },
    System { content: String },
}

impl Input {
    pub fn user(content: impl Into<String>) -> Self {
        Input::User {
            content: content.into(),
        }
    }

    pub fn system(content: impl Into<String>) -> Self {
        Input::System {
            content: content.into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolCallDelta {
    pub id: CallId,
    pub name: String,
    pub args_delta: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssistantDelta {
    pub turn: TurnId,
    pub text: Option<String>,
    pub tool_call: Option<ToolCallDelta>,
}

impl AssistantDelta {
    pub fn text(turn: TurnId, text: impl Into<String>) -> Self {
        Self {
            turn,
            text: Some(text.into()),
            tool_call: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ToolCall {
    pub id: CallId,
    pub name: String,
    pub arguments: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Message {
    System {
        content: String,
        op: OpId,
    },
    User {
        content: String,
        op: OpId,
    },
    Assistant {
        turn: TurnId,
        content: String,
        tool_calls: Vec<ToolCall>,
        finish_reason: FinishReason,
        model: Option<String>,
    },
    Tool {
        call_id: CallId,
        name: String,
        status: SideEffectStatus,
        result_text: Option<String>,
        error: Option<String>,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Event {
    SessionOpened,
    SessionClosed,
    User {
        content: String,
        op: OpId,
    },
    System {
        content: String,
        op: OpId,
    },
    AssistantDelta {
        turn: TurnId,
        text: Option<String>,
        tool_call: Option<ToolCallDelta>,
    },
    AssistantSealed {
        turn: TurnId,
        finish_reason: FinishReason,
        model: Option<String>,
        input_tokens: Option<i64>,
        output_tokens: Option<i64>,
    },
    WorkspaceSnapshotted {
        rev: SnapshotRev,
        tree: BlobRef,
    },
    ToolPending {
        call_id: CallId,
        name: String,
        args_hash: ArgsHash,
        args: serde_json::Value,
        policy: ToolPolicy,
        workspace_rev: SnapshotRev,
    },
    ToolApplied {
        call_id: CallId,
        result_ref: Option<BlobRef>,
        result_text: Option<String>,
        workspace_rev: SnapshotRev,
    },
    ToolFailed {
        call_id: CallId,
        error: String,
    },
    ToolAbandoned {
        call_id: CallId,
        reason: String,
    },
}

impl Event {
    pub fn kind(&self) -> &'static str {
        match self {
            Event::SessionOpened => "session_opened",
            Event::SessionClosed => "session_closed",
            Event::User { .. } => "user",
            Event::System { .. } => "system",
            Event::AssistantDelta { .. } => "assistant_delta",
            Event::AssistantSealed { .. } => "assistant_sealed",
            Event::WorkspaceSnapshotted { .. } => "workspace_snapshot",
            Event::ToolPending { .. } => "tool_pending",
            Event::ToolApplied { .. } => "tool_applied",
            Event::ToolFailed { .. } => "tool_failed",
            Event::ToolAbandoned { .. } => "tool_abandoned",
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct LoggedEvent {
    pub seq: EventSeq,
    pub t_ms: i64,
    pub event: Event,
}
