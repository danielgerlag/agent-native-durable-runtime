use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::ids::{sha256_hex, ArgsHash, BlobRef, CallId, SnapshotRev};
use crate::error::Error;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolPolicy {
    Idempotent,
    AtMostOnce,
}

impl ToolPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            ToolPolicy::Idempotent => "idempotent",
            ToolPolicy::AtMostOnce => "at_most_once",
        }
    }

    pub(crate) fn parse(s: &str) -> Result<Self, Error> {
        match s {
            "idempotent" => Ok(ToolPolicy::Idempotent),
            "at_most_once" => Ok(ToolPolicy::AtMostOnce),
            other => Err(Error::corrupt(format!("unknown tool policy {other}"))),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SideEffectStatus {
    Pending,
    Applied,
    Failed,
    Abandoned,
}

impl SideEffectStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            SideEffectStatus::Pending => "pending",
            SideEffectStatus::Applied => "applied",
            SideEffectStatus::Failed => "failed",
            SideEffectStatus::Abandoned => "abandoned",
        }
    }

    pub(crate) fn parse(s: &str) -> Result<Self, Error> {
        match s {
            "pending" => Ok(SideEffectStatus::Pending),
            "applied" => Ok(SideEffectStatus::Applied),
            "failed" => Ok(SideEffectStatus::Failed),
            "abandoned" => Ok(SideEffectStatus::Abandoned),
            other => Err(Error::corrupt(format!("unknown side-effect status {other}"))),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FinishReason {
    Stop,
    Length,
    ToolCalls,
    TruncatedCrash,
    Error,
}

impl FinishReason {
    pub fn as_str(self) -> &'static str {
        match self {
            FinishReason::Stop => "stop",
            FinishReason::Length => "length",
            FinishReason::ToolCalls => "tool_calls",
            FinishReason::TruncatedCrash => "truncated_crash",
            FinishReason::Error => "error",
        }
    }

    pub(crate) fn parse(s: &str) -> Result<Self, Error> {
        match s {
            "stop" => Ok(FinishReason::Stop),
            "length" => Ok(FinishReason::Length),
            "tool_calls" => Ok(FinishReason::ToolCalls),
            "truncated_crash" => Ok(FinishReason::TruncatedCrash),
            "error" => Ok(FinishReason::Error),
            other => Err(Error::corrupt(format!("unknown finish reason {other}"))),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ToolSpec {
    pub name: String,
    pub policy: ToolPolicy,
}

impl ToolSpec {
    pub fn new(name: impl Into<String>, policy: ToolPolicy) -> Self {
        Self {
            name: name.into(),
            policy,
        }
    }

    pub fn idempotent(name: impl Into<String>) -> Self {
        Self::new(name, ToolPolicy::Idempotent)
    }

    pub fn at_most_once(name: impl Into<String>) -> Self {
        Self::new(name, ToolPolicy::AtMostOnce)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ToolResult {
    pub text: Option<String>,
    pub bytes: Option<Vec<u8>>,
}

impl ToolResult {
    pub fn text(s: impl Into<String>) -> Self {
        Self {
            text: Some(s.into()),
            bytes: None,
        }
    }

    pub fn bytes(bytes: Vec<u8>) -> Self {
        Self {
            text: None,
            bytes: Some(bytes),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct AppliedTool {
    pub call_id: CallId,
    pub name: String,
    pub args_hash: ArgsHash,
    pub result_ref: Option<BlobRef>,
    pub result_text: Option<String>,
    pub workspace_rev: SnapshotRev,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PendingTool {
    pub call_id: CallId,
    pub name: String,
    pub args_hash: ArgsHash,
    pub args: Value,
    pub policy: ToolPolicy,
    pub workspace_rev: SnapshotRev,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ToolDisposition {
    Run,
    AlreadyApplied(AppliedTool),
    Inspect(PendingTool),
}

#[derive(Clone, Debug, PartialEq)]
pub enum ToolRun {
    AlreadyApplied(AppliedTool),
    Inspect(PendingTool),
    Completed(AppliedTool),
}

#[derive(Clone, Debug, PartialEq)]
pub enum Recovery {
    Clean,
    Truncated {
        prefix: String,
        model: Option<String>,
    },
    PendingTool(PendingTool),
}

pub struct ToolCtx<'a> {
    workspace: &'a Path,
    call_id: &'a CallId,
}

impl<'a> ToolCtx<'a> {
    pub(crate) fn new(workspace: &'a Path, call_id: &'a CallId) -> Self {
        Self { workspace, call_id }
    }

    pub fn workspace(&self) -> &Path {
        self.workspace
    }

    pub fn call_id(&self) -> &CallId {
        self.call_id
    }

    pub fn path(&self, rel: impl AsRef<Path>) -> PathBuf {
        self.workspace.join(rel)
    }
}

pub(crate) fn canonicalize_json(v: &Value) -> Value {
    match v {
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort_unstable();
            let mut out = serde_json::Map::with_capacity(map.len());
            for k in keys {
                out.insert(k.clone(), canonicalize_json(&map[k]));
            }
            Value::Object(out)
        }
        Value::Array(items) => Value::Array(items.iter().map(canonicalize_json).collect()),
        other => other.clone(),
    }
}

pub(crate) fn args_hash(args: &Value) -> ArgsHash {
    let canonical = canonicalize_json(args);
    let bytes = serde_json::to_vec(&canonical).unwrap_or_else(|_| b"null".to_vec());
    ArgsHash::from_hex(sha256_hex(&bytes))
}
