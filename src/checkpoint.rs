use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use serde_json::{json, Value};

use crate::bundle::{parse_rfc3339, wire_to_event};
use crate::error::Error;
use crate::event::{Event, LoggedEvent, Message};
use crate::ids::{BlobRef, EventSeq, SessionId};
use crate::reducer::SessionState;
use crate::session::fold_log;
use crate::tool::SideEffectStatus;

/// A checkpoint bundle on disk. No SQLite. No lease.
pub struct Checkpoint {
    session_id: SessionId,
    event_head: u64,
    workspace_head: Option<(u64, String)>,
    events: Vec<LoggedEvent>,
    state: SessionState,
}

impl Checkpoint {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, Error> {
        let root = path.as_ref();
        let manifest: Value = serde_json::from_slice(&std::fs::read(root.join("manifest.json"))?)
            .map_err(|e| Error::bundle(e.to_string()))?;
        let format = manifest
            .get("format")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if format != "durable_session.checkpoint" {
            return Err(Error::bundle(format!("unknown format {format}")));
        }
        let version = manifest
            .get("checkpoint_version")
            .and_then(|v| v.as_u64())
            .unwrap_or(u64::MAX);
        if version != 0 {
            return Err(Error::bundle(format!(
                "unsupported checkpoint_version {version}"
            )));
        }
        let sid = manifest
            .get("session_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::bundle("manifest missing session_id"))?;
        let session_id = SessionId::parse(sid)?;
        let transcript_name = manifest
            .get("files")
            .and_then(|f| f.get("transcript"))
            .and_then(|t| t.as_str())
            .unwrap_or("transcript.ndjson");
        let transcript_path = resolve_in_bundle(root, transcript_name)?;

        let file = File::open(transcript_path)?;
        let reader = BufReader::new(file);
        let mut expect = 1u64;
        let mut events = Vec::new();
        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let v: Value = serde_json::from_str(&line).map_err(|e| Error::bundle(e.to_string()))?;
            let seq = v
                .get("seq")
                .and_then(|s| s.as_u64())
                .ok_or_else(|| Error::bundle("event missing seq"))?;
            if seq != expect {
                return Err(Error::bundle(format!(
                    "seq gap: expected {expect}, got {seq}"
                )));
            }
            expect += 1;
            let t = v
                .get("t")
                .and_then(|s| s.as_str())
                .ok_or_else(|| Error::bundle("event missing t"))?;
            let t_ms = parse_rfc3339(t)?;
            let event = wire_to_event(&v)?;
            events.push(LoggedEvent {
                seq: EventSeq::new(seq),
                t_ms,
                event,
            });
        }

        let state = fold_log(&events)?;
        let event_head = manifest
            .get("event_head")
            .and_then(|v| v.as_u64())
            .unwrap_or(events.last().map(|e| e.seq.get()).unwrap_or(0));
        if event_head != events.last().map(|e| e.seq.get()).unwrap_or(0) {
            return Err(Error::bundle(format!(
                "event_head {event_head} does not match transcript"
            )));
        }

        let manifest_head = parse_workspace_head(manifest.get("workspace_head"))?;
        let folded_head = folded_workspace_head(&events);
        let workspace_head = match (manifest_head, folded_head) {
            (None, folded) => folded,
            (Some(m), Some(f)) if m == f => Some(f),
            (Some(_), Some(_)) => {
                return Err(Error::bundle("workspace_head does not match transcript"));
            }
            (Some(_), None) => {
                return Err(Error::bundle("workspace_head does not match transcript"));
            }
        };

        for logged in &events {
            ensure_referenced_blob(root, &logged.event)?;
        }

        Ok(Self {
            session_id,
            event_head,
            workspace_head,
            events,
            state,
        })
    }

    pub fn session_id(&self) -> &SessionId {
        &self.session_id
    }

    pub fn events(&self) -> &[LoggedEvent] {
        &self.events
    }

    pub fn messages(&self) -> &[Message] {
        &self.state.messages
    }

    /// Canonical projection for cross-language conformance. Not part of the bundle.
    pub fn view(&self) -> Value {
        let messages: Vec<Value> = self.state.messages.iter().map(message_view).collect();
        json!({
            "session_id": self.session_id.as_str(),
            "event_head": self.event_head,
            "workspace_head": match &self.workspace_head {
                None => Value::Null,
                Some((rev, tree)) => json!({"rev": rev, "tree": tree}),
            },
            "messages": messages,
            "ledger": ledger_view(&self.events),
        })
    }
}

fn resolve_in_bundle(root: &Path, rel: &str) -> Result<std::path::PathBuf, Error> {
    let rel_path = Path::new(rel);
    if rel_path.is_absolute() {
        return Err(Error::bundle("path escapes bundle"));
    }
    for c in rel_path.components() {
        match c {
            std::path::Component::Normal(_) | std::path::Component::CurDir => {}
            _ => return Err(Error::bundle("path escapes bundle")),
        }
    }
    Ok(root.join(rel_path))
}

fn parse_workspace_head(v: Option<&Value>) -> Result<Option<(u64, String)>, Error> {
    match v {
        None | Some(Value::Null) => Ok(None),
        Some(obj) => {
            let rev = obj
                .get("rev")
                .and_then(|r| r.as_u64())
                .ok_or_else(|| Error::bundle("workspace_head missing rev"))?;
            let tree = obj
                .get("tree")
                .and_then(|t| t.as_str())
                .ok_or_else(|| Error::bundle("workspace_head missing tree"))?;
            let _ = BlobRef::parse(tree)?;
            Ok(Some((rev, tree.to_owned())))
        }
    }
}

fn folded_workspace_head(events: &[LoggedEvent]) -> Option<(u64, String)> {
    events.iter().rev().find_map(|e| match &e.event {
        Event::WorkspaceSnapshotted { rev, tree } => Some((rev.get(), tree.uri())),
        _ => None,
    })
}

fn ensure_referenced_blob(root: &Path, event: &Event) -> Result<(), Error> {
    let uri = match event {
        Event::WorkspaceSnapshotted { tree, .. } => tree.uri(),
        Event::ToolApplied {
            result_ref: Some(r),
            ..
        } => r.uri(),
        _ => return Ok(()),
    };
    let hex = uri
        .strip_prefix("sha256:")
        .ok_or_else(|| Error::bundle(format!("invalid blob ref {uri}")))?;
    if hex.len() < 2 {
        return Err(Error::bundle(format!("invalid blob ref {uri}")));
    }
    let rel = format!("blobs/{}/{hex}", &hex[..2]);
    let path = resolve_in_bundle(root, &rel)?;
    if !path.is_file() {
        return Err(Error::corrupt(format!("missing blob {uri}")));
    }
    Ok(())
}

fn message_view(m: &Message) -> Value {
    match m {
        Message::System { content, op } => json!({
            "role": "system",
            "content": content,
            "op": op.as_str(),
        }),
        Message::User { content, op } => json!({
            "role": "user",
            "content": content,
            "op": op.as_str(),
        }),
        Message::Assistant {
            turn,
            content,
            tool_calls,
            finish_reason,
            model,
        } => {
            let calls: Vec<Value> = tool_calls
                .iter()
                .map(|c| {
                    json!({
                        "id": c.id.as_str(),
                        "name": c.name,
                        "arguments": c.arguments,
                    })
                })
                .collect();
            json!({
                "role": "assistant",
                "content": content,
                "turn": turn.as_str(),
                "finish_reason": finish_reason.as_str(),
                "model": model,
                "tool_calls": calls,
            })
        }
        Message::Tool {
            call_id,
            name,
            status,
            result_text,
            error,
        } => json!({
            "role": "tool",
            "call_id": call_id.as_str(),
            "name": name,
            "status": status.as_str(),
            "result_text": result_text,
            "error": error,
        }),
    }
}

fn ledger_view(events: &[LoggedEvent]) -> Vec<Value> {
    let mut rows: Vec<Value> = Vec::new();
    for logged in events {
        match &logged.event {
            Event::ToolPending {
                call_id,
                name,
                args_hash,
                policy,
                ..
            } => {
                rows.push(json!({
                    "call_id": call_id.as_str(),
                    "tool": name,
                    "args_hash": args_hash.as_str(),
                    "status": SideEffectStatus::Pending.as_str(),
                    "policy": policy.as_str(),
                    "result_ref": Value::Null,
                }));
            }
            Event::ToolApplied {
                call_id,
                result_ref,
                ..
            } => {
                if let Some(row) = find_row(&mut rows, call_id.as_str()) {
                    row["status"] = json!("applied");
                    row["result_ref"] = match result_ref {
                        Some(r) => json!(r.uri()),
                        None => Value::Null,
                    };
                }
            }
            Event::ToolFailed { call_id, .. } => {
                if let Some(row) = find_row(&mut rows, call_id.as_str()) {
                    row["status"] = json!("failed");
                }
            }
            Event::ToolAbandoned { call_id, .. } => {
                if let Some(row) = find_row(&mut rows, call_id.as_str()) {
                    row["status"] = json!("abandoned");
                }
            }
            _ => {}
        }
    }
    rows
}

fn find_row<'a>(rows: &'a mut [Value], call_id: &str) -> Option<&'a mut Value> {
    rows.iter_mut()
        .find(|r| r.get("call_id").and_then(|c| c.as_str()) == Some(call_id))
}
