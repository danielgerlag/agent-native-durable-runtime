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

        let file = File::open(root.join(transcript_name))?;
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

        let workspace_head = match manifest.get("workspace_head") {
            None | Some(Value::Null) => None,
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
                Some((rev, tree.to_owned()))
            }
        };

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
