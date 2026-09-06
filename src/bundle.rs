use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::Path;

use serde_json::{json, Map, Value};

use crate::error::Error;
use crate::event::{Event, LoggedEvent, ToolCallDelta};
use crate::ids::{BlobRef, CallId, EventSeq, OpId, SessionId, SnapshotRev, TurnId};
use crate::reducer::apply;
use crate::semconv;
use crate::session::fold_log;
use crate::store::Store;
use crate::tool::{FinishReason, SideEffectStatus, ToolPolicy};
use crate::workspace::{self, Filter, Tree};

pub(crate) fn export(
    dest: &Path,
    session_id: &SessionId,
    created_at_ms: i64,
    exported_at_ms: i64,
    log: &[LoggedEvent],
    workspace_head: Option<(SnapshotRev, BlobRef)>,
    blob_dir: &Path,
    workspace: &Path,
    filter: &Filter,
    store_dir: &Path,
) -> Result<(), Error> {
    if dest.exists() {
        fs::remove_dir_all(dest)?;
    }
    fs::create_dir_all(dest)?;
    fs::create_dir_all(dest.join("trees"))?;
    fs::create_dir_all(dest.join("blobs"))?;
    fs::create_dir_all(dest.join("views"))?;
    fs::create_dir_all(dest.join("workspace"))?;

    let mut exported: Vec<(u64, i64, Value)> = Vec::new();
    let mut seq = 0u64;
    let mut last_head: Option<(SnapshotRev, BlobRef)> = None;
    let mut blobs_needed: Vec<BlobRef> = Vec::new();
    let mut ledger = Vec::new();

    for logged in log {
        let Some(wire) = event_to_wire(&logged.event)? else {
            continue;
        };
        seq += 1;
        let mut obj = wire;
        obj.insert("seq".into(), json!(seq));
        obj.insert("t".into(), json!(rfc3339_millis(logged.t_ms)));
        if let Event::WorkspaceSnapshotted { rev, tree } = &logged.event {
            last_head = Some((*rev, tree.clone()));
            blobs_needed.push(tree.clone());
            let bytes = read_blob(blob_dir, tree)?;
            let hex = tree.as_hex();
            fs::write(dest.join("trees").join(format!("{hex}.json")), &bytes)?;
            collect_tree_blobs(&bytes, &mut blobs_needed)?;
        }
        collect_event_blobs(&logged.event, &mut blobs_needed);
        if let Some(row) = ledger_row(&logged.event) {
            ledger.push(row);
        }
        exported.push((seq, logged.t_ms, Value::Object(obj)));
    }

    let mut transcript = String::new();
    for (_, _, v) in &exported {
        transcript.push_str(&serde_json::to_string(v).map_err(|e| Error::bundle(e.to_string()))?);
        transcript.push('\n');
    }
    fs::write(dest.join("transcript.ndjson"), transcript)?;

    for blob in blobs_needed {
        copy_blob(blob_dir, dest.join("blobs").as_path(), &blob)?;
    }

    let head = last_head.or(workspace_head);
    if let Some((rev, tree)) = &head {
        let _ = rev;
        if let Ok(bytes) = read_blob(blob_dir, tree) {
            if let Ok(parsed) = workspace::parse_tree(&bytes) {
                let _ = workspace::materialize(&dest.join("workspace"), &parsed, |b| {
                    read_blob(blob_dir, b)
                });
            }
        }
    } else if workspace.exists() {
        let mut put = |bytes: &[u8]| -> Result<BlobRef, Error> { Ok(BlobRef::of_bytes(bytes)) };
        let _ = workspace::capture(workspace, filter, store_dir, &mut put);
    }

    fs::write(
        dest.join("views/ledger.json"),
        serde_json::to_vec_pretty(&ledger).map_err(|e| Error::bundle(e.to_string()))?,
    )?;

    let workspace_head_json = match &head {
        Some((rev, tree)) => json!({"rev": rev.get(), "tree": tree.uri()}),
        None => Value::Null,
    };
    let manifest = json!({
        "format": "durable_session.checkpoint",
        "checkpoint_version": 0,
        "session_id": session_id.as_str(),
        semconv::GEN_AI_CONVERSATION_ID: session_id.as_str(),
        "created_at": rfc3339_millis(created_at_ms),
        "exported_at": rfc3339_millis(exported_at_ms),
        "event_head": seq,
        "workspace_head": workspace_head_json,
        "files": {
            "transcript": "transcript.ndjson",
            "trees": "trees/",
            "blobs": "blobs/",
            "workspace": "workspace/",
            "ledger_view": "views/ledger.json"
        },
        "derived": ["workspace/", "views/ledger.json"]
    });
    fs::write(
        dest.join("manifest.json"),
        serde_json::to_vec_pretty(&manifest).map_err(|e| Error::bundle(e.to_string()))?,
    )?;
    Ok(())
}

pub(crate) fn import(store_dir: &Path, src: &Path) -> Result<SessionId, Error> {
    let manifest_bytes = fs::read(src.join("manifest.json"))?;
    let manifest: Value =
        serde_json::from_slice(&manifest_bytes).map_err(|e| Error::bundle(e.to_string()))?;
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
    let created_at = manifest
        .get("created_at")
        .and_then(|v| v.as_str())
        .map(parse_rfc3339)
        .transpose()?
        .unwrap_or(0);

    let mut store = Store::open_import(store_dir)?;
    store.import_session_row(&session_id, created_at, "")?;
    store.set_session_id(&session_id);

    copy_all_blobs(&src.join("blobs"), store.blob_dir())?;

    let file = File::open(src.join("transcript.ndjson"))?;
    let reader = BufReader::new(file);
    let mut expect = 1u64;
    let mut logged_all = Vec::new();
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
        ensure_event_blobs(&store, &event)?;
        let logged = LoggedEvent {
            seq: EventSeq::new(seq),
            t_ms,
            event,
        };
        store.import_event(&session_id, &logged)?;
        logged_all.push(logged);
    }

    let mut state = crate::reducer::SessionState::origin();
    for e in &logged_all {
        state = apply(&state, &e.event).map_err(Error::from)?;
        state.last_seq = e.seq.get();
    }
    store.rebuild_projections(&logged_all, &state)?;
    let _ = fold_log;
    Ok(session_id)
}

fn event_to_wire(event: &Event) -> Result<Option<Map<String, Value>>, Error> {
    let mut m = Map::new();
    match event {
        Event::SessionOpened | Event::SessionClosed => return Ok(None),
        Event::User { content, op } => {
            m.insert("type".into(), json!("user"));
            m.insert("role".into(), json!("user"));
            m.insert("content".into(), json!(content));
            m.insert("op".into(), json!(op.as_str()));
        }
        Event::System { content, op } => {
            m.insert("type".into(), json!("system"));
            m.insert("role".into(), json!("system"));
            m.insert("content".into(), json!(content));
            m.insert("op".into(), json!(op.as_str()));
        }
        Event::AssistantDelta {
            turn,
            text,
            tool_call,
        } => {
            m.insert("type".into(), json!("assistant_delta"));
            m.insert("turn".into(), json!(turn.as_str()));
            if let Some(t) = text {
                m.insert("text".into(), json!(t));
            }
            if let Some(tc) = tool_call {
                m.insert(
                    "tool_call".into(),
                    json!({
                        "id": tc.id.as_str(),
                        "name": tc.name,
                        "args_delta": tc.args_delta
                    }),
                );
            }
        }
        Event::AssistantSealed {
            turn,
            finish_reason,
            model,
            input_tokens,
            output_tokens,
        } => {
            m.insert("type".into(), json!("assistant_sealed"));
            m.insert("turn".into(), json!(turn.as_str()));
            m.insert("finish_reason".into(), json!(finish_reason.as_str()));
            if let Some(model) = model {
                m.insert(semconv::GEN_AI_REQUEST_MODEL.into(), json!(model));
            }
            if let Some(n) = input_tokens {
                m.insert(semconv::GEN_AI_USAGE_INPUT_TOKENS.into(), json!(n));
            }
            if let Some(n) = output_tokens {
                m.insert(semconv::GEN_AI_USAGE_OUTPUT_TOKENS.into(), json!(n));
            }
        }
        Event::WorkspaceSnapshotted { rev, tree } => {
            m.insert("type".into(), json!("workspace_snapshot"));
            m.insert("rev".into(), json!(rev.get()));
            m.insert("tree".into(), json!(tree.uri()));
        }
        Event::ToolPending {
            call_id,
            name,
            args_hash,
            args,
            policy,
            workspace_rev,
        } => {
            m.insert("type".into(), json!("tool_pending"));
            m.insert(semconv::GEN_AI_TOOL_CALL_ID.into(), json!(call_id.as_str()));
            m.insert(semconv::GEN_AI_TOOL_NAME.into(), json!(name));
            m.insert("args_hash".into(), json!(args_hash.as_str()));
            m.insert("args".into(), args.clone());
            m.insert("policy".into(), json!(policy.as_str()));
            m.insert("workspace_rev".into(), json!(workspace_rev.get()));
        }
        Event::ToolApplied {
            call_id,
            result_ref,
            result_text,
            workspace_rev,
        } => {
            m.insert("type".into(), json!("tool_applied"));
            m.insert(semconv::GEN_AI_TOOL_CALL_ID.into(), json!(call_id.as_str()));
            if let Some(r) = result_ref {
                m.insert("result_ref".into(), json!(r.uri()));
            }
            if let Some(t) = result_text {
                m.insert("result_text".into(), json!(t));
            }
            m.insert("workspace_rev".into(), json!(workspace_rev.get()));
        }
        Event::ToolFailed { call_id, error } => {
            m.insert("type".into(), json!("tool_failed"));
            m.insert(semconv::GEN_AI_TOOL_CALL_ID.into(), json!(call_id.as_str()));
            m.insert("error".into(), json!(error));
        }
        Event::ToolAbandoned { call_id, reason } => {
            m.insert("type".into(), json!("tool_abandoned"));
            m.insert(semconv::GEN_AI_TOOL_CALL_ID.into(), json!(call_id.as_str()));
            m.insert("reason".into(), json!(reason));
        }
    }
    Ok(Some(m))
}

pub(crate) fn wire_to_event(v: &Value) -> Result<Event, Error> {
    let ty = v
        .get("type")
        .and_then(|t| t.as_str())
        .ok_or_else(|| Error::bundle("event missing type"))?;
    match ty {
        "user" => Ok(Event::User {
            content: req_str(v, "content")?.to_owned(),
            op: OpId::parse(req_str(v, "op")?)?,
        }),
        "system" => Ok(Event::System {
            content: req_str(v, "content")?.to_owned(),
            op: OpId::parse(req_str(v, "op")?)?,
        }),
        "assistant_delta" => {
            let turn = TurnId::parse(req_str(v, "turn")?)?;
            let text = v.get("text").and_then(|t| t.as_str()).map(|s| s.to_owned());
            let tool_call = match v.get("tool_call") {
                None | Some(Value::Null) => None,
                Some(tc) => Some(ToolCallDelta {
                    id: CallId::parse(req_str(tc, "id")?)?,
                    name: req_str(tc, "name")?.to_owned(),
                    args_delta: tc
                        .get("args_delta")
                        .and_then(|s| s.as_str())
                        .unwrap_or("")
                        .to_owned(),
                }),
            };
            Ok(Event::AssistantDelta {
                turn,
                text,
                tool_call,
            })
        }
        "assistant_sealed" => Ok(Event::AssistantSealed {
            turn: TurnId::parse(req_str(v, "turn")?)?,
            finish_reason: FinishReason::parse(req_str(v, "finish_reason")?)?,
            model: v
                .get(semconv::GEN_AI_REQUEST_MODEL)
                .and_then(|m| m.as_str())
                .map(|s| s.to_owned()),
            input_tokens: v
                .get(semconv::GEN_AI_USAGE_INPUT_TOKENS)
                .and_then(|n| n.as_i64()),
            output_tokens: v
                .get(semconv::GEN_AI_USAGE_OUTPUT_TOKENS)
                .and_then(|n| n.as_i64()),
        }),
        "workspace_snapshot" => Ok(Event::WorkspaceSnapshotted {
            rev: SnapshotRev::new(
                v.get("rev")
                    .and_then(|r| r.as_u64())
                    .ok_or_else(|| Error::bundle("snapshot missing rev"))?,
            ),
            tree: BlobRef::parse(req_str(v, "tree")?)?,
        }),
        "tool_pending" => {
            let args = v.get("args").cloned().unwrap_or(Value::Null);
            let hash = req_str(v, "args_hash")?;
            if hash.len() != 64 || !hash.chars().all(|c| c.is_ascii_hexdigit()) {
                return Err(Error::bundle("missing field args_hash"));
            }
            Ok(Event::ToolPending {
                call_id: CallId::parse(req_str(v, semconv::GEN_AI_TOOL_CALL_ID)?)?,
                name: req_str(v, semconv::GEN_AI_TOOL_NAME)?.to_owned(),
                args_hash: crate::ids::ArgsHash::from_hex(hash.to_owned()),
                args,
                policy: ToolPolicy::parse(req_str(v, "policy")?)?,
                workspace_rev: SnapshotRev::new(
                    v.get("workspace_rev").and_then(|r| r.as_u64()).unwrap_or(0),
                ),
            })
        }
        "tool_applied" => Ok(Event::ToolApplied {
            call_id: CallId::parse(req_str(v, semconv::GEN_AI_TOOL_CALL_ID)?)?,
            result_ref: match v.get("result_ref").and_then(|r| r.as_str()) {
                Some(s) => Some(BlobRef::parse(s)?),
                None => None,
            },
            result_text: v
                .get("result_text")
                .and_then(|t| t.as_str())
                .map(|s| s.to_owned()),
            workspace_rev: SnapshotRev::new(
                v.get("workspace_rev").and_then(|r| r.as_u64()).unwrap_or(0),
            ),
        }),
        "tool_failed" => Ok(Event::ToolFailed {
            call_id: CallId::parse(req_str(v, semconv::GEN_AI_TOOL_CALL_ID)?)?,
            error: req_str(v, "error")?.to_owned(),
        }),
        "tool_abandoned" => Ok(Event::ToolAbandoned {
            call_id: CallId::parse(req_str(v, semconv::GEN_AI_TOOL_CALL_ID)?)?,
            reason: req_str(v, "reason")?.to_owned(),
        }),
        other => Err(Error::bundle(format!("unknown event type {other}"))),
    }
}

fn req_str<'a>(v: &'a Value, key: &str) -> Result<&'a str, Error> {
    v.get(key)
        .and_then(|s| s.as_str())
        .ok_or_else(|| Error::bundle(format!("missing field {key}")))
}

fn ledger_row(event: &Event) -> Option<Value> {
    match event {
        Event::ToolPending {
            call_id,
            name,
            args_hash,
            policy,
            ..
        } => Some(json!({
            "tool": name,
            "args_hash": args_hash.as_str(),
            "status": SideEffectStatus::Pending.as_str(),
            semconv::GEN_AI_TOOL_CALL_ID: call_id.as_str(),
            "policy": policy.as_str()
        })),
        Event::ToolApplied {
            call_id,
            result_ref,
            ..
        } => Some(json!({
            "tool": "",
            "result_ref": result_ref.as_ref().map(|r| r.uri()),
            "status": "applied",
            semconv::GEN_AI_TOOL_CALL_ID: call_id.as_str()
        })),
        Event::ToolFailed { call_id, .. } => Some(json!({
            "status": "failed",
            semconv::GEN_AI_TOOL_CALL_ID: call_id.as_str()
        })),
        Event::ToolAbandoned { call_id, .. } => Some(json!({
            "status": "abandoned",
            semconv::GEN_AI_TOOL_CALL_ID: call_id.as_str()
        })),
        _ => None,
    }
}

fn collect_event_blobs(event: &Event, out: &mut Vec<BlobRef>) {
    match event {
        Event::ToolApplied {
            result_ref: Some(r),
            ..
        } => out.push(r.clone()),
        _ => {}
    }
}

fn collect_tree_blobs(bytes: &[u8], out: &mut Vec<BlobRef>) -> Result<(), Error> {
    let tree: Tree = workspace::parse_tree(bytes)?;
    for e in tree.entries {
        out.push(BlobRef::parse(&e.blob)?);
    }
    Ok(())
}

fn read_blob(blob_dir: &Path, r: &BlobRef) -> Result<Vec<u8>, Error> {
    let hex = r.as_hex();
    let path = blob_dir.join(&hex[..2]).join(hex);
    fs::read(&path).map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            Error::corrupt(format!("missing blob {}", r.uri()))
        } else {
            Error::Io(e)
        }
    })
}

fn copy_blob(src_blobs: &Path, dest_blobs: &Path, r: &BlobRef) -> Result<(), Error> {
    let hex = r.as_hex();
    let src = src_blobs.join(&hex[..2]).join(hex);
    if !src.exists() {
        return Err(Error::corrupt(format!("missing blob {}", r.uri())));
    }
    let dest_dir = dest_blobs.join(&hex[..2]);
    fs::create_dir_all(&dest_dir)?;
    let dest = dest_dir.join(hex);
    if !dest.exists() {
        fs::copy(&src, &dest)?;
    }
    Ok(())
}

fn copy_all_blobs(src: &Path, dest: &Path) -> Result<(), Error> {
    if !src.exists() {
        return Ok(());
    }
    for ent in walkdir::WalkDir::new(src).follow_links(false) {
        let ent = ent.map_err(|e| Error::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?;
        if !ent.file_type().is_file() {
            continue;
        }
        let name = ent.file_name().to_string_lossy();
        if name.ends_with(".part") {
            continue;
        }
        let rel = ent.path().strip_prefix(src).unwrap();
        let dest_path = dest.join(rel);
        if let Some(parent) = dest_path.parent() {
            fs::create_dir_all(parent)?;
        }
        if !dest_path.exists() {
            fs::copy(ent.path(), &dest_path)?;
        }
    }
    Ok(())
}

fn ensure_event_blobs(store: &Store, event: &Event) -> Result<(), Error> {
    match event {
        Event::WorkspaceSnapshotted { tree, .. } => {
            let bytes = store.get_blob(tree)?;
            let parsed = workspace::parse_tree(&bytes)?;
            for e in parsed.entries {
                let _ = store.get_blob(&BlobRef::parse(&e.blob)?)?;
            }
            Ok(())
        }
        Event::ToolApplied {
            result_ref: Some(r),
            ..
        } => {
            let _ = store.get_blob(r)?;
            Ok(())
        }
        _ => Ok(()),
    }
}

pub(crate) fn rfc3339_millis(ms: i64) -> String {
    let secs = ms.div_euclid(1000);
    let millis = ms.rem_euclid(1000) as u32;
    let days = secs.div_euclid(86_400);
    let tod = secs.rem_euclid(86_400) as u32;
    let hour = tod / 3600;
    let min = (tod % 3600) / 60;
    let sec = tod % 60;
    let (y, m, d) = civil_from_days(days);
    format!("{y:04}-{m:02}-{d:02}T{hour:02}:{min:02}:{sec:02}.{millis:03}Z")
}

pub(crate) fn parse_rfc3339(s: &str) -> Result<i64, Error> {
    let s = s.trim();
    let (body, offset_min) = if let Some(rest) = s.strip_suffix('Z').or_else(|| s.strip_suffix('z'))
    {
        (rest, 0i64)
    } else if let Some(idx) = s.rfind(['+', '-']) {
        if idx < 11 {
            return Err(Error::bundle(format!("bad timestamp {s}")));
        }
        let (body, off) = s.split_at(idx);
        let sign = if off.starts_with('-') { -1 } else { 1 };
        let off = off.trim_start_matches(['+', '-']);
        let parts: Vec<&str> = off.split(':').collect();
        let h: i64 = parts
            .first()
            .and_then(|p| p.parse().ok())
            .ok_or_else(|| Error::bundle(format!("bad timestamp {s}")))?;
        let m: i64 = if parts.len() > 1 {
            parts[1].parse().unwrap_or(0)
        } else {
            0
        };
        (body, sign * (h * 60 + m))
    } else {
        (s, 0)
    };
    let (date, time) = body
        .split_once('T')
        .or_else(|| body.split_once('t'))
        .ok_or_else(|| Error::bundle(format!("bad timestamp {s}")))?;
    let mut dp = date.split('-');
    let y: i32 = dp
        .next()
        .and_then(|p| p.parse().ok())
        .ok_or_else(|| Error::bundle(format!("bad timestamp {s}")))?;
    let mo: u32 = dp
        .next()
        .and_then(|p| p.parse().ok())
        .ok_or_else(|| Error::bundle(format!("bad timestamp {s}")))?;
    let d: u32 = dp
        .next()
        .and_then(|p| p.parse().ok())
        .ok_or_else(|| Error::bundle(format!("bad timestamp {s}")))?;
    let mut tp = time.split(':');
    let hour: i64 = tp
        .next()
        .and_then(|p| p.parse().ok())
        .ok_or_else(|| Error::bundle(format!("bad timestamp {s}")))?;
    let min: i64 = tp
        .next()
        .and_then(|p| p.parse().ok())
        .ok_or_else(|| Error::bundle(format!("bad timestamp {s}")))?;
    let sec_s = tp
        .next()
        .ok_or_else(|| Error::bundle(format!("bad timestamp {s}")))?;
    let (sec_s, frac) = sec_s.split_once('.').unwrap_or((sec_s, "0"));
    let sec: i64 = sec_s
        .parse()
        .map_err(|_| Error::bundle(format!("bad timestamp {s}")))?;
    let mut frac = frac.to_string();
    frac.truncate(3);
    while frac.len() < 3 {
        frac.push('0');
    }
    let millis: i64 = frac
        .parse()
        .map_err(|_| Error::bundle(format!("bad timestamp {s}")))?;
    let days = days_from_civil(y, mo, d);
    let tod = hour * 3600 + min * 60 + sec - offset_min * 60;
    Ok(days * 86_400_000 + tod * 1000 + millis)
}

fn civil_from_days(z: i64) -> (i32, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y as i32, m as u32, d as u32)
}

fn days_from_civil(y: i32, m: u32, d: u32) -> i64 {
    let y = y as i64;
    let m = m as i64;
    let d = d as i64;
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u64;
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy as u64;
    era * 146_097 + doe as i64 - 719_468
}
