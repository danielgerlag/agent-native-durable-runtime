use std::fs;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use durable_session::{
    AssistantDelta, CrashAtNth, Error, Filter, FinishReason, Hooks, Input, Message, OpId,
    OpenOptions, Recovery, Session, SessionId, SideEffectStatus, ToolPolicy, ToolResult, ToolRun,
    ToolSpec, TurnId, WorkerId,
};
use serde_json::{json, Value};

const WRITE: &str = "idempotent_write";
const EMAIL: &str = "at_most_once_email";
const USER_OP: &str = "user-1";
const CHILD_DIR: &str = "DURABLE_SESSION_CHILD_DIR";
const CHILD_N: &str = "DURABLE_SESSION_ABORT_N";
const CHILD_ORACLE: &str = "DURABLE_SESSION_CHILD_ORACLE";

fn opts(root: &Path) -> OpenOptions {
    OpenOptions {
        store_dir: root.join("store"),
        session_id: SessionId::parse("sess_demo").unwrap(),
        worker: WorkerId::parse("conformance").unwrap(),
        workspace: root.join("work"),
        ttl: Duration::from_secs(60),
        filter: Filter::default(),
    }
}

fn oracle_append(path: &Path, tool: &str, args_hash: &str, call_id: &str) -> Result<(), String> {
    let mut body = if path.exists() {
        fs::read_to_string(path).map_err(|e| e.to_string())?
    } else {
        String::new()
    };
    if !body.is_empty() && !body.ends_with('\n') {
        body.push('\n');
    }
    body.push_str(&json!({"tool": tool, "args_hash": args_hash, "call_id": call_id}).to_string());
    body.push('\n');
    let part = path.with_extension("jsonl.part");
    fs::write(&part, &body).map_err(|e| e.to_string())?;
    fs::rename(&part, path).map_err(|e| e.to_string())?;
    Ok(())
}

fn read_oracle(path: &Path) -> Vec<Value> {
    if !path.exists() {
        return Vec::new();
    }
    fs::read_to_string(path)
        .unwrap()
        .lines()
        .filter(|l| !l.is_empty())
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

fn tool_named(messages: &[Message], name: &str) -> Option<SideEffectStatus> {
    messages.iter().rev().find_map(|m| match m {
        Message::Tool {
            name: n, status, ..
        } if n == name => Some(*status),
        _ => None,
    })
}

fn run_tool(
    session: &mut Session,
    oracle: &Path,
    spec: ToolSpec,
    args: Value,
) -> Result<(), Error> {
    let name = spec.name.clone();
    let hash = args.to_string();
    match session.run_tool(spec, args.clone(), |ctx| {
        oracle_append(oracle, &name, &hash, ctx.call_id().as_str())?;
        if name == WRITE {
            let path = args
                .get("path")
                .and_then(|p| p.as_str())
                .ok_or_else(|| "missing path".to_string())?;
            let contents = args
                .get("contents")
                .and_then(|p| p.as_str())
                .ok_or_else(|| "missing contents".to_string())?;
            if let Some(parent) = ctx.path(path).parent() {
                fs::create_dir_all(parent).map_err(|e| e.to_string())?;
            }
            fs::write(ctx.path(path), contents).map_err(|e| e.to_string())?;
        }
        Ok(ToolResult::text("ok"))
    })? {
        ToolRun::Inspect(p) => session.abandon_tool(&p.call_id, "at-most-once after crash")?,
        ToolRun::AlreadyApplied(_) | ToolRun::Completed(_) => {}
    }
    Ok(())
}

fn stream(session: &mut Session, text: &str) -> Result<TurnId, Error> {
    let turn = TurnId::new();
    for chunk in text.as_bytes().chunks(16) {
        let s = std::str::from_utf8(chunk).unwrap();
        session.append_assistant_chunk(AssistantDelta::text(turn.clone(), s))?;
    }
    Ok(turn)
}

fn drive(session: &mut Session, oracle: &Path) -> Result<(), Error> {
    match session.recovery() {
        Recovery::PendingTool(p) if p.policy == ToolPolicy::AtMostOnce => {
            session.abandon_tool(&p.call_id, "at-most-once after crash")?;
        }
        Recovery::PendingTool(p) if p.policy == ToolPolicy::Idempotent => {
            run_tool(session, oracle, ToolSpec::idempotent(WRITE), p.args)?;
        }
        _ => {}
    }

    session.append(OpId::from_static(USER_OP), Input::user("write then email"))?;

    loop {
        let write_st = tool_named(session.messages(), WRITE);
        let email_st = tool_named(session.messages(), EMAIL);
        let stopped = session.messages().iter().any(|m| {
            matches!(
                m,
                Message::Assistant {
                    finish_reason: FinishReason::Stop,
                    ..
                }
            )
        });
        if write_st == Some(SideEffectStatus::Applied)
            && (email_st == Some(SideEffectStatus::Applied)
                || email_st == Some(SideEffectStatus::Abandoned))
            && stopped
        {
            break;
        }
        if write_st != Some(SideEffectStatus::Applied) {
            let turn = stream(session, "I will write the file.")?;
            session.seal_assistant(FinishReason::ToolCalls, Some("fake".into()))?;
            let _ = turn;
            run_tool(
                session,
                oracle,
                ToolSpec::idempotent(WRITE),
                json!({"path": "out.txt", "contents": "hello"}),
            )?;
            continue;
        }
        if email_st != Some(SideEffectStatus::Applied)
            && email_st != Some(SideEffectStatus::Abandoned)
        {
            let turn = stream(session, "I will send the email.")?;
            session.seal_assistant(FinishReason::ToolCalls, Some("fake".into()))?;
            let _ = turn;
            run_tool(
                session,
                oracle,
                ToolSpec::at_most_once(EMAIL),
                json!({"to": "a@b.c", "body": "hi"}),
            )?;
            continue;
        }
        let turn = stream(session, "Done.")?;
        session.seal_assistant(FinishReason::Stop, Some("fake".into()))?;
        let _ = turn;
        break;
    }
    Ok(())
}

fn open_and_drive(root: &Path, oracle: &Path, hooks: Hooks) -> Result<Session, Error> {
    let mut session = Session::open_with_hooks(opts(root), hooks)?;
    drive(&mut session, oracle)?;
    Ok(session)
}

fn assert_invariants(oracle: &Path, session: &Session) {
    let lines = read_oracle(oracle);
    let email: Vec<_> = lines
        .iter()
        .filter(|v| v.get("tool").and_then(|t| t.as_str()) == Some(EMAIL))
        .collect();
    assert!(
        email.len() <= 1,
        "at-most-once email invoked {} times: {email:?}",
        email.len()
    );

    let write_applied = session.events().iter().filter(|e| {
        matches!(&e.event, durable_session::Event::ToolApplied { .. })
            && tool_named(session.messages(), WRITE) == Some(SideEffectStatus::Applied)
    });
    let _ = write_applied;

    let mut applied_write = 0u32;
    let mut pending_write_ids = std::collections::HashSet::new();
    for e in session.events() {
        match &e.event {
            durable_session::Event::ToolPending { name, call_id, .. } if name == WRITE => {
                pending_write_ids.insert(call_id.as_str().to_owned());
            }
            durable_session::Event::ToolApplied { call_id, .. }
                if pending_write_ids.contains(call_id.as_str()) =>
            {
                applied_write += 1;
            }
            _ => {}
        }
    }
    assert!(
        applied_write <= 1,
        "idempotent write Applied count {applied_write}"
    );
    if applied_write == 1 {
        let write_calls: std::collections::HashSet<_> = lines
            .iter()
            .filter(|v| v.get("tool").and_then(|t| t.as_str()) == Some(WRITE))
            .filter_map(|v| {
                v.get("call_id")
                    .and_then(|c| c.as_str())
                    .map(|s| s.to_owned())
            })
            .collect();
        assert!(
            write_calls.len() <= 1,
            "idempotent write used multiple call ids after apply: {write_calls:?}"
        );
    }

    let mut turns = std::collections::HashSet::new();
    for m in session.messages() {
        if let Message::Assistant {
            turn,
            finish_reason,
            ..
        } = m
        {
            assert!(
                turns.insert(turn.as_str().to_owned()),
                "assistant turn sampled twice"
            );
            if *finish_reason == FinishReason::TruncatedCrash {
                // prefix remains; a later turn may continue the work
            }
        }
    }

    let out = session.workspace_path().join("out.txt");
    if out.exists() {
        assert_eq!(fs::read_to_string(&out).unwrap(), "hello");
    }
    if tool_named(session.messages(), WRITE) == Some(SideEffectStatus::Applied) {
        assert_eq!(fs::read_to_string(&out).unwrap(), "hello");
    }
}

fn dry_run_count(root: &Path, oracle: &Path) -> usize {
    let crash = Arc::new(CrashAtNth::new(0, false));
    let hooks = Hooks {
        fault: Some(crash.clone()),
        synchronous_full: true,
        ..Hooks::default()
    };
    open_and_drive(root, oracle, hooks).unwrap();
    crash.count()
}

#[test]
fn conformance_crash_at_each_persist_op() {
    let probe = tempfile::tempdir().unwrap();
    let oracle_probe = probe.path().join("oracle.jsonl");
    let n = dry_run_count(probe.path(), &oracle_probe);
    assert!(n > 0, "expected persist ops");

    for i in 1..=n {
        let tmp = tempfile::tempdir().unwrap();
        let oracle = tmp.path().join("oracle.jsonl");
        let crash = Arc::new(CrashAtNth::new(i, false));
        let hooks = Hooks {
            fault: Some(crash.clone()),
            synchronous_full: true,
            ..Hooks::default()
        };
        let err = match open_and_drive(tmp.path(), &oracle, hooks) {
            Err(e) => e,
            Ok(_) => panic!("crash at {i}/{n} completed without Injected"),
        };
        assert!(
            matches!(err, Error::Injected),
            "crash at {i}/{n} got {err:?}"
        );

        let session = open_and_drive(tmp.path(), &oracle, Hooks::default()).unwrap();
        assert_invariants(&oracle, &session);
        let email: Vec<_> = read_oracle(&oracle)
            .into_iter()
            .filter(|v| v.get("tool").and_then(|t| t.as_str()) == Some(EMAIL))
            .collect();
        assert!(email.len() <= 1, "n={i} email oracle {email:?}");
    }
}

#[test]
fn conformance_subprocess_abort_lane() {
    if let Ok(dir) = std::env::var(CHILD_DIR) {
        let n: usize = std::env::var(CHILD_N)
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1);
        let oracle = std::env::var(CHILD_ORACLE).unwrap_or_else(|_| {
            Path::new(&dir)
                .join("oracle.jsonl")
                .to_string_lossy()
                .into_owned()
        });
        let crash = Arc::new(CrashAtNth::new(n, true));
        let hooks = Hooks {
            fault: Some(crash),
            synchronous_full: true,
            ..Hooks::default()
        };
        let _ = open_and_drive(Path::new(&dir), Path::new(&oracle), hooks);
        panic!("child should have aborted");
    }

    let tmp = tempfile::tempdir().unwrap();
    let oracle = tmp.path().join("oracle.jsonl");
    let probe = tempfile::tempdir().unwrap();
    let n = dry_run_count(probe.path(), &probe.path().join("oracle.jsonl"));
    let n = (n / 2).max(1);

    let exe = std::env::current_exe().expect("current exe");
    let status = Command::new(&exe)
        .arg("conformance_subprocess_abort_lane")
        .arg("--exact")
        .arg("--nocapture")
        .env(CHILD_DIR, tmp.path())
        .env(CHILD_N, n.to_string())
        .env(CHILD_ORACLE, &oracle)
        .status()
        .expect("spawn child");
    assert!(!status.success(), "child should abort at persist op {n}");
    assert!(
        tmp.path().join("store").join("durable.sqlite").exists()
            || tmp.path().join("store").exists()
    );

    let session = open_and_drive(
        tmp.path(),
        &oracle,
        Hooks {
            synchronous_full: true,
            ..Hooks::default()
        },
    )
    .unwrap();
    assert_invariants(&oracle, &session);
}
