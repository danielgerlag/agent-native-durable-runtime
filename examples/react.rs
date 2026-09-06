//! ~200-line ReAct loop: fake model, idempotent write, at-most-once email.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use durable_session::{
    AssistantDelta, Filter, FinishReason, Input, Message, OpId, OpenOptions, Recovery, Session,
    SessionId, SideEffectStatus, ToolPolicy, ToolResult, ToolRun, ToolSpec, TurnId, WorkerId,
};
use serde_json::{json, Value};

fn main() {
    if let Err(e) = run() {
        eprintln!("{e}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), durable_session::Error> {
    let root = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("demo-run"));
    fs::create_dir_all(&root)?;
    let oracle = root.join("oracle.jsonl");
    let mut session = Session::open(OpenOptions {
        store_dir: root.join(".durable"),
        session_id: SessionId::parse("sess_demo")?,
        worker: WorkerId::this_process(),
        workspace: root.join("work"),
        ttl: Duration::from_secs(60),
        filter: Filter::default(),
    })?;
    recover(&mut session, &oracle)?;
    session.append(
        OpId::from_static("user-1"),
        Input::user("write out.txt then email a@b.c"),
    )?;
    loop {
        match next_action(session.messages()) {
            Action::Write => {
                stream_and_seal(&mut session, "Writing the file.", FinishReason::ToolCalls)?;
                invoke(
                    &mut session,
                    &oracle,
                    ToolSpec::idempotent("idempotent_write"),
                    json!({"path": "out.txt", "contents": "hello from react"}),
                )?;
            }
            Action::Email => {
                stream_and_seal(&mut session, "Sending the email.", FinishReason::ToolCalls)?;
                invoke(
                    &mut session,
                    &oracle,
                    ToolSpec::at_most_once("at_most_once_email"),
                    json!({"to": "a@b.c", "body": "hi"}),
                )?;
            }
            Action::Stop => {
                let already_stopped = session.messages().iter().any(|m| {
                    matches!(
                        m,
                        Message::Assistant {
                            finish_reason: FinishReason::Stop,
                            ..
                        }
                    )
                });
                if !already_stopped {
                    stream_and_seal(&mut session, "Done.", FinishReason::Stop)?;
                }
                break;
            }
        }
    }
    println!("workspace: {}", session.workspace_path().display());
    for m in session.messages() {
        match m {
            Message::User { content, .. } => println!("user: {content}"),
            Message::Assistant {
                content,
                finish_reason,
                ..
            } => {
                println!("assistant[{}]: {content}", finish_reason.as_str());
            }
            Message::Tool { name, status, .. } => {
                println!("tool {name}: {}", status.as_str());
            }
            Message::System { content, .. } => println!("system: {content}"),
        }
    }
    Ok(())
}

enum Action {
    Write,
    Email,
    Stop,
}

fn status_of(messages: &[Message], name: &str) -> Option<SideEffectStatus> {
    messages.iter().rev().find_map(|m| match m {
        Message::Tool {
            name: n, status, ..
        } if n == name => Some(*status),
        _ => None,
    })
}

fn next_action(messages: &[Message]) -> Action {
    let write = status_of(messages, "idempotent_write");
    let email = status_of(messages, "at_most_once_email");
    if write != Some(SideEffectStatus::Applied) {
        Action::Write
    } else if email != Some(SideEffectStatus::Applied) && email != Some(SideEffectStatus::Abandoned)
    {
        Action::Email
    } else {
        Action::Stop
    }
}

fn recover(session: &mut Session, oracle: &Path) -> Result<(), durable_session::Error> {
    match session.recovery() {
        Recovery::PendingTool(p) if p.policy == ToolPolicy::AtMostOnce => {
            session.abandon_tool(&p.call_id, "not retried")?;
        }
        Recovery::PendingTool(p) if p.policy == ToolPolicy::Idempotent => {
            invoke(session, oracle, ToolSpec::idempotent(p.name), p.args)?;
        }
        Recovery::Truncated { prefix, .. } => {
            eprintln!("resumed after truncated turn: {prefix}");
        }
        _ => {}
    }
    Ok(())
}

fn stream_and_seal(
    session: &mut Session,
    text: &str,
    reason: FinishReason,
) -> Result<(), durable_session::Error> {
    let turn = TurnId::new();
    session.append_assistant_chunk(AssistantDelta::text(turn.clone(), text))?;
    session.seal_assistant(reason, Some("fake-model".into()))
}

fn invoke(
    session: &mut Session,
    oracle: &Path,
    spec: ToolSpec,
    args: Value,
) -> Result<(), durable_session::Error> {
    let name = spec.name.clone();
    match session.run_tool(spec, args.clone(), |ctx| {
        append_oracle(oracle, &name, ctx.call_id().as_str())?;
        if name == "idempotent_write" {
            let path = args["path"].as_str().ok_or("path")?;
            let contents = args["contents"].as_str().ok_or("contents")?;
            fs::write(ctx.path(path), contents).map_err(|e| e.to_string())?;
        }
        Ok(ToolResult::text("ok"))
    })? {
        ToolRun::Inspect(p) => session.abandon_tool(&p.call_id, "inspect")?,
        ToolRun::AlreadyApplied(_) | ToolRun::Completed(_) => {}
    }
    Ok(())
}

fn append_oracle(path: &Path, tool: &str, call_id: &str) -> Result<(), String> {
    let mut body = fs::read_to_string(path).unwrap_or_default();
    body.push_str(&format!("{tool} {call_id}\n"));
    let part = path.with_extension("part");
    fs::write(&part, &body).map_err(|e| e.to_string())?;
    fs::rename(part, path).map_err(|e| e.to_string())?;
    Ok(())
}
