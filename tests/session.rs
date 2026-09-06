use std::fs;
use std::time::Duration;

use durable_session::{
    import_bundle, AssistantDelta, Filter, FinishReason, Input, OpId, OpenOptions, Recovery,
    Session, SessionId, SessionView, ToolDisposition, ToolPolicy, ToolResult, ToolSpec, WorkerId,
};
use serde_json::json;

fn open_pair(tmp: &tempfile::TempDir, worker: &str) -> (OpenOptions, Session) {
    let opts = OpenOptions {
        store_dir: tmp.path().join("store"),
        session_id: SessionId::parse("sess_demo").unwrap(),
        worker: WorkerId::parse(worker).unwrap(),
        workspace: tmp.path().join("work"),
        ttl: Duration::from_secs(60),
        filter: Filter::default(),
    };
    let session = Session::open(opts.clone()).unwrap();
    (opts, session)
}

#[test]
fn append_is_idempotent_on_op_id() {
    let tmp = tempfile::tempdir().unwrap();
    let (_, mut session) = open_pair(&tmp, "w1");
    session
        .append(OpId::from_static("req-1"), Input::user("hello"))
        .unwrap();
    session
        .append(OpId::from_static("req-1"), Input::user("ignored"))
        .unwrap();
    let users: Vec<_> = session
        .messages()
        .iter()
        .filter_map(|m| match m {
            durable_session::Message::User { content, .. } => Some(content.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(users, vec!["hello".to_string()]);
}

#[test]
fn begin_tool_already_applied_on_same_hash() {
    let tmp = tempfile::tempdir().unwrap();
    let (_, mut session) = open_pair(&tmp, "w1");
    session
        .append(OpId::from_static("u1"), Input::user("write"))
        .unwrap();
    let turn = durable_session::TurnId::new();
    session
        .append_assistant_chunk(AssistantDelta::text(turn.clone(), "ok"))
        .unwrap();
    session
        .seal_assistant(FinishReason::ToolCalls, Some("fake".into()))
        .unwrap();
    let args = json!({"path": "a.txt", "contents": "hi"});
    let spec = ToolSpec::idempotent("idempotent_write");
    match session.begin_tool(spec.clone(), args.clone()).unwrap() {
        ToolDisposition::Run => {}
        other => panic!("{other:?}"),
    }
    let call_id = match session.recovery() {
        Recovery::PendingTool(p) => p.call_id,
        other => panic!("{other:?}"),
    };
    session
        .complete_tool(&call_id, ToolResult::text("ok"))
        .unwrap();
    match session.begin_tool(spec, args).unwrap() {
        ToolDisposition::AlreadyApplied(a) => assert_eq!(a.call_id, call_id),
        other => panic!("{other:?}"),
    }
}

#[test]
fn pending_at_most_once_returns_inspect() {
    let tmp = tempfile::tempdir().unwrap();
    let (opts, mut session) = open_pair(&tmp, "w1");
    session
        .append(OpId::from_static("u1"), Input::user("email"))
        .unwrap();
    let turn = durable_session::TurnId::new();
    session
        .append_assistant_chunk(AssistantDelta::text(turn.clone(), "send"))
        .unwrap();
    session
        .seal_assistant(FinishReason::ToolCalls, None)
        .unwrap();
    let args = json!({"to": "a@b.c", "body": "hi"});
    let spec = ToolSpec::at_most_once("at_most_once_email");
    assert!(matches!(
        session.begin_tool(spec.clone(), args.clone()).unwrap(),
        ToolDisposition::Run
    ));
    drop(session);

    let mut session = Session::open(opts).unwrap();
    match session.begin_tool(spec, args).unwrap() {
        ToolDisposition::Inspect(p) => {
            assert_eq!(p.policy, ToolPolicy::AtMostOnce);
            session.abandon_tool(&p.call_id, "not retried").unwrap();
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn pending_idempotent_returns_run_and_restores_before_image() {
    let tmp = tempfile::tempdir().unwrap();
    let (opts, mut session) = open_pair(&tmp, "w1");
    session
        .append(OpId::from_static("u1"), Input::user("write"))
        .unwrap();
    let turn = durable_session::TurnId::new();
    session
        .append_assistant_chunk(AssistantDelta::text(turn.clone(), "write"))
        .unwrap();
    session
        .seal_assistant(FinishReason::ToolCalls, None)
        .unwrap();
    let args = json!({"path": "a.txt", "contents": "hello"});
    let spec = ToolSpec::idempotent("idempotent_write");
    session.begin_tool(spec.clone(), args.clone()).unwrap();
    fs::write(session.workspace_path().join("a.txt"), b"dirty").unwrap();
    drop(session);

    let mut session = Session::open(opts).unwrap();
    match session.begin_tool(spec, args).unwrap() {
        ToolDisposition::Run => {}
        other => panic!("{other:?}"),
    }
    assert!(
        !session.workspace_path().join("a.txt").exists()
            || fs::read(session.workspace_path().join("a.txt")).unwrap() != b"dirty"
    );
}

#[test]
fn open_auto_seals_truncated_assistant() {
    let tmp = tempfile::tempdir().unwrap();
    let (opts, mut session) = open_pair(&tmp, "w1");
    session
        .append(OpId::from_static("u1"), Input::user("hi"))
        .unwrap();
    let turn = durable_session::TurnId::new();
    session
        .append_assistant_chunk(AssistantDelta::text(turn.clone(), "partial-"))
        .unwrap();
    session
        .append_assistant_chunk(AssistantDelta::text(turn.clone(), "answer"))
        .unwrap();
    drop(session);

    let mut session = Session::open(opts).unwrap();
    match session.recovery() {
        Recovery::Truncated { prefix, .. } => assert_eq!(prefix, "partial-answer"),
        other => panic!("{other:?}"),
    }
    match &session.messages()[1] {
        durable_session::Message::Assistant {
            content,
            finish_reason,
            ..
        } => {
            assert_eq!(content, "partial-answer");
            assert_eq!(*finish_reason, FinishReason::TruncatedCrash);
        }
        other => panic!("{other:?}"),
    }
    let turn2 = durable_session::TurnId::new();
    session
        .append_assistant_chunk(AssistantDelta::text(turn2, "new turn"))
        .unwrap();
    session.seal_assistant(FinishReason::Stop, None).unwrap();
    let sealed: Vec<_> = session
        .messages()
        .iter()
        .filter_map(|m| match m {
            durable_session::Message::Assistant {
                content,
                finish_reason,
                ..
            } => Some((content.clone(), *finish_reason)),
            _ => None,
        })
        .collect();
    assert_eq!(sealed.len(), 2);
    assert_eq!(sealed[0].1, FinishReason::TruncatedCrash);
    assert_eq!(sealed[1].0, "new turn");
}

#[test]
fn session_view_replay_from_token() {
    let tmp = tempfile::tempdir().unwrap();
    let (opts, mut session) = open_pair(&tmp, "w1");
    let token = session.resume_token();
    session
        .append(OpId::from_static("u1"), Input::user("one"))
        .unwrap();
    session
        .append(OpId::from_static("u2"), Input::user("two"))
        .unwrap();
    let mid = session.resume_token();
    drop(session);

    let view = SessionView::open(&opts.store_dir, opts.session_id.clone()).unwrap();
    let from_start = view.replay(&token).unwrap();
    assert_eq!(from_start.len(), 2);
    let from_mid = view.replay(&mid).unwrap();
    assert!(from_mid.is_empty());
    assert_eq!(view.messages().unwrap().len(), 2);
}

#[test]
fn export_import_bundle_roundtrip() {
    let tmp = tempfile::tempdir().unwrap();
    let (_, mut session) = open_pair(&tmp, "w1");
    session
        .append(OpId::from_static("u1"), Input::user("hello bundle"))
        .unwrap();
    let turn = durable_session::TurnId::new();
    session
        .append_assistant_chunk(AssistantDelta::text(turn.clone(), "hi"))
        .unwrap();
    session.seal_assistant(FinishReason::Stop, None).unwrap();
    let bundle = tmp.path().join("bundle");
    session.export_bundle(&bundle).unwrap();
    session.close().unwrap();

    let store2 = tmp.path().join("store2");
    let imported = import_bundle(&store2, &bundle).unwrap();
    assert_eq!(imported.as_str(), "sess_demo");
    let opts = OpenOptions {
        store_dir: store2,
        session_id: imported,
        worker: WorkerId::parse("w2").unwrap(),
        workspace: tmp.path().join("work2"),
        ttl: Duration::from_secs(60),
        filter: Filter::default(),
    };
    let session = Session::open(opts).unwrap();
    match &session.messages()[0] {
        durable_session::Message::User { content, .. } => {
            assert_eq!(content, "hello bundle");
        }
        other => panic!("{other:?}"),
    }
}
