use std::collections::HashMap;
use std::fmt;

use crate::error::Error;
use crate::event::{Event, Message, ToolCall};
use crate::ids::{ArgsHash, BlobRef, CallId, SnapshotRev, TurnId};
use crate::tool::{AppliedTool, FinishReason, PendingTool, SideEffectStatus, ToolPolicy};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Reject {
    SessionNotOpen,
    AlreadyClosed,
    UnexpectedTurn,
    UnknownCall,
    DuplicateCall,
    NotPending,
    EmptyChunk,
    TwoPendingTools,
    AssistantWhileToolPending,
    OutcomeMismatch,
}

impl fmt::Display for Reject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Reject::SessionNotOpen => "session not open",
            Reject::AlreadyClosed => "session already closed",
            Reject::UnexpectedTurn => "unexpected turn",
            Reject::UnknownCall => "unknown tool call",
            Reject::DuplicateCall => "duplicate tool call",
            Reject::NotPending => "tool call is not pending",
            Reject::EmptyChunk => "empty assistant chunk",
            Reject::TwoPendingTools => "two pending tools",
            Reject::AssistantWhileToolPending => "assistant input while a tool is pending",
            Reject::OutcomeMismatch => "complete_tool outcome does not match recorded Applied",
        };
        f.write_str(s)
    }
}

impl From<Reject> for Error {
    fn from(value: Reject) -> Self {
        Error::invalid(value.to_string())
    }
}

#[derive(Clone, Debug)]
pub(crate) struct UnsealedTurn {
    pub turn: TurnId,
    pub text: String,
    pub tool_calls: Vec<ToolCall>,
    pub model: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct CallRecord {
    pub status: SideEffectStatus,
    pub name: String,
    pub args_hash: ArgsHash,
    pub policy: ToolPolicy,
    pub result_ref: Option<BlobRef>,
    pub result_text: Option<String>,
    pub error: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct SessionState {
    pub opened: bool,
    pub closed: bool,
    pub unsealed: Option<UnsealedTurn>,
    pub pending: Option<PendingTool>,
    pub messages: Vec<Message>,
    pub applied_by_hash: HashMap<(String, ArgsHash), AppliedTool>,
    pub calls: HashMap<CallId, CallRecord>,
    pub workspace_head: Option<(SnapshotRev, BlobRef)>,
    pub last_seq: u64,
}

impl SessionState {
    pub(crate) fn origin() -> Self {
        Self {
            opened: true,
            closed: false,
            unsealed: None,
            pending: None,
            messages: Vec::new(),
            applied_by_hash: HashMap::new(),
            calls: HashMap::new(),
            workspace_head: None,
            last_seq: 0,
        }
    }

    pub(crate) fn unopened() -> Self {
        let mut s = Self::origin();
        s.opened = false;
        s
    }

    pub(crate) fn next_rev(&self) -> SnapshotRev {
        match self.workspace_head {
            Some((rev, _)) => SnapshotRev::new(rev.get() + 1),
            None => SnapshotRev::new(1),
        }
    }
}

fn chunk_is_empty(text: &Option<String>, tool_call: &Option<crate::event::ToolCallDelta>) -> bool {
    let text_empty = match text {
        None => true,
        Some(s) => s.is_empty(),
    };
    text_empty && tool_call.is_none()
}

fn require_open(state: &SessionState) -> Result<(), Reject> {
    if state.closed {
        return Err(Reject::AlreadyClosed);
    }
    if !state.opened {
        return Err(Reject::SessionNotOpen);
    }
    Ok(())
}

pub(crate) fn apply(state: &SessionState, event: &Event) -> Result<SessionState, Reject> {
    if let Event::SessionOpened = event {
        if state.closed {
            return Err(Reject::AlreadyClosed);
        }
        if state.opened {
            return Err(Reject::UnexpectedTurn);
        }
        let mut next = state.clone();
        next.opened = true;
        return Ok(next);
    }

    if state.closed {
        return Err(Reject::AlreadyClosed);
    }
    if !state.opened {
        return Err(Reject::SessionNotOpen);
    }

    match event {
        Event::SessionOpened => unreachable!(),
        Event::SessionClosed => {
            let mut next = state.clone();
            next.closed = true;
            Ok(next)
        }
        Event::User { content, op } => {
            if state.pending.is_some() {
                return Err(Reject::AssistantWhileToolPending);
            }
            if state.unsealed.is_some() {
                return Err(Reject::UnexpectedTurn);
            }
            let mut next = state.clone();
            next.messages.push(Message::User {
                content: content.clone(),
                op: op.clone(),
            });
            Ok(next)
        }
        Event::System { content, op } => {
            if state.pending.is_some() {
                return Err(Reject::AssistantWhileToolPending);
            }
            if state.unsealed.is_some() {
                return Err(Reject::UnexpectedTurn);
            }
            let mut next = state.clone();
            next.messages.push(Message::System {
                content: content.clone(),
                op: op.clone(),
            });
            Ok(next)
        }
        Event::AssistantDelta {
            turn,
            text,
            tool_call,
        } => {
            if state.pending.is_some() {
                return Err(Reject::AssistantWhileToolPending);
            }
            if chunk_is_empty(text, tool_call) {
                return Err(Reject::EmptyChunk);
            }
            let mut next = state.clone();
            match next.unsealed.as_mut() {
                None => {
                    let mut tool_calls = Vec::new();
                    if let Some(tc) = tool_call {
                        tool_calls.push(ToolCall {
                            id: tc.id.clone(),
                            name: tc.name.clone(),
                            arguments: tc.args_delta.clone(),
                        });
                    }
                    next.unsealed = Some(UnsealedTurn {
                        turn: turn.clone(),
                        text: text.clone().unwrap_or_default(),
                        tool_calls,
                        model: None,
                    });
                }
                Some(unsealed) => {
                    if unsealed.turn != *turn {
                        return Err(Reject::UnexpectedTurn);
                    }
                    if let Some(t) = text {
                        unsealed.text.push_str(t);
                    }
                    if let Some(tc) = tool_call {
                        if let Some(existing) =
                            unsealed.tool_calls.iter_mut().find(|c| c.id == tc.id)
                        {
                            existing.arguments.push_str(&tc.args_delta);
                            if !tc.name.is_empty() {
                                existing.name = tc.name.clone();
                            }
                        } else {
                            unsealed.tool_calls.push(ToolCall {
                                id: tc.id.clone(),
                                name: tc.name.clone(),
                                arguments: tc.args_delta.clone(),
                            });
                        }
                    }
                }
            }
            Ok(next)
        }
        Event::AssistantSealed {
            turn,
            finish_reason,
            model,
            ..
        } => {
            let unsealed = match &state.unsealed {
                Some(u) if u.turn == *turn => u.clone(),
                _ => return Err(Reject::UnexpectedTurn),
            };
            let mut next = state.clone();
            next.unsealed = None;
            next.messages.push(Message::Assistant {
                turn: unsealed.turn,
                content: unsealed.text,
                tool_calls: unsealed.tool_calls,
                finish_reason: *finish_reason,
                model: model.clone().or(unsealed.model),
            });
            Ok(next)
        }
        Event::WorkspaceSnapshotted { rev, tree } => {
            require_open(state)?;
            let mut next = state.clone();
            next.workspace_head = Some((*rev, tree.clone()));
            Ok(next)
        }
        Event::ToolPending {
            call_id,
            name,
            args_hash,
            args,
            policy,
            workspace_rev,
        } => {
            if state.unsealed.is_some() {
                return Err(Reject::UnexpectedTurn);
            }
            if state.pending.is_some() {
                return Err(Reject::TwoPendingTools);
            }
            if state.calls.contains_key(call_id) {
                return Err(Reject::DuplicateCall);
            }
            if state
                .applied_by_hash
                .contains_key(&(name.clone(), args_hash.clone()))
            {
                return Err(Reject::DuplicateCall);
            }
            let pending = PendingTool {
                call_id: call_id.clone(),
                name: name.clone(),
                args_hash: args_hash.clone(),
                args: args.clone(),
                policy: *policy,
                workspace_rev: *workspace_rev,
            };
            let mut next = state.clone();
            next.calls.insert(
                call_id.clone(),
                CallRecord {
                    status: SideEffectStatus::Pending,
                    name: name.clone(),
                    args_hash: args_hash.clone(),
                    policy: *policy,
                    result_ref: None,
                    result_text: None,
                    error: None,
                },
            );
            next.pending = Some(pending);
            Ok(next)
        }
        Event::ToolApplied {
            call_id,
            result_ref,
            result_text,
            workspace_rev,
        } => apply_terminal(
            state,
            call_id,
            SideEffectStatus::Applied,
            result_ref.clone(),
            result_text.clone(),
            None,
            Some(*workspace_rev),
        ),
        Event::ToolFailed { call_id, error } => apply_terminal(
            state,
            call_id,
            SideEffectStatus::Failed,
            None,
            None,
            Some(error.clone()),
            None,
        ),
        Event::ToolAbandoned { call_id, reason } => apply_terminal(
            state,
            call_id,
            SideEffectStatus::Abandoned,
            None,
            None,
            Some(reason.clone()),
            None,
        ),
    }
}

fn apply_terminal(
    state: &SessionState,
    call_id: &CallId,
    status: SideEffectStatus,
    result_ref: Option<BlobRef>,
    result_text: Option<String>,
    error: Option<String>,
    workspace_rev: Option<SnapshotRev>,
) -> Result<SessionState, Reject> {
    match &state.pending {
        Some(pending) if pending.call_id == *call_id => {
            let mut next = state.clone();
            next.pending = None;
            if let Some(record) = next.calls.get_mut(call_id) {
                record.status = status;
                record.result_ref = result_ref.clone();
                record.result_text = result_text.clone();
                record.error = error.clone();
            }
            if status == SideEffectStatus::Applied {
                let rev = workspace_rev.unwrap_or(pending.workspace_rev);
                let applied = AppliedTool {
                    call_id: call_id.clone(),
                    name: pending.name.clone(),
                    args_hash: pending.args_hash.clone(),
                    result_ref: result_ref.clone(),
                    result_text: result_text.clone(),
                    workspace_rev: rev,
                };
                next.applied_by_hash
                    .insert((pending.name.clone(), pending.args_hash.clone()), applied);
            }
            next.messages.push(Message::Tool {
                call_id: call_id.clone(),
                name: pending.name.clone(),
                status,
                result_text,
                error,
            });
            Ok(next)
        }
        Some(_) => Err(Reject::UnknownCall),
        None => {
            if let Some(existing) = state.calls.get(call_id) {
                if existing.status == SideEffectStatus::Applied && status == SideEffectStatus::Applied
                {
                    let same = existing.result_ref == result_ref
                        && existing.result_text == result_text;
                    if same {
                        return Ok(state.clone());
                    }
                    return Err(Reject::OutcomeMismatch);
                }
                return Err(Reject::NotPending);
            }
            Err(Reject::UnknownCall)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::ToolCallDelta;
    use crate::ids::{CallId, OpId};
    use crate::tool::args_hash;
    use serde_json::json;

    fn user(s: &str) -> Event {
        Event::User {
            content: s.into(),
            op: OpId::from_static("op-user"),
        }
    }

    fn delta(turn: &TurnId, text: &str) -> Event {
        Event::AssistantDelta {
            turn: turn.clone(),
            text: Some(text.into()),
            tool_call: None,
        }
    }

    fn seal(turn: &TurnId) -> Event {
        Event::AssistantSealed {
            turn: turn.clone(),
            finish_reason: FinishReason::Stop,
            model: None,
            input_tokens: None,
            output_tokens: None,
        }
    }

    fn pending(call: &str, name: &str, args: serde_json::Value) -> Event {
        Event::ToolPending {
            call_id: CallId::parse(call).unwrap(),
            name: name.into(),
            args_hash: args_hash(&args),
            args,
            policy: ToolPolicy::Idempotent,
            workspace_rev: SnapshotRev::new(1),
        }
    }

    fn fold(events: &[Event]) -> SessionState {
        let mut state = SessionState::origin();
        for event in events {
            state = apply(&state, event).expect("legal prefix");
        }
        state
    }

    fn reject_of(prefix: &[Event], last: Event) -> Reject {
        let state = fold(prefix);
        apply(&state, &last).unwrap_err()
    }

    #[test]
    fn reducer_user_after_close_rejected() {
        assert_eq!(
            reject_of(&[Event::SessionClosed], user("hi")),
            Reject::AlreadyClosed
        );
    }

    #[test]
    fn reducer_empty_chunk_rejected() {
        let turn = TurnId::parse("t1").unwrap();
        assert_eq!(
            reject_of(
                &[user("hi")],
                Event::AssistantDelta {
                    turn,
                    text: None,
                    tool_call: None,
                }
            ),
            Reject::EmptyChunk
        );
    }

    #[test]
    fn reducer_empty_string_chunk_rejected() {
        let turn = TurnId::parse("t1").unwrap();
        assert_eq!(
            reject_of(&[user("hi")], delta(&turn, "")),
            Reject::EmptyChunk
        );
    }

    #[test]
    fn reducer_assistant_while_pending_rejected() {
        let turn = TurnId::parse("t1").unwrap();
        let prefix = [
            user("hi"),
            delta(&turn, "call"),
            Event::AssistantSealed {
                turn: turn.clone(),
                finish_reason: FinishReason::ToolCalls,
                model: None,
                input_tokens: None,
                output_tokens: None,
            },
            pending("c1", "write", json!({"path": "a"})),
        ];
        let next_turn = TurnId::parse("t2").unwrap();
        assert_eq!(
            reject_of(&prefix, delta(&next_turn, "nope")),
            Reject::AssistantWhileToolPending
        );
    }

    #[test]
    fn reducer_two_pendings_rejected() {
        let turn = TurnId::parse("t1").unwrap();
        let prefix = [
            user("hi"),
            delta(&turn, "call"),
            Event::AssistantSealed {
                turn: turn.clone(),
                finish_reason: FinishReason::ToolCalls,
                model: None,
                input_tokens: None,
                output_tokens: None,
            },
            pending("c1", "write", json!({"path": "a"})),
        ];
        assert_eq!(
            reject_of(&prefix, pending("c2", "email", json!({"to": "a"}))),
            Reject::TwoPendingTools
        );
    }

    #[test]
    fn reducer_complete_unknown_call_rejected() {
        assert_eq!(
            reject_of(
                &[user("hi")],
                Event::ToolApplied {
                    call_id: CallId::parse("missing").unwrap(),
                    result_ref: None,
                    result_text: Some("ok".into()),
                    workspace_rev: SnapshotRev::new(1),
                }
            ),
            Reject::UnknownCall
        );
    }

    #[test]
    fn reducer_user_while_unsealed_rejected() {
        let turn = TurnId::parse("t1").unwrap();
        assert_eq!(
            reject_of(&[user("hi"), delta(&turn, "partial")], user("again")),
            Reject::UnexpectedTurn
        );
    }

    #[test]
    fn reducer_second_turn_while_unsealed_rejected() {
        let t1 = TurnId::parse("t1").unwrap();
        let t2 = TurnId::parse("t2").unwrap();
        assert_eq!(
            reject_of(&[user("hi"), delta(&t1, "a")], delta(&t2, "b")),
            Reject::UnexpectedTurn
        );
    }

    #[test]
    fn reducer_tool_pending_while_unsealed_rejected() {
        let turn = TurnId::parse("t1").unwrap();
        assert_eq!(
            reject_of(
                &[user("hi"), delta(&turn, "a")],
                pending("c1", "write", json!({}))
            ),
            Reject::UnexpectedTurn
        );
    }

    #[test]
    fn reducer_legal_assistant_and_idempotent_tool() {
        let turn = TurnId::parse("t1").unwrap();
        let args = json!({"path": "README.md", "contents": "hi"});
        let events = [
            user("summarize"),
            delta(&turn, "Working"),
            Event::AssistantDelta {
                turn: turn.clone(),
                text: Some(" on it".into()),
                tool_call: Some(ToolCallDelta {
                    id: CallId::parse("c1").unwrap(),
                    name: "write".into(),
                    args_delta: "{\"path\"".into(),
                }),
            },
            Event::AssistantSealed {
                turn: turn.clone(),
                finish_reason: FinishReason::ToolCalls,
                model: Some("fake".into()),
                input_tokens: None,
                output_tokens: None,
            },
            Event::WorkspaceSnapshotted {
                rev: SnapshotRev::new(1),
                tree: BlobRef::of_bytes(b"tree"),
            },
            pending("c1", "write", args.clone()),
            Event::ToolApplied {
                call_id: CallId::parse("c1").unwrap(),
                result_ref: None,
                result_text: Some("ok".into()),
                workspace_rev: SnapshotRev::new(2),
            },
        ];
        let state = fold(&events);
        assert!(state.pending.is_none());
        assert!(state.unsealed.is_none());
        assert!(state
            .applied_by_hash
            .contains_key(&("write".into(), args_hash(&args))));
        match &state.messages[1] {
            Message::Assistant { content, tool_calls, .. } => {
                assert_eq!(content, "Working on it");
                assert_eq!(tool_calls.len(), 1);
                assert_eq!(tool_calls[0].arguments, "{\"path\"");
            }
            other => panic!("expected assistant, got {other:?}"),
        }
    }

    #[test]
    fn reducer_applied_hash_blocks_second_pending() {
        let turn = TurnId::parse("t1").unwrap();
        let args = json!({"path": "a"});
        let prefix = [
            user("hi"),
            delta(&turn, "x"),
            seal(&turn),
            pending("c1", "write", args.clone()),
            Event::ToolApplied {
                call_id: CallId::parse("c1").unwrap(),
                result_ref: None,
                result_text: Some("ok".into()),
                workspace_rev: SnapshotRev::new(1),
            },
        ];
        assert_eq!(
            reject_of(&prefix, pending("c2", "write", args)),
            Reject::DuplicateCall
        );
    }

    #[test]
    fn reducer_session_opened_from_unopened() {
        let state = apply(&SessionState::unopened(), &Event::SessionOpened).unwrap();
        assert!(state.opened);
        assert_eq!(
            apply(&state, &Event::SessionOpened).unwrap_err(),
            Reject::UnexpectedTurn
        );
    }

    #[test]
    fn reducer_outcome_mismatch_on_replayed_applied() {
        let turn = TurnId::parse("t1").unwrap();
        let mut state = fold(&[
            user("hi"),
            delta(&turn, "x"),
            seal(&turn),
            pending("c1", "write", json!({"a": 1})),
            Event::ToolApplied {
                call_id: CallId::parse("c1").unwrap(),
                result_ref: None,
                result_text: Some("ok".into()),
                workspace_rev: SnapshotRev::new(1),
            },
        ]);
        state.pending = None;
        let err = apply(
            &state,
            &Event::ToolApplied {
                call_id: CallId::parse("c1").unwrap(),
                result_ref: None,
                result_text: Some("other".into()),
                workspace_rev: SnapshotRev::new(1),
            },
        )
        .unwrap_err();
        assert_eq!(err, Reject::OutcomeMismatch);
    }
}
