use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::Value;

use crate::bundle;
use crate::error::Error;
use crate::event::{AssistantDelta, Event, Input, LoggedEvent, Message};
use crate::fault::{Hooks, PersistOp};
use crate::ids::{BlobRef, CallId, OpId, ResumeToken, SessionId, SnapshotRev, WorkerId};
use crate::lease::LeaseState;
use crate::reducer::{apply, SessionState};
use crate::store::Store;
use crate::tool::{
    AppliedTool, FinishReason, Recovery, ToolCtx, ToolDisposition, ToolPolicy, ToolResult, ToolRun,
    ToolSpec,
};
use crate::workspace::{self, Filter};

#[derive(Clone, Debug)]
pub struct OpenOptions {
    pub store_dir: PathBuf,
    pub session_id: SessionId,
    pub worker: WorkerId,
    pub workspace: PathBuf,
    pub ttl: Duration,
    pub filter: Filter,
}

pub struct Session {
    store: Store,
    state: SessionState,
    log: Vec<LoggedEvent>,
    opts: OpenOptions,
    recovery: Recovery,
    poisoned: bool,
}

impl Session {
    pub fn open(opts: OpenOptions) -> Result<Self, Error> {
        Self::open_with_hooks(opts, Hooks::default())
    }

    pub fn open_with_hooks(opts: OpenOptions, hooks: Hooks) -> Result<Self, Error> {
        std::fs::create_dir_all(&opts.workspace)?;
        std::fs::create_dir_all(&opts.store_dir)?;
        let mut store = Store::open(
            &opts.store_dir,
            &opts.session_id,
            &opts.worker,
            opts.ttl,
            &opts.workspace,
            hooks,
        )?;
        store.reconcile_restore(&opts.workspace)?;
        let log = store.load_events()?;
        let state = fold_log(&log)?;
        store.rebuild_projections(&log, &state)?;

        let mut session = Self {
            store,
            state,
            log,
            opts,
            recovery: Recovery::Clean,
            poisoned: false,
        };
        if session.state.unsealed.is_some() {
            let prefix = session
                .state
                .unsealed
                .as_ref()
                .map(|u| u.text.clone())
                .unwrap_or_default();
            let model = session
                .state
                .unsealed
                .as_ref()
                .and_then(|u| u.model.clone());
            session.seal_assistant(FinishReason::TruncatedCrash, model.clone())?;
            session.recovery = Recovery::Truncated { prefix, model };
        } else {
            session.recovery = recovery_of(&session.state);
        }
        Ok(session)
    }

    pub fn id(&self) -> &SessionId {
        &self.opts.session_id
    }

    pub fn worker(&self) -> &WorkerId {
        &self.opts.worker
    }

    pub fn workspace_path(&self) -> &Path {
        &self.opts.workspace
    }

    pub fn resume_token(&self) -> ResumeToken {
        ResumeToken::new(self.opts.session_id.clone(), self.state.last_seq)
    }

    pub fn recovery(&self) -> Recovery {
        self.recovery.clone()
    }

    pub fn messages(&self) -> &[Message] {
        &self.state.messages
    }

    pub fn events(&self) -> &[LoggedEvent] {
        &self.log
    }

    pub fn replay(&self, token: &ResumeToken) -> Result<Vec<LoggedEvent>, Error> {
        replay_from(&self.opts.session_id, &self.log, token)
    }

    pub fn append(&mut self, op: OpId, input: Input) -> Result<(), Error> {
        self.check()?;
        let event = match input {
            Input::User { content } => Event::User {
                content,
                op: op.clone(),
            },
            Input::System { content } => Event::System {
                content,
                op: op.clone(),
            },
        };
        let (logged, new_state) = self.catch_fence(|s| {
            s.store.commit_events(
                &s.state,
                &[event],
                PersistOp::Append,
                &[],
                Some(&op),
                |_, _| Ok(()),
            )
        })?;
        if logged.is_empty() {
            return Ok(());
        }
        self.install(logged, new_state);
        Ok(())
    }

    pub fn append_assistant_chunk(&mut self, delta: AssistantDelta) -> Result<ResumeToken, Error> {
        self.check()?;
        let event = Event::AssistantDelta {
            turn: delta.turn,
            text: delta.text,
            tool_call: delta.tool_call,
        };
        let (logged, new_state) = self.catch_fence(|s| {
            s.store.commit_events(
                &s.state,
                &[event],
                PersistOp::AppendAssistantChunk,
                &[],
                None,
                |_, _| Ok(()),
            )
        })?;
        self.install(logged, new_state);
        Ok(self.resume_token())
    }

    pub fn seal_assistant(
        &mut self,
        reason: FinishReason,
        model: Option<String>,
    ) -> Result<(), Error> {
        self.check()?;
        let turn = self
            .state
            .unsealed
            .as_ref()
            .ok_or_else(|| Error::invalid("no unsealed assistant turn"))?
            .turn
            .clone();
        let event = Event::AssistantSealed {
            turn,
            finish_reason: reason,
            model,
            input_tokens: None,
            output_tokens: None,
        };
        let (logged, new_state) = self.catch_fence(|s| {
            s.store.commit_events(
                &s.state,
                &[event],
                PersistOp::SealAssistant,
                &[],
                None,
                |_, _| Ok(()),
            )
        })?;
        self.install(logged, new_state);
        Ok(())
    }

    pub fn begin_tool(&mut self, spec: ToolSpec, args: Value) -> Result<ToolDisposition, Error> {
        self.check()?;
        let hash = crate::tool::args_hash(&args);
        if let Some(applied) = self
            .state
            .applied_by_hash
            .get(&(spec.name.clone(), hash.clone()))
        {
            return Ok(ToolDisposition::AlreadyApplied(applied.clone()));
        }
        if let Some(pending) = self.state.pending.clone() {
            if pending.name == spec.name && pending.args_hash == hash {
                return match pending.policy {
                    ToolPolicy::AtMostOnce => Ok(ToolDisposition::Inspect(pending)),
                    ToolPolicy::Idempotent => {
                        self.restore_workspace(pending.workspace_rev)?;
                        Ok(ToolDisposition::Run)
                    }
                };
            }
            return Err(Error::invalid("a tool is already pending"));
        }
        let rev = self.snapshot_workspace()?;
        let call_id = CallId::new();
        let event = Event::ToolPending {
            call_id,
            name: spec.name,
            args_hash: hash,
            args,
            policy: spec.policy,
            workspace_rev: rev,
        };
        let (logged, new_state) = self.catch_fence(|s| {
            s.store.commit_events(
                &s.state,
                &[event],
                PersistOp::BeginTool,
                &[],
                None,
                |_, _| Ok(()),
            )
        })?;
        self.install(logged, new_state);
        self.recovery = recovery_of(&self.state);
        Ok(ToolDisposition::Run)
    }

    pub fn complete_tool(
        &mut self,
        call_id: &CallId,
        result: ToolResult,
    ) -> Result<AppliedTool, Error> {
        self.check()?;
        let pending = self
            .state
            .pending
            .as_ref()
            .ok_or_else(|| Error::invalid("no pending tool"))?;
        if pending.call_id != *call_id {
            return Err(Error::invalid(
                "complete_tool call id does not match pending",
            ));
        }
        let result_ref = match &result.bytes {
            Some(bytes) => Some(self.store.put_blob(bytes)?),
            None => None,
        };
        let result_text = result.text.clone();
        let (rev, tree) = self.capture_tree()?;
        let events = [
            Event::WorkspaceSnapshotted {
                rev,
                tree: tree.clone(),
            },
            Event::ToolApplied {
                call_id: call_id.clone(),
                result_ref,
                result_text,
                workspace_rev: rev,
            },
        ];
        let (logged, new_state) = self.catch_fence(|s| {
            s.store.commit_events(
                &s.state,
                &events,
                PersistOp::CompleteTool,
                &[PersistOp::SnapshotCommit],
                None,
                |_, _| Ok(()),
            )
        })?;
        self.install(logged, new_state);
        self.recovery = recovery_of(&self.state);
        self.state
            .applied_by_hash
            .values()
            .find(|a| a.call_id == *call_id)
            .cloned()
            .ok_or_else(|| Error::invalid("applied tool missing after complete"))
    }

    pub fn fail_tool(&mut self, call_id: &CallId, error: impl Into<String>) -> Result<(), Error> {
        self.check()?;
        let event = Event::ToolFailed {
            call_id: call_id.clone(),
            error: error.into(),
        };
        let (logged, new_state) = self.catch_fence(|s| {
            s.store.commit_events(
                &s.state,
                &[event],
                PersistOp::CompleteTool,
                &[],
                None,
                |_, _| Ok(()),
            )
        })?;
        self.install(logged, new_state);
        self.recovery = recovery_of(&self.state);
        Ok(())
    }

    pub fn abandon_tool(
        &mut self,
        call_id: &CallId,
        reason: impl Into<String>,
    ) -> Result<(), Error> {
        self.check()?;
        let event = Event::ToolAbandoned {
            call_id: call_id.clone(),
            reason: reason.into(),
        };
        let (logged, new_state) = self.catch_fence(|s| {
            s.store.commit_events(
                &s.state,
                &[event],
                PersistOp::CompleteTool,
                &[],
                None,
                |_, _| Ok(()),
            )
        })?;
        self.install(logged, new_state);
        self.recovery = recovery_of(&self.state);
        Ok(())
    }

    pub fn run_tool<F>(&mut self, spec: ToolSpec, args: Value, f: F) -> Result<ToolRun, Error>
    where
        F: FnOnce(&ToolCtx<'_>) -> Result<ToolResult, String>,
    {
        match self.begin_tool(spec, args)? {
            ToolDisposition::AlreadyApplied(a) => Ok(ToolRun::AlreadyApplied(a)),
            ToolDisposition::Inspect(p) => Ok(ToolRun::Inspect(p)),
            ToolDisposition::Run => {
                let pending = self
                    .state
                    .pending
                    .clone()
                    .ok_or_else(|| Error::invalid("pending missing after Run"))?;
                let ctx = ToolCtx::new(&self.opts.workspace, &pending.call_id);
                match f(&ctx) {
                    Ok(result) => {
                        let applied = self.complete_tool(&pending.call_id, result)?;
                        Ok(ToolRun::Completed(applied))
                    }
                    Err(err) => {
                        self.fail_tool(&pending.call_id, err.clone())?;
                        Err(Error::invalid(err))
                    }
                }
            }
        }
    }

    pub fn snapshot_workspace(&mut self) -> Result<SnapshotRev, Error> {
        self.check()?;
        self.store.before_commit(PersistOp::SnapshotStage)?;
        let (rev, tree) = self.capture_tree()?;
        let event = Event::WorkspaceSnapshotted { rev, tree };
        let (logged, new_state) = self.catch_fence(|s| {
            s.store.commit_events(
                &s.state,
                &[event],
                PersistOp::SnapshotCommit,
                &[],
                None,
                |_, _| Ok(()),
            )
        })?;
        self.install(logged, new_state);
        Ok(rev)
    }

    pub fn restore_workspace(&mut self, rev: SnapshotRev) -> Result<(), Error> {
        self.check()?;
        self.catch_fence(|s| s.store.restore_to(&s.opts.workspace, rev))?;
        Ok(())
    }

    pub fn heartbeat(&mut self) -> Result<(), Error> {
        self.check()?;
        self.catch_fence(|s| s.store.heartbeat())
    }

    pub fn export_bundle(&self, dest: impl AsRef<Path>) -> Result<(), Error> {
        bundle::export(
            dest.as_ref(),
            &self.opts.session_id,
            self.store.created_at_ms()?,
            self.store.now_ms(),
            &self.log,
            self.state.workspace_head.clone(),
            self.store.blob_dir(),
            &self.opts.workspace,
            &self.opts.filter,
            self.store.dir(),
        )
    }

    pub fn close(&mut self) -> Result<(), Error> {
        self.check()?;
        if self.state.unsealed.is_some() {
            self.seal_assistant(FinishReason::TruncatedCrash, None)?;
        }
        if !self.state.closed {
            let (logged, new_state) = self.catch_fence(|s| {
                s.store.commit_events(
                    &s.state,
                    &[Event::SessionClosed],
                    PersistOp::Append,
                    &[],
                    None,
                    |_, _| Ok(()),
                )
            })?;
            self.install(logged, new_state);
        }
        self.store.release_lease()?;
        Ok(())
    }

    fn capture_tree(&mut self) -> Result<(SnapshotRev, BlobRef), Error> {
        let store_dir = self.store.dir().to_path_buf();
        let (tree_ref, _) = workspace::capture(
            &self.opts.workspace,
            &self.opts.filter,
            &store_dir,
            |bytes| self.store.put_blob(bytes),
        )?;
        Ok((self.state.next_rev(), tree_ref))
    }

    fn install(&mut self, logged: Vec<LoggedEvent>, new_state: SessionState) {
        self.log.extend(logged);
        self.state = new_state;
        if self.state.pending.is_none()
            && self.state.unsealed.is_none()
            && !matches!(self.recovery, Recovery::Truncated { .. })
        {
            self.recovery = recovery_of(&self.state);
        }
    }

    fn check(&self) -> Result<(), Error> {
        if self.poisoned {
            Err(Error::Fenced)
        } else {
            Ok(())
        }
    }

    fn catch_fence<T>(
        &mut self,
        f: impl FnOnce(&mut Self) -> Result<T, Error>,
    ) -> Result<T, Error> {
        match f(self) {
            Err(Error::Fenced) => {
                self.poisoned = true;
                Err(Error::Fenced)
            }
            other => other,
        }
    }
}

pub struct SessionView {
    store: Store,
    session_id: SessionId,
}

impl SessionView {
    pub fn open(store_dir: impl AsRef<Path>, session_id: SessionId) -> Result<Self, Error> {
        let store = Store::open_readonly(store_dir.as_ref(), &session_id)?;
        Ok(Self { store, session_id })
    }

    pub fn messages(&self) -> Result<Vec<Message>, Error> {
        Ok(self.fold()?.messages)
    }

    pub fn events(&self) -> Result<Vec<LoggedEvent>, Error> {
        self.store.load_events()
    }

    pub fn replay(&self, token: &ResumeToken) -> Result<Vec<LoggedEvent>, Error> {
        let log = self.store.load_events()?;
        replay_from(&self.session_id, &log, token)
    }

    pub fn resume_token(&self) -> Result<ResumeToken, Error> {
        let log = self.store.load_events()?;
        let seq = log.last().map(|e| e.seq.get()).unwrap_or(0);
        Ok(ResumeToken::new(self.session_id.clone(), seq))
    }

    pub fn recovery(&self) -> Result<Recovery, Error> {
        Ok(recovery_of(&self.fold()?))
    }

    pub fn lease_state(&self) -> Result<LeaseState, Error> {
        self.store.lease_state()
    }

    pub fn export_bundle(&self, dest: impl AsRef<Path>) -> Result<(), Error> {
        let log = self.store.load_events()?;
        let state = fold_log(&log)?;
        bundle::export(
            dest.as_ref(),
            &self.session_id,
            self.store.created_at_ms()?,
            self.store.now_ms(),
            &log,
            state.workspace_head,
            self.store.blob_dir(),
            Path::new(""),
            &Filter::default(),
            self.store.dir(),
        )
    }

    fn fold(&self) -> Result<SessionState, Error> {
        fold_log(&self.store.load_events()?)
    }
}

pub fn import_bundle(
    store_dir: impl AsRef<Path>,
    src: impl AsRef<Path>,
) -> Result<SessionId, Error> {
    bundle::import(store_dir.as_ref(), src.as_ref())
}

pub(crate) fn fold_log(events: &[LoggedEvent]) -> Result<SessionState, Error> {
    let mut state = SessionState::origin();
    for e in events {
        state = apply(&state, &e.event)?;
        state.last_seq = e.seq.get();
    }
    Ok(state)
}

fn recovery_of(state: &SessionState) -> Recovery {
    if let Some(u) = &state.unsealed {
        Recovery::Truncated {
            prefix: u.text.clone(),
            model: u.model.clone(),
        }
    } else if let Some(p) = &state.pending {
        Recovery::PendingTool(p.clone())
    } else {
        Recovery::Clean
    }
}

fn replay_from(
    session: &SessionId,
    log: &[LoggedEvent],
    token: &ResumeToken,
) -> Result<Vec<LoggedEvent>, Error> {
    if token.session() != session {
        return Err(Error::invalid("resume token session mismatch"));
    }
    Ok(log
        .iter()
        .filter(|e| e.seq.get() > token.seq())
        .cloned()
        .collect())
}
