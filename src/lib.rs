//! Crash-safe agent sessions: transcript, workspace snapshot, and tool ledger.

mod bundle;
mod error;
mod event;
mod fault;
mod ids;
mod lease;
mod reducer;
mod session;
mod store;
mod tool;
mod workspace;

pub mod semconv;

pub use error::Error;

pub type Result<T> = std::result::Result<T, Error>;
pub use event::{AssistantDelta, Event, Input, LoggedEvent, Message, ToolCall, ToolCallDelta};
pub use fault::{Clock, CrashAtNth, Fault, Hooks, ManualClock, PersistOp, SystemClock};
pub use ids::{
    ArgsHash, BlobRef, CallId, EventSeq, OpId, ResumeToken, SessionId, SnapshotRev, TurnId,
    WorkerId,
};
pub use lease::LeaseState;
pub use session::{import_bundle, OpenOptions, Session, SessionView};
pub use tool::{
    AppliedTool, FinishReason, PendingTool, Recovery, SideEffectStatus, ToolCtx, ToolDisposition,
    ToolPolicy, ToolResult, ToolRun, ToolSpec,
};
pub use workspace::Filter;
pub use workspace::Filter as WorkspaceFilter;
