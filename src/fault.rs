use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::Error;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PersistOp {
    OpenLease,
    Append,
    AppendAssistantChunk,
    SealAssistant,
    BeginTool,
    CompleteTool,
    SnapshotStage,
    SnapshotCommit,
    RestoreStage,
    RestoreCommit,
    BlobRename,
    Heartbeat,
}

pub trait Fault: Send + Sync {
    fn before_commit(&self, op: PersistOp) -> Result<(), Error>;
}

pub trait Clock: Send + Sync {
    fn now_ms(&self) -> i64;
}

pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> i64 {
        now_system_ms()
    }
}

pub(crate) fn now_system_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

pub struct ManualClock {
    ms: AtomicI64,
}

impl ManualClock {
    pub fn new(ms: i64) -> Self {
        Self {
            ms: AtomicI64::new(ms),
        }
    }

    pub fn set(&self, ms: i64) {
        self.ms.store(ms, Ordering::SeqCst);
    }

    pub fn advance(&self, delta_ms: i64) {
        self.ms.fetch_add(delta_ms, Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now_ms(&self) -> i64 {
        self.ms.load(Ordering::SeqCst)
    }
}

pub struct CrashAtNth {
    n: usize,
    abort_process: bool,
    count: AtomicUsize,
}

impl CrashAtNth {
    pub fn new(n: usize, abort_process: bool) -> Self {
        Self {
            n,
            abort_process,
            count: AtomicUsize::new(0),
        }
    }

    pub fn count(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }

    pub fn reset(&self) {
        self.count.store(0, Ordering::SeqCst);
    }
}

impl Fault for CrashAtNth {
    fn before_commit(&self, _op: PersistOp) -> Result<(), Error> {
        let i = self.count.fetch_add(1, Ordering::SeqCst) + 1;
        if self.n > 0 && i == self.n {
            if self.abort_process {
                std::process::abort();
            }
            return Err(Error::Injected);
        }
        Ok(())
    }
}

#[derive(Clone, Default)]
pub struct Hooks {
    pub fault: Option<Arc<dyn Fault>>,
    pub clock: Option<Arc<dyn Clock>>,
    pub synchronous_full: bool,
}

impl Hooks {
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn now_ms(&self) -> i64 {
        match &self.clock {
            Some(clock) => clock.now_ms(),
            None => now_system_ms(),
        }
    }

    pub(crate) fn before_commit(&self, op: PersistOp) -> Result<(), Error> {
        if let Some(fault) = &self.fault {
            fault.before_commit(op)?;
        }
        Ok(())
    }
}
