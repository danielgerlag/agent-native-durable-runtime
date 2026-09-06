use std::io;
use std::time::Duration;

use crate::ids::WorkerId;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("lease held by worker {holder} (ttl {ttl:?})")]
    LeaseHeld { holder: WorkerId, ttl: Duration },
    #[error("fenced: lease generation advanced")]
    Fenced,
    #[error("invalid state: {0}")]
    InvalidState(String),
    #[error("corrupt store: {0}")]
    Corrupt(String),
    #[error("injected fault")]
    Injected,
    #[error("store: {0}")]
    Store(String),
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("bundle: {0}")]
    Bundle(String),
}

impl Error {
    pub(crate) fn store(err: impl ToString) -> Self {
        Error::Store(err.to_string())
    }

    pub(crate) fn corrupt(err: impl ToString) -> Self {
        Error::Corrupt(err.to_string())
    }

    pub(crate) fn invalid(err: impl ToString) -> Self {
        Error::InvalidState(err.to_string())
    }

    pub(crate) fn bundle(err: impl ToString) -> Self {
        Error::Bundle(err.to_string())
    }
}
