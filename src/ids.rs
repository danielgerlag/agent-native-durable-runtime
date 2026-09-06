use std::fmt;
use std::sync::OnceLock;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::Error;

fn valid_token(s: &str) -> bool {
    !s.is_empty()
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SessionId(String);

impl SessionId {
    pub fn parse(s: &str) -> Result<Self, Error> {
        if !valid_token(s) {
            return Err(Error::invalid("invalid session id"));
        }
        Ok(Self(s.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SessionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WorkerId(String);

impl WorkerId {
    pub fn this_process() -> Self {
        static ID: OnceLock<WorkerId> = OnceLock::new();
        ID.get_or_init(|| {
            WorkerId(format!(
                "pid-{}-{}",
                std::process::id(),
                uuid::Uuid::new_v4().simple()
            ))
        })
        .clone()
    }

    pub fn parse(s: &str) -> Result<Self, Error> {
        if s.is_empty() {
            return Err(Error::invalid("invalid worker id"));
        }
        Ok(Self(s.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for WorkerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ResumeToken {
    session: SessionId,
    seq: u64,
}

impl ResumeToken {
    pub(crate) fn new(session: SessionId, seq: u64) -> Self {
        Self { session, seq }
    }

    pub fn encode(&self) -> String {
        format!("{}:{}", self.session.0, self.seq)
    }

    pub fn decode(s: &str) -> Result<Self, Error> {
        let (sess, seq) = s
            .rsplit_once(':')
            .ok_or_else(|| Error::invalid("malformed resume token"))?;
        let seq: u64 = seq
            .parse()
            .map_err(|_| Error::invalid("malformed resume token"))?;
        Ok(Self {
            session: SessionId::parse(sess)?,
            seq,
        })
    }

    pub fn session(&self) -> &SessionId {
        &self.session
    }

    pub(crate) fn seq(&self) -> u64 {
        self.seq
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EventSeq(u64);

impl EventSeq {
    pub(crate) fn new(v: u64) -> Self {
        Self(v)
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for EventSeq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CallId(String);

impl CallId {
    pub fn new() -> Self {
        Self(format!("call_{}", uuid::Uuid::new_v4().simple()))
    }

    pub fn parse(s: &str) -> Result<Self, Error> {
        if s.is_empty() {
            return Err(Error::invalid("invalid call id"));
        }
        Ok(Self(s.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for CallId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for CallId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ArgsHash(String);

impl ArgsHash {
    pub(crate) fn from_hex(hex: String) -> Self {
        Self(hex)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ArgsHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct BlobRef(String);

impl BlobRef {
    pub(crate) fn of_bytes(bytes: &[u8]) -> Self {
        Self(sha256_hex(bytes))
    }

    pub(crate) fn from_hex(hex: impl Into<String>) -> Self {
        Self(hex.into())
    }

    pub fn parse(s: &str) -> Result<Self, Error> {
        let hex = s.strip_prefix("sha256:").unwrap_or(s);
        if hex.len() != 64 || !hex.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err(Error::corrupt(format!("invalid blob ref {s}")));
        }
        Ok(Self(hex.to_ascii_lowercase()))
    }

    pub fn as_hex(&self) -> &str {
        &self.0
    }

    pub fn uri(&self) -> String {
        format!("sha256:{}", self.0)
    }
}

impl fmt::Display for BlobRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "sha256:{}", self.0)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OpId(String);

impl OpId {
    pub fn from_static(s: &'static str) -> Self {
        Self(s.to_owned())
    }

    pub fn parse(s: &str) -> Result<Self, Error> {
        if s.is_empty() {
            return Err(Error::invalid("invalid op id"));
        }
        Ok(Self(s.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for OpId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TurnId(String);

impl TurnId {
    pub fn new() -> Self {
        Self(format!("turn_{}", uuid::Uuid::new_v4().simple()))
    }

    pub fn parse(s: &str) -> Result<Self, Error> {
        if s.is_empty() {
            return Err(Error::invalid("invalid turn id"));
        }
        Ok(Self(s.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for TurnId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for TurnId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SnapshotRev(u64);

impl SnapshotRev {
    pub(crate) fn new(v: u64) -> Self {
        Self(v)
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for SnapshotRev {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(bytes);
    hex::encode(h.finalize())
}
