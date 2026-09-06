use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rusqlite::{params, Connection, OptionalExtension, Transaction};

use crate::error::Error;
use crate::ids::{SessionId, WorkerId};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LeaseState {
    Vacant,
    Held {
        worker: WorkerId,
        last_heartbeat: SystemTime,
        ttl: Duration,
    },
    Expired {
        last_worker: WorkerId,
        last_heartbeat: SystemTime,
        ttl: Duration,
    },
}

fn ms_to_system_time(ms: i64) -> SystemTime {
    let ms = ms.max(0) as u64;
    UNIX_EPOCH + Duration::from_millis(ms)
}

pub(crate) fn acquire(
    conn: &Connection,
    session_id: &str,
    worker_id: &str,
    now_ms: i64,
    ttl_ms: i64,
) -> Result<u64, Error> {
    conn.execute(
        "INSERT INTO leases (session_id, worker_id, generation, heartbeat_ms, ttl_ms)
         VALUES (?1, ?2, 1, ?3, ?4)
         ON CONFLICT(session_id) DO UPDATE SET
            worker_id = excluded.worker_id,
            generation = leases.generation + 1,
            heartbeat_ms = excluded.heartbeat_ms,
            ttl_ms = excluded.ttl_ms
         WHERE leases.worker_id = excluded.worker_id
            OR (?3 - leases.heartbeat_ms) >= leases.ttl_ms",
        params![session_id, worker_id, now_ms, ttl_ms],
    )
    .map_err(Error::store)?;

    if conn.changes() != 1 {
        let holder = read_state(conn, session_id, now_ms)?;
        let (worker, ttl) = match holder {
            LeaseState::Held { worker, ttl, .. } | LeaseState::Expired { last_worker: worker, ttl, .. } => {
                (worker, ttl)
            }
            LeaseState::Vacant => {
                return Err(Error::store("lease acquire failed without a holder"));
            }
        };
        return Err(Error::LeaseHeld { holder: worker, ttl });
    }

    let generation: i64 = conn
        .query_row(
            "SELECT generation FROM leases WHERE session_id = ?1",
            params![session_id],
            |row| row.get(0),
        )
        .map_err(Error::store)?;
    Ok(generation as u64)
}

pub(crate) fn fence(
    tx: &Transaction<'_>,
    session_id: &str,
    worker_id: &str,
    generation: u64,
    now_ms: i64,
) -> Result<(), Error> {
    tx.execute(
        "UPDATE leases
         SET heartbeat_ms = ?1
         WHERE session_id = ?2 AND worker_id = ?3 AND generation = ?4",
        params![now_ms, session_id, worker_id, generation as i64],
    )
    .map_err(Error::store)?;
    if tx.changes() == 0 {
        return Err(Error::Fenced);
    }
    Ok(())
}

pub(crate) fn release(
    conn: &Connection,
    session_id: &str,
    worker_id: &str,
    generation: u64,
) -> Result<(), Error> {
    conn.execute(
        "DELETE FROM leases WHERE session_id = ?1 AND worker_id = ?2 AND generation = ?3",
        params![session_id, worker_id, generation as i64],
    )
    .map_err(Error::store)?;
    Ok(())
}

pub(crate) fn read_state(conn: &Connection, session_id: &str, now_ms: i64) -> Result<LeaseState, Error> {
    let row: Option<(String, i64, i64)> = conn
        .query_row(
            "SELECT worker_id, heartbeat_ms, ttl_ms FROM leases WHERE session_id = ?1",
            params![session_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(Error::store)?;
    match row {
        None => Ok(LeaseState::Vacant),
        Some((worker, heartbeat_ms, ttl_ms)) => {
            let worker = WorkerId::parse(&worker)?;
            let last_heartbeat = ms_to_system_time(heartbeat_ms);
            let ttl = Duration::from_millis(ttl_ms.max(0) as u64);
            if now_ms.saturating_sub(heartbeat_ms) >= ttl_ms {
                Ok(LeaseState::Expired {
                    last_worker: worker,
                    last_heartbeat,
                    ttl,
                })
            } else {
                Ok(LeaseState::Held {
                    worker,
                    last_heartbeat,
                    ttl,
                })
            }
        }
    }
}

pub(crate) fn heartbeat(
    conn: &Connection,
    session_id: &str,
    worker_id: &str,
    generation: u64,
    now_ms: i64,
) -> Result<(), Error> {
    conn.execute(
        "UPDATE leases
         SET heartbeat_ms = ?1
         WHERE session_id = ?2 AND worker_id = ?3 AND generation = ?4",
        params![now_ms, session_id, worker_id, generation as i64],
    )
    .map_err(Error::store)?;
    if conn.changes() == 0 {
        return Err(Error::Fenced);
    }
    Ok(())
}

#[allow(dead_code)]
pub(crate) fn session_exists(conn: &Connection, session_id: &SessionId) -> Result<bool, Error> {
    let n: i64 = conn
        .query_row(
            "SELECT COUNT(1) FROM sessions WHERE id = ?1",
            params![session_id.as_str()],
            |row| row.get(0),
        )
        .map_err(Error::store)?;
    Ok(n > 0)
}
