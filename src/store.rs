use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};

use crate::error::Error;
use crate::event::{Event, LoggedEvent};
use crate::fault::{Hooks, PersistOp};
use crate::ids::{BlobRef, EventSeq, OpId, SessionId, SnapshotRev, WorkerId};
use crate::lease::{self, LeaseState};
use crate::reducer::{apply, SessionState};
use crate::tool::{SideEffectStatus, ToolPolicy};
use crate::workspace::{self, RestoreJournal, RestorePhase, Tree};

const SCHEMA_VERSION: i64 = 1;

struct WriteCtx {
    session_id: String,
    worker: String,
    generation: u64,
    hooks: Hooks,
    now_ms: i64,
}

impl WriteCtx {
    fn fence(&self, tx: &Transaction<'_>) -> Result<(), Error> {
        if self.generation == 0 {
            return Ok(());
        }
        lease::fence(
            tx,
            &self.session_id,
            &self.worker,
            self.generation,
            self.now_ms,
        )
    }
}

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS meta (
    schema_version INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS sessions (
    id TEXT PRIMARY KEY,
    created_at INTEGER NOT NULL,
    workspace_path TEXT NOT NULL,
    workspace_head_rev INTEGER,
    closed_at INTEGER
);
CREATE TABLE IF NOT EXISTS leases (
    session_id TEXT PRIMARY KEY,
    worker_id TEXT NOT NULL,
    generation INTEGER NOT NULL,
    heartbeat_ms INTEGER NOT NULL,
    ttl_ms INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
    session_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    t_ms INTEGER NOT NULL,
    write_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    body_json TEXT NOT NULL,
    blob_ref TEXT,
    PRIMARY KEY (session_id, seq),
    UNIQUE (session_id, write_id)
);
CREATE TABLE IF NOT EXISTS ops (
    session_id TEXT NOT NULL,
    op_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    PRIMARY KEY (session_id, op_id)
);
CREATE TABLE IF NOT EXISTS tool_calls (
    session_id TEXT NOT NULL,
    call_id TEXT NOT NULL,
    name TEXT NOT NULL,
    args_hash TEXT NOT NULL,
    args_json TEXT NOT NULL,
    policy TEXT NOT NULL,
    status TEXT NOT NULL,
    result_text TEXT,
    result_ref TEXT,
    workspace_rev INTEGER,
    error TEXT,
    PRIMARY KEY (session_id, call_id)
);
CREATE TABLE IF NOT EXISTS snapshots (
    session_id TEXT NOT NULL,
    rev INTEGER NOT NULL,
    tree_blob TEXT NOT NULL,
    event_seq INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (session_id, rev)
);
CREATE TABLE IF NOT EXISTS restore_journal (
    session_id TEXT PRIMARY KEY,
    rev INTEGER NOT NULL,
    phase TEXT NOT NULL,
    scratch_path TEXT,
    bak_path TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS tool_calls_idempotent_applied
    ON tool_calls(session_id, name, args_hash)
    WHERE status = 'applied' AND policy = 'idempotent';
"#;

pub(crate) struct Store {
    conn: Connection,
    dir: PathBuf,
    blob_dir: PathBuf,
    hooks: Hooks,
    generation: u64,
    session_id: String,
    worker: String,
    ttl_ms: i64,
}

impl Store {
    pub(crate) fn open(
        store_dir: &Path,
        session_id: &SessionId,
        worker: &WorkerId,
        ttl: Duration,
        workspace: &Path,
        hooks: Hooks,
    ) -> Result<Self, Error> {
        fs::create_dir_all(store_dir)?;
        let blob_dir = store_dir.join("blobs");
        fs::create_dir_all(&blob_dir)?;
        let db_path = store_dir.join("durable.sqlite");
        let conn = Connection::open(&db_path).map_err(Error::store)?;
        conn.busy_timeout(Duration::from_millis(5000))
            .map_err(Error::store)?;
        conn.pragma_update(None, "journal_mode", "WAL")
            .map_err(Error::store)?;
        conn.pragma_update(None, "foreign_keys", "ON")
            .map_err(Error::store)?;
        if hooks.synchronous_full {
            conn.pragma_update(None, "synchronous", "FULL")
                .map_err(Error::store)?;
        }
        conn.execute_batch(SCHEMA).map_err(Error::store)?;
        ensure_meta(&conn)?;

        let mut store = Self {
            conn,
            dir: store_dir.to_path_buf(),
            blob_dir,
            hooks,
            generation: 0,
            session_id: session_id.as_str().to_owned(),
            worker: worker.as_str().to_owned(),
            ttl_ms: ttl.as_millis() as i64,
        };

        let now = store.now_ms();
        let tx = store
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(Error::store)?;
        tx.execute(
            "INSERT INTO sessions (id, created_at, workspace_path, workspace_head_rev, closed_at)
             VALUES (?1, ?2, ?3, NULL, NULL)
             ON CONFLICT(id) DO UPDATE SET workspace_path = excluded.workspace_path",
            params![session_id.as_str(), now, workspace.to_string_lossy().as_ref()],
        )
        .map_err(Error::store)?;
        tx.commit().map_err(Error::store)?;

        store.hooks.before_commit(PersistOp::OpenLease)?;
        store.generation = lease::acquire(
            &store.conn,
            &store.session_id,
            &store.worker,
            store.now_ms(),
            store.ttl_ms,
        )?;
        Ok(store)
    }

    pub(crate) fn open_readonly(store_dir: &Path, session_id: &SessionId) -> Result<Self, Error> {
        let db_path = store_dir.join("durable.sqlite");
        let conn = Connection::open(&db_path).map_err(Error::store)?;
        conn.busy_timeout(Duration::from_millis(5000))
            .map_err(Error::store)?;
        conn.pragma_update(None, "query_only", true)
            .map_err(Error::store)?;
        Ok(Self {
            conn,
            dir: store_dir.to_path_buf(),
            blob_dir: store_dir.join("blobs"),
            hooks: Hooks::default(),
            generation: 0,
            session_id: session_id.as_str().to_owned(),
            worker: String::new(),
            ttl_ms: 0,
        })
    }

    pub(crate) fn open_import(store_dir: &Path) -> Result<Self, Error> {
        fs::create_dir_all(store_dir)?;
        let blob_dir = store_dir.join("blobs");
        fs::create_dir_all(&blob_dir)?;
        let conn = Connection::open(store_dir.join("durable.sqlite")).map_err(Error::store)?;
        conn.busy_timeout(Duration::from_millis(5000))
            .map_err(Error::store)?;
        conn.pragma_update(None, "journal_mode", "WAL")
            .map_err(Error::store)?;
        conn.pragma_update(None, "foreign_keys", "ON")
            .map_err(Error::store)?;
        conn.execute_batch(SCHEMA).map_err(Error::store)?;
        ensure_meta(&conn)?;
        Ok(Self {
            conn,
            dir: store_dir.to_path_buf(),
            blob_dir,
            hooks: Hooks::default(),
            generation: 0,
            session_id: String::new(),
            worker: String::new(),
            ttl_ms: 0,
        })
    }

    pub(crate) fn dir(&self) -> &Path {
        &self.dir
    }

    pub(crate) fn blob_dir(&self) -> &Path {
        &self.blob_dir
    }

    pub(crate) fn session_id_str(&self) -> &str {
        &self.session_id
    }

    pub(crate) fn now_ms(&self) -> i64 {
        self.hooks.now_ms()
    }

    pub(crate) fn before_commit(&self, op: PersistOp) -> Result<(), Error> {
        self.hooks.before_commit(op)
    }

    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    pub(crate) fn created_at_ms(&self) -> Result<i64, Error> {
        self.conn
            .query_row(
                "SELECT created_at FROM sessions WHERE id = ?1",
                params![self.session_id],
                |row| row.get(0),
            )
            .map_err(Error::store)
    }

    pub(crate) fn closed_at_ms(&self) -> Result<Option<i64>, Error> {
        self.conn
            .query_row(
                "SELECT closed_at FROM sessions WHERE id = ?1",
                params![self.session_id],
                |row| row.get(0),
            )
            .map_err(Error::store)
    }

    pub(crate) fn lease_state(&self) -> Result<LeaseState, Error> {
        lease::read_state(&self.conn, &self.session_id, self.now_ms())
    }

    pub(crate) fn release_lease(&self) -> Result<(), Error> {
        lease::release(&self.conn, &self.session_id, &self.worker, self.generation)
    }

    pub(crate) fn heartbeat(&self) -> Result<(), Error> {
        self.before_commit(PersistOp::Heartbeat)?;
        lease::heartbeat(
            &self.conn,
            &self.session_id,
            &self.worker,
            self.generation,
            self.now_ms(),
        )
    }

    pub(crate) fn put_blob(&self, bytes: &[u8]) -> Result<BlobRef, Error> {
        let r = BlobRef::of_bytes(bytes);
        let hex = r.as_hex();
        let dir = self.blob_dir.join(&hex[..2]);
        fs::create_dir_all(&dir)?;
        let dest = dir.join(hex);
        if dest.exists() {
            return Ok(r);
        }
        let part = dir.join(format!("{hex}.part"));
        {
            let mut f = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&part)?;
            f.write_all(bytes)?;
            f.sync_all()?;
        }
        self.before_commit(PersistOp::BlobRename)?;
        fs::rename(&part, &dest)?;
        if let Ok(dirf) = File::open(&dir) {
            let _ = dirf.sync_all();
        }
        Ok(r)
    }

    pub(crate) fn get_blob(&self, r: &BlobRef) -> Result<Vec<u8>, Error> {
        let hex = r.as_hex();
        let path = self.blob_dir.join(&hex[..2]).join(hex);
        fs::read(&path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                Error::corrupt(format!("missing blob {}", r.uri()))
            } else {
                Error::Io(e)
            }
        })
    }

    pub(crate) fn copy_blob_from(&self, src_root: &Path, r: &BlobRef) -> Result<(), Error> {
        let hex = r.as_hex();
        let dest_dir = self.blob_dir.join(&hex[..2]);
        fs::create_dir_all(&dest_dir)?;
        let dest = dest_dir.join(hex);
        if dest.exists() {
            return Ok(());
        }
        let src = src_root.join(&hex[..2]).join(hex);
        if !src.exists() {
            return Err(Error::corrupt(format!("missing bundle blob {}", r.uri())));
        }
        let bytes = fs::read(src)?;
        let got = BlobRef::of_bytes(&bytes);
        if got.as_hex() != hex {
            return Err(Error::corrupt(format!("blob hash mismatch {}", r.uri())));
        }
        self.put_blob(&bytes)?;
        Ok(())
    }

    fn write_ctx(&self) -> WriteCtx {
        WriteCtx {
            session_id: self.session_id.clone(),
            worker: self.worker.clone(),
            generation: self.generation,
            hooks: self.hooks.clone(),
            now_ms: self.hooks.now_ms(),
        }
    }

    pub(crate) fn load_events(&self) -> Result<Vec<LoggedEvent>, Error> {
        load_events_for(&self.conn, &self.session_id)
    }

    pub(crate) fn lookup_op(&self, op: &OpId) -> Result<Option<u64>, Error> {
        self.conn
            .query_row(
                "SELECT seq FROM ops WHERE session_id = ?1 AND op_id = ?2",
                params![self.session_id, op.as_str()],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .map_err(Error::store)
            .map(|v| v.map(|s| s as u64))
    }

    pub(crate) fn commit_events(
        &mut self,
        state: &SessionState,
        events: &[Event],
        op: PersistOp,
        extra_ops: &[PersistOp],
        op_id: Option<&OpId>,
        extra: impl FnOnce(&Transaction<'_>, &[EventSeq]) -> Result<(), Error>,
    ) -> Result<(Vec<LoggedEvent>, SessionState), Error> {
        if events.is_empty() {
            return Ok((Vec::new(), state.clone()));
        }
        if let Some(op_id) = op_id {
            if events.len() == 1 {
                if self.lookup_op(op_id)?.is_some() {
                    return Ok((Vec::new(), state.clone()));
                }
            }
        }

        let ctx = self.write_ctx();
        let t_ms = ctx.now_ms;
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(Error::store)?;
        ctx.fence(&tx)?;

        if let Some(op_id) = op_id {
            if lookup_op_tx(&tx, &ctx.session_id, op_id)?.is_some() {
                drop(tx);
                return Ok((Vec::new(), state.clone()));
            }
        }

        let mut new_state = state.clone();
        let mut logged = Vec::new();
        let mut seqs = Vec::new();
        for event in events {
            new_state = apply(&new_state, event)?;
            let seq = next_seq(&tx, &ctx.session_id)?;
            let write_id = uuid::Uuid::new_v4().to_string();
            insert_event(&tx, &ctx.session_id, seq, t_ms, &write_id, event)?;
            upsert_projections(&tx, &ctx.session_id, seq, t_ms, event, &new_state)?;
            if let Some(op_id) = op_id {
                tx.execute(
                    "INSERT INTO ops (session_id, op_id, seq) VALUES (?1, ?2, ?3)",
                    params![ctx.session_id, op_id.as_str(), seq as i64],
                )
                .map_err(Error::store)?;
            }
            let es = EventSeq::new(seq);
            seqs.push(es);
            new_state.last_seq = seq;
            logged.push(LoggedEvent {
                seq: es,
                t_ms,
                event: event.clone(),
            });
        }
        extra(&tx, &seqs)?;
        for extra_op in extra_ops {
            ctx.hooks.before_commit(*extra_op)?;
        }
        ctx.hooks.before_commit(op)?;
        tx.commit().map_err(Error::store)?;
        Ok((logged, new_state))
    }

    pub(crate) fn set_closed(&self, closed_at: i64) -> Result<(), Error> {
        self.conn
            .execute(
                "UPDATE sessions SET closed_at = ?1 WHERE id = ?2",
                params![closed_at, self.session_id],
            )
            .map_err(Error::store)?;
        Ok(())
    }

    pub(crate) fn journal_get(&self) -> Result<Option<RestoreJournal>, Error> {
        let row: Option<(i64, String, Option<String>, Option<String>)> = self
            .conn
            .query_row(
                "SELECT rev, phase, scratch_path, bak_path FROM restore_journal WHERE session_id = ?1",
                params![self.session_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()
            .map_err(Error::store)?;
        match row {
            None => Ok(None),
            Some((rev, phase, scratch, bak)) => Ok(Some(RestoreJournal {
                rev: SnapshotRev::new(rev as u64),
                phase: RestorePhase::parse(&phase)?,
                scratch_path: PathBuf::from(scratch.unwrap_or_default()),
                bak_path: PathBuf::from(bak.unwrap_or_default()),
            })),
        }
    }

    pub(crate) fn journal_put(&mut self, journal: &RestoreJournal, op: Option<PersistOp>) -> Result<(), Error> {
        let ctx = self.write_ctx();
        let scratch = journal.scratch_path.to_string_lossy().into_owned();
        let bak = journal.bak_path.to_string_lossy().into_owned();
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(Error::store)?;
        ctx.fence(&tx)?;
        tx.execute(
            "INSERT INTO restore_journal (session_id, rev, phase, scratch_path, bak_path)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(session_id) DO UPDATE SET
                rev = excluded.rev,
                phase = excluded.phase,
                scratch_path = excluded.scratch_path,
                bak_path = excluded.bak_path",
            params![
                ctx.session_id,
                journal.rev.get() as i64,
                journal.phase.as_str(),
                scratch,
                bak
            ],
        )
        .map_err(Error::store)?;
        if let Some(op) = op {
            ctx.hooks.before_commit(op)?;
        }
        tx.commit().map_err(Error::store)?;
        Ok(())
    }

    pub(crate) fn journal_clear(&mut self) -> Result<(), Error> {
        let ctx = self.write_ctx();
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(Error::store)?;
        ctx.fence(&tx)?;
        tx.execute(
            "DELETE FROM restore_journal WHERE session_id = ?1",
            params![ctx.session_id],
        )
        .map_err(Error::store)?;
        tx.commit().map_err(Error::store)?;
        Ok(())
    }

    pub(crate) fn snapshot_tree(&self, rev: SnapshotRev) -> Result<BlobRef, Error> {
        let uri: String = self
            .conn
            .query_row(
                "SELECT tree_blob FROM snapshots WHERE session_id = ?1 AND rev = ?2",
                params![self.session_id, rev.get() as i64],
                |row| row.get(0),
            )
            .map_err(|e| match e {
                rusqlite::Error::QueryReturnedNoRows => {
                    Error::invalid(format!("unknown snapshot rev {}", rev.get()))
                }
                other => Error::store(other),
            })?;
        BlobRef::parse(&uri)
    }

    pub(crate) fn load_tree(&self, rev: SnapshotRev) -> Result<Tree, Error> {
        let blob = self.snapshot_tree(rev)?;
        let bytes = self.get_blob(&blob)?;
        workspace::parse_tree(&bytes)
    }

    pub(crate) fn insert_snapshot_row(
        tx: &Transaction<'_>,
        session_id: &str,
        rev: SnapshotRev,
        tree: &BlobRef,
        seq: u64,
        t_ms: i64,
    ) -> Result<(), Error> {
        tx.execute(
            "INSERT OR REPLACE INTO snapshots (session_id, rev, tree_blob, event_seq, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                session_id,
                rev.get() as i64,
                tree.uri(),
                seq as i64,
                t_ms
            ],
        )
        .map_err(Error::store)?;
        tx.execute(
            "UPDATE sessions SET workspace_head_rev = ?1 WHERE id = ?2",
            params![rev.get() as i64, session_id],
        )
        .map_err(Error::store)?;
        Ok(())
    }

    pub(crate) fn reconcile_restore(&mut self, live: &Path) -> Result<(), Error> {
        let Some(journal) = self.journal_get()? else {
            return Ok(());
        };
        match journal.phase {
            RestorePhase::Copying => {
                let tree = self.load_tree(journal.rev)?;
                if journal.scratch_path.exists() {
                    fs::remove_dir_all(&journal.scratch_path)?;
                }
                self.before_commit(PersistOp::RestoreStage)?;
                workspace::materialize(&journal.scratch_path, &tree, |b| self.get_blob(b))?;
                let mut next = journal.clone();
                next.phase = RestorePhase::Swapping;
                self.journal_put(&next, None)?;
                self.before_commit(PersistOp::RestoreCommit)?;
                workspace::swap_live(live, &next.scratch_path, &next.bak_path)?;
                workspace::cleanup_bak(&next.bak_path)?;
                self.journal_clear()?;
            }
            RestorePhase::Swapping => {
                self.before_commit(PersistOp::RestoreCommit)?;
                workspace::swap_live(live, &journal.scratch_path, &journal.bak_path)?;
                workspace::cleanup_bak(&journal.bak_path)?;
                self.journal_clear()?;
            }
            RestorePhase::Done => {
                workspace::cleanup_bak(&journal.bak_path)?;
                self.journal_clear()?;
            }
        }
        Ok(())
    }

    pub(crate) fn restore_to(&mut self, live: &Path, rev: SnapshotRev) -> Result<(), Error> {
        let tree = self.load_tree(rev)?;
        let scratch = workspace::scratch_path(live);
        let bak = workspace::bak_path(live);
        let journal = RestoreJournal {
            rev,
            phase: RestorePhase::Copying,
            scratch_path: scratch.clone(),
            bak_path: bak.clone(),
        };
        self.journal_put(&journal, Some(PersistOp::RestoreStage))?;
        if scratch.exists() {
            fs::remove_dir_all(&scratch)?;
        }
        workspace::materialize(&scratch, &tree, |b| self.get_blob(b))?;
        let mut swapping = journal;
        swapping.phase = RestorePhase::Swapping;
        self.journal_put(&swapping, None)?;
        self.before_commit(PersistOp::RestoreCommit)?;
        workspace::swap_live(live, &scratch, &bak)?;
        workspace::cleanup_bak(&bak)?;
        self.journal_clear()?;
        Ok(())
    }

    pub(crate) fn rebuild_projections(&self, events: &[LoggedEvent], state: &SessionState) -> Result<(), Error> {
        self.conn
            .execute(
                "DELETE FROM tool_calls WHERE session_id = ?1",
                params![self.session_id],
            )
            .map_err(Error::store)?;
        let tx = self.conn.unchecked_transaction().map_err(Error::store)?;
        let mut fold = SessionState::origin();
        for logged in events {
            fold = apply(&fold, &logged.event)?;
            fold.last_seq = logged.seq.get();
            upsert_projections(
                &tx,
                &self.session_id,
                logged.seq.get(),
                logged.t_ms,
                &logged.event,
                &fold,
            )?;
        }
        if let Some((rev, tree)) = &state.workspace_head {
            tx.execute(
                "UPDATE sessions SET workspace_head_rev = ?1 WHERE id = ?2",
                params![rev.get() as i64, self.session_id],
            )
            .map_err(Error::store)?;
            let _ = tree;
        }
        tx.commit().map_err(Error::store)?;
        Ok(())
    }

    pub(crate) fn import_session_row(
        &self,
        session_id: &SessionId,
        created_at: i64,
        workspace_path: &str,
    ) -> Result<(), Error> {
        self.conn
            .execute(
                "INSERT INTO sessions (id, created_at, workspace_path, workspace_head_rev, closed_at)
                 VALUES (?1, ?2, ?3, NULL, NULL)
                 ON CONFLICT(id) DO UPDATE SET created_at = excluded.created_at",
                params![session_id.as_str(), created_at, workspace_path],
            )
            .map_err(Error::store)?;
        Ok(())
    }

    pub(crate) fn import_event(
        &self,
        session_id: &SessionId,
        logged: &LoggedEvent,
    ) -> Result<(), Error> {
        let write_id = uuid::Uuid::new_v4().to_string();
        self.conn
            .execute(
                "INSERT INTO events (session_id, seq, t_ms, write_id, kind, body_json, blob_ref)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    session_id.as_str(),
                    logged.seq.get() as i64,
                    logged.t_ms,
                    write_id,
                    logged.event.kind(),
                    serde_json::to_string(&logged.event).map_err(|e| Error::bundle(e.to_string()))?,
                    event_blob_ref(&logged.event)
                ],
            )
            .map_err(Error::store)?;
        Ok(())
    }

    pub(crate) fn conn(&self) -> &Connection {
        &self.conn
    }

    pub(crate) fn set_session_id(&mut self, id: &SessionId) {
        self.session_id = id.as_str().to_owned();
    }
}

fn ensure_meta(conn: &Connection) -> Result<(), Error> {
    let version: Option<i64> = conn
        .query_row("SELECT schema_version FROM meta LIMIT 1", [], |row| row.get(0))
        .optional()
        .map_err(Error::store)?;
    match version {
        None => {
            conn.execute("INSERT INTO meta (schema_version) VALUES (?1)", params![SCHEMA_VERSION])
                .map_err(Error::store)?;
            Ok(())
        }
        Some(v) if v == SCHEMA_VERSION => Ok(()),
        Some(v) => Err(Error::corrupt(format!("unsupported schema_version {v}"))),
    }
}

fn next_seq(tx: &Transaction<'_>, session_id: &str) -> Result<u64, Error> {
    let max: i64 = tx
        .query_row(
            "SELECT COALESCE(MAX(seq), 0) FROM events WHERE session_id = ?1",
            params![session_id],
            |row| row.get(0),
        )
        .map_err(Error::store)?;
    Ok((max as u64) + 1)
}

fn lookup_op_tx(tx: &Transaction<'_>, session_id: &str, op: &OpId) -> Result<Option<u64>, Error> {
    tx.query_row(
        "SELECT seq FROM ops WHERE session_id = ?1 AND op_id = ?2",
        params![session_id, op.as_str()],
        |row| row.get::<_, i64>(0),
    )
    .optional()
    .map_err(Error::store)
    .map(|v| v.map(|s| s as u64))
}

fn insert_event(
    tx: &Transaction<'_>,
    session_id: &str,
    seq: u64,
    t_ms: i64,
    write_id: &str,
    event: &Event,
) -> Result<(), Error> {
    let body = serde_json::to_string(event).map_err(|e| Error::store(e.to_string()))?;
    tx.execute(
        "INSERT INTO events (session_id, seq, t_ms, write_id, kind, body_json, blob_ref)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            session_id,
            seq as i64,
            t_ms,
            write_id,
            event.kind(),
            body,
            event_blob_ref(event)
        ],
    )
    .map_err(Error::store)?;
    Ok(())
}

fn event_blob_ref(event: &Event) -> Option<String> {
    match event {
        Event::WorkspaceSnapshotted { tree, .. } => Some(tree.uri()),
        Event::ToolApplied {
            result_ref: Some(r),
            ..
        } => Some(r.uri()),
        _ => None,
    }
}

fn upsert_projections(
    tx: &Transaction<'_>,
    session_id: &str,
    seq: u64,
    t_ms: i64,
    event: &Event,
    state: &SessionState,
) -> Result<(), Error> {
    match event {
        Event::WorkspaceSnapshotted { rev, tree } => {
            Store::insert_snapshot_row(tx, session_id, *rev, tree, seq, t_ms)?;
        }
        Event::ToolPending {
            call_id,
            name,
            args_hash,
            args,
            policy,
            workspace_rev,
        } => {
            tx.execute(
                "INSERT INTO tool_calls
                    (session_id, call_id, name, args_hash, args_json, policy, status, workspace_rev)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'pending', ?7)
                 ON CONFLICT(session_id, call_id) DO UPDATE SET
                    status = 'pending',
                    args_json = excluded.args_json,
                    workspace_rev = excluded.workspace_rev",
                params![
                    session_id,
                    call_id.as_str(),
                    name,
                    args_hash.as_str(),
                    serde_json::to_string(args).map_err(|e| Error::store(e.to_string()))?,
                    policy.as_str(),
                    workspace_rev.get() as i64
                ],
            )
            .map_err(Error::store)?;
        }
        Event::ToolApplied {
            call_id,
            result_ref,
            result_text,
            workspace_rev,
        } => {
            tx.execute(
                "UPDATE tool_calls
                 SET status = 'applied', result_text = ?1, result_ref = ?2, workspace_rev = ?3, error = NULL
                 WHERE session_id = ?4 AND call_id = ?5",
                params![
                    result_text,
                    result_ref.as_ref().map(|r| r.uri()),
                    workspace_rev.get() as i64,
                    session_id,
                    call_id.as_str()
                ],
            )
            .map_err(Error::store)?;
        }
        Event::ToolFailed { call_id, error } => {
            tx.execute(
                "UPDATE tool_calls SET status = 'failed', error = ?1 WHERE session_id = ?2 AND call_id = ?3",
                params![error, session_id, call_id.as_str()],
            )
            .map_err(Error::store)?;
        }
        Event::ToolAbandoned { call_id, reason } => {
            tx.execute(
                "UPDATE tool_calls SET status = 'abandoned', error = ?1 WHERE session_id = ?2 AND call_id = ?3",
                params![reason, session_id, call_id.as_str()],
            )
            .map_err(Error::store)?;
        }
        Event::SessionClosed => {
            tx.execute(
                "UPDATE sessions SET closed_at = ?1 WHERE id = ?2",
                params![t_ms, session_id],
            )
            .map_err(Error::store)?;
        }
        _ => {}
    }
    let _ = state;
    let _ = ToolPolicy::Idempotent;
    let _ = SideEffectStatus::Applied;
    Ok(())
}

pub(crate) fn load_events_for(conn: &Connection, session_id: &str) -> Result<Vec<LoggedEvent>, Error> {
    let mut stmt = conn
        .prepare(
            "SELECT seq, t_ms, body_json FROM events WHERE session_id = ?1 ORDER BY seq ASC",
        )
        .map_err(Error::store)?;
    let rows = stmt
        .query_map(params![session_id], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .map_err(Error::store)?;
    let mut out = Vec::new();
    let mut expect = 1u64;
    for row in rows {
        let (seq, t_ms, body) = row.map_err(Error::store)?;
        let seq = seq as u64;
        if seq != expect {
            return Err(Error::corrupt(format!(
                "event seq gap: expected {expect}, got {seq}"
            )));
        }
        expect += 1;
        let event: Event =
            serde_json::from_str(&body).map_err(|e| Error::corrupt(format!("event json: {e}")))?;
        out.push(LoggedEvent {
            seq: EventSeq::new(seq),
            t_ms,
            event,
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::Input;
    use crate::fault::ManualClock;
    use crate::ids::OpId;
    use std::sync::Arc;
    use std::time::Duration;

    fn opts(dir: &Path, worker: &str) -> (SessionId, WorkerId, PathBuf) {
        (
            SessionId::parse("sess_demo").unwrap(),
            WorkerId::parse(worker).unwrap(),
            dir.join("work"),
        )
    }

    #[test]
    fn store_blob_and_event_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let store_dir = tmp.path().join("store");
        let (sid, worker, ws) = opts(tmp.path(), "w1");
        fs::create_dir_all(&ws).unwrap();
        let mut store = Store::open(
            &store_dir,
            &sid,
            &worker,
            Duration::from_secs(60),
            &ws,
            Hooks::default(),
        )
        .unwrap();
        let blob = store.put_blob(b"hello-blob").unwrap();
        assert_eq!(store.get_blob(&blob).unwrap(), b"hello-blob");

        let state = SessionState::origin();
        let event = Event::User {
            content: "hi".into(),
            op: OpId::from_static("req-1"),
        };
        let (logged, state) = store
            .commit_events(
                &state,
                &[event.clone()],
                PersistOp::Append,
                &[],
                Some(&OpId::from_static("req-1")),
                |_, _| Ok(()),
            )
            .unwrap();
        assert_eq!(logged.len(), 1);
        assert_eq!(state.last_seq, 1);
        let (logged2, _) = store
            .commit_events(
                &state,
                &[Event::User {
                    content: "ignored".into(),
                    op: OpId::from_static("req-1"),
                }],
                PersistOp::Append,
                &[],
                Some(&OpId::from_static("req-1")),
                |_, _| Ok(()),
            )
            .unwrap();
        assert!(logged2.is_empty());
        drop(store);

        let store = Store::open(
            &store_dir,
            &sid,
            &worker,
            Duration::from_secs(60),
            &ws,
            Hooks::default(),
        )
        .unwrap();
        let events = store.load_events().unwrap();
        assert_eq!(events.len(), 1);
        match &events[0].event {
            Event::User { content, .. } => assert_eq!(content, "hi"),
            other => panic!("{other:?}"),
        }
        let _ = Input::user("x");
    }

    #[test]
    fn lease_acquire_same_worker_reattach() {
        let tmp = tempfile::tempdir().unwrap();
        let store_dir = tmp.path().join("store");
        let (sid, worker, ws) = opts(tmp.path(), "w1");
        fs::create_dir_all(&ws).unwrap();
        let store = Store::open(
            &store_dir,
            &sid,
            &worker,
            Duration::from_secs(60),
            &ws,
            Hooks::default(),
        )
        .unwrap();
        let gen1 = store.generation();
        drop(store);
        let store = Store::open(
            &store_dir,
            &sid,
            &worker,
            Duration::from_secs(60),
            &ws,
            Hooks::default(),
        )
        .unwrap();
        assert!(store.generation() > gen1);
    }

    #[test]
    fn lease_foreign_worker_held() {
        let tmp = tempfile::tempdir().unwrap();
        let store_dir = tmp.path().join("store");
        let (sid, w1, ws) = opts(tmp.path(), "alice");
        let w2 = WorkerId::parse("bob").unwrap();
        fs::create_dir_all(&ws).unwrap();
        let _a = Store::open(
            &store_dir,
            &sid,
            &w1,
            Duration::from_secs(60),
            &ws,
            Hooks::default(),
        )
        .unwrap();
        let err = match Store::open(
            &store_dir,
            &sid,
            &w2,
            Duration::from_secs(60),
            &ws,
            Hooks::default(),
        ) {
            Err(e) => e,
            Ok(_) => panic!("expected LeaseHeld"),
        };
        match err {
            Error::LeaseHeld { holder, .. } => assert_eq!(holder.as_str(), "alice"),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn lease_steal_after_ttl_and_fence() {
        let tmp = tempfile::tempdir().unwrap();
        let store_dir = tmp.path().join("store");
        let (sid, w1, ws) = opts(tmp.path(), "alice");
        let w2 = WorkerId::parse("bob").unwrap();
        fs::create_dir_all(&ws).unwrap();
        let clock = Arc::new(ManualClock::new(1_000));
        let hooks = Hooks {
            clock: Some(clock.clone()),
            ..Hooks::default()
        };
        let mut a = Store::open(
            &store_dir,
            &sid,
            &w1,
            Duration::from_millis(60),
            &ws,
            hooks.clone(),
        )
        .unwrap();
        clock.set(1_000 + 60);
        let mut b = Store::open(
            &store_dir,
            &sid,
            &w2,
            Duration::from_millis(60),
            &ws,
            hooks,
        )
        .unwrap();
        let err = a
            .commit_events(
                &SessionState::origin(),
                &[Event::User {
                    content: "nope".into(),
                    op: OpId::from_static("x"),
                }],
                PersistOp::Append,
                &[],
                Some(&OpId::from_static("x")),
                |_, _| Ok(()),
            )
            .unwrap_err();
        assert!(matches!(err, Error::Fenced));
        let (logged, _) = b
            .commit_events(
                &SessionState::origin(),
                &[Event::User {
                    content: "ok".into(),
                    op: OpId::from_static("y"),
                }],
                PersistOp::Append,
                &[],
                Some(&OpId::from_static("y")),
                |_, _| Ok(()),
            )
            .unwrap();
        assert_eq!(logged.len(), 1);
    }
}
