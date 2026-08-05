//! LogStore owning the SQLite connection and lifecycle.

use crate::artifact_privacy::{ArtifactPrivacy, PlatformArtifactPrivacy};
use crate::error::LogStoreError;
use rusqlite::{Connection, InterruptHandle, Transaction};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// Clock abstraction for deterministic timestamps in tests.
pub trait Clock: Send + Sync {
    fn now(&self) -> String;
}

#[derive(Debug, Clone)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> String {
        let dt = chrono::Utc::now();
        format!("{}", dt.format("%Y-%m-%dT%H:%M:%SZ"))
    }
}

pub struct LogStore {
    conn: Mutex<Connection>,
    /// A connection-scoped interrupt handle for an exclusively owned
    /// background connection. Request persistence must never call this.
    interrupt_handle: InterruptHandle,
    clock: std::sync::Arc<dyn Clock>,
    #[cfg_attr(not(test), allow(unused))]
    db_path: PathBuf,
}

impl LogStore {
    pub fn open(
        root_path: impl AsRef<Path>,
        clock: std::sync::Arc<dyn Clock>,
    ) -> Result<Self, LogStoreError> {
        let root = prepare_private_store_root(root_path.as_ref())?;

        let db_path = root.join("log_store.db");
        reject_link_if_present(&db_path)?;
        let conn = Connection::open(&db_path).map_err(|e| {
            LogStoreError::IoError(std::io::Error::other(format!("sqlite open: {}", e)))
        })?;
        prepare_private_database_files(&db_path)?;

        let pragmas = "
            PRAGMA journal_mode = WAL;
            PRAGMA foreign_keys = ON;
            PRAGMA busy_timeout = 30000;
        ";
        conn.execute_batch(pragmas).map_err(LogStoreError::Sqlite)?;

        crate::migrations::apply_migrations(&conn)
            .map_err(|e| LogStoreError::MigrationFailed(e.to_string()))?;
        prepare_private_database_files(&db_path)?;
        let interrupt_handle = conn.get_interrupt_handle();

        Ok(Self {
            conn: Mutex::new(conn),
            interrupt_handle,
            clock,
            db_path,
        })
    }

    pub fn reopen_at(
        root_path: impl AsRef<Path>,
        clock: std::sync::Arc<dyn Clock>,
    ) -> Result<Self, LogStoreError> {
        Self::open(root_path, clock)
    }

    /// Open a dedicated connection for a background owner. SQLite interrupts
    /// are connection-scoped, so this lets retention stop without disturbing
    /// request-path persistence on the primary connection.
    pub fn reopen_for_background_worker(&self) -> Result<Self, LogStoreError> {
        let parent = self.db_path.parent().ok_or_else(|| {
            LogStoreError::IoError(std::io::Error::other("logging database has no parent"))
        })?;
        Self::open(parent, std::sync::Arc::clone(&self.clock))
    }

    /// Interrupt the query currently executing on this exclusive connection.
    pub fn interrupt(&self) {
        self.interrupt_handle.interrupt();
    }

    pub fn txn<T>(
        &self,
        f: impl FnOnce(&Transaction) -> Result<T, LogStoreError>,
    ) -> Result<T, LogStoreError> {
        let mut conn = self
            .conn
            .lock()
            .map_err(|_| LogStoreError::ConnectionPoisoned)?;

        let tx = conn.transaction().map_err(LogStoreError::Sqlite)?;
        let result = f(&tx);
        if result.is_ok() {
            tx.commit().map_err(LogStoreError::Sqlite)?;
            self.prepare_private_database_files()?;
        }
        result
    }

    pub fn conn(&self) -> std::sync::MutexGuard<'_, Connection> {
        match self.conn.lock() {
            Ok(conn) => conn,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    pub fn now(&self) -> String {
        self.clock.now()
    }

    fn prepare_private_database_files(&self) -> Result<(), LogStoreError> {
        prepare_private_database_files(&self.db_path)
    }

    #[cfg(test)]
    pub fn db_path(&self) -> &Path {
        &self.db_path
    }

    pub fn schema_version(&self) -> u32 {
        self.conn()
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap_or(0) as u32
    }

    #[cfg(test)]
    pub fn reopen(&self, clock: std::sync::Arc<dyn Clock>) -> Result<Self, LogStoreError> {
        let parent = self.db_path.parent().ok_or_else(|| {
            LogStoreError::IoError(std::io::Error::other("no parent dir for db path"))
        })?;

        Self::open(parent, clock)
    }
}

fn prepare_private_store_root(root: &Path) -> Result<PathBuf, LogStoreError> {
    std::fs::create_dir_all(root)?;
    // Inspect the caller-supplied final component before canonicalizing it.
    // Canonicalization itself follows links, which would otherwise silently
    // bless a database root redirected outside the configured app-state area.
    let privacy = PlatformArtifactPrivacy;
    privacy.prepare_directory(root)?;
    let canonical = root.canonicalize().map_err(LogStoreError::IoError)?;
    privacy.prepare_directory(&canonical)?;
    Ok(canonical)
}

/// SQLite can materialize `-wal` and `-shm` lazily. Prepare the main database
/// and every sidecar that exists after opening/committing, rather than relying
/// on the process umask or inherited Windows ACL alone.
fn prepare_private_database_files(db_path: &Path) -> Result<(), LogStoreError> {
    let privacy = PlatformArtifactPrivacy;
    privacy.prepare_file(db_path)?;
    for suffix in ["-wal", "-shm"] {
        let path = PathBuf::from(format!("{}{}", db_path.display(), suffix));
        match std::fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(LogStoreError::PathUnsafe {
                    segment: "symlink_not_allowed".to_string(),
                });
            }
            Ok(_) => privacy.prepare_file(&path)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(LogStoreError::IoError(error)),
        }
    }
    Ok(())
}

fn reject_link_if_present(path: &Path) -> Result<(), LogStoreError> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(LogStoreError::PathUnsafe {
            segment: "symlink_not_allowed".to_string(),
        }),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(LogStoreError::IoError(error)),
    }
}

#[cfg(test)]
mod tests {
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::sync::Arc;

    use super::{LogStore, SystemClock};
    use crate::LogStoreError;

    #[test]
    fn poisoned_connection_returns_typed_transaction_error_without_panicking_reads() {
        let root = tempfile::tempdir().expect("temporary log store root");
        let store = LogStore::open(root.path(), Arc::new(SystemClock)).expect("open log store");

        let panic_result = catch_unwind(AssertUnwindSafe(|| {
            let _connection = match store.conn.lock() {
                Ok(connection) => connection,
                Err(poisoned) => poisoned.into_inner(),
            };
            panic!("intentionally poison the connection lock");
        }));
        assert!(panic_result.is_err());

        let transaction_error = store
            .txn(|_| Ok(()))
            .expect_err("poisoned connection must reject transactions");
        assert!(matches!(
            transaction_error,
            LogStoreError::ConnectionPoisoned
        ));

        assert_eq!(store.schema_version(), crate::migrations::CURRENT_VERSION);
    }
}
