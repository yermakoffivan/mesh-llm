//! LogStore owning the SQLite connection and lifecycle.

use crate::artifact_privacy::{ArtifactPrivacy, PlatformArtifactPrivacy};
use crate::error::LogStoreError;
use rusqlite::{Connection, Transaction};
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

        Ok(Self {
            conn: Mutex::new(conn),
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

    pub fn txn<T>(
        &self,
        f: impl FnOnce(&Transaction) -> Result<T, LogStoreError>,
    ) -> Result<T, LogStoreError> {
        let mut conn = self
            .conn
            .lock()
            .map_err(|_| LogStoreError::Sqlite(rusqlite::Error::ExecuteReturnedResults))?;

        let tx = conn.transaction().map_err(LogStoreError::Sqlite)?;
        let result = f(&tx);
        if result.is_ok() {
            tx.commit().map_err(LogStoreError::Sqlite)?;
            self.prepare_private_database_files()?;
        }
        result
    }

    pub fn conn(&self) -> std::sync::MutexGuard<'_, Connection> {
        self.conn.lock().expect("connection mutex poisoned")
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
