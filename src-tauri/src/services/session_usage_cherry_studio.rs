//! Cherry Studio usage import.
//!
//! Reads Cherry Studio's SQLite usage ledger without modifying it, then copies
//! language-model invocation records into CC Switch's unified usage table.

use crate::database::{lock_conn, Database};
use crate::error::AppError;
use crate::services::session_usage::{
    get_sync_state, metadata_modified_nanos, update_sync_state, SessionSyncResult,
};
use std::fs;
use std::path::{Path, PathBuf};

const APP_TYPE: &str = "cherry-studio";
const DATA_SOURCE: &str = "cherry_studio";

#[derive(Debug)]
struct CherryUsageRecord {
    request_id: String,
    provider_id: String,
    provider_name: String,
    model_id: String,
    input_tokens: u32,
    output_tokens: u32,
    cache_read_tokens: u32,
    cache_write_tokens: u32,
    total_cost_usd: f64,
    first_token_ms: Option<i64>,
    duration_ms: i64,
    created_at: i64,
}

/// Returns Cherry Studio's usage database path.
///
/// `CHERRY_STUDIO_DB` supports portable and relocated installations. Otherwise
/// the platform-standard packaged Electron user-data location is used.
pub fn get_cherry_studio_db_path() -> PathBuf {
    if let Some(path) = std::env::var_os("CHERRY_STUDIO_DB").filter(|value| !value.is_empty()) {
        return PathBuf::from(path);
    }

    #[cfg(target_os = "macos")]
    let user_data = crate::config::get_home_dir()
        .join("Library")
        .join("Application Support")
        .join("CherryStudio");

    #[cfg(target_os = "windows")]
    let user_data = std::env::var_os("APPDATA")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            crate::config::get_home_dir()
                .join("AppData")
                .join("Roaming")
        })
        .join("CherryStudio");

    #[cfg(target_os = "linux")]
    let user_data = std::env::var_os("XDG_CONFIG_HOME")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| crate::config::get_home_dir().join(".config"))
        .join("CherryStudio");

    user_data.join("Data").join("cherrystudio.sqlite")
}

/// Imports newly committed Cherry Studio usage records.
pub fn sync_cherry_studio_usage(db: &Database) -> Result<SessionSyncResult, AppError> {
    let db_path = get_cherry_studio_db_path();
    sync_cherry_studio_usage_from_path(db, &db_path)
}

fn sync_cherry_studio_usage_from_path(
    db: &Database,
    db_path: &Path,
) -> Result<SessionSyncResult, AppError> {
    if !db_path.exists() {
        return Ok(SessionSyncResult::default());
    }

    let db_path_str = db_path.to_string_lossy().to_string();
    let metadata = fs::metadata(db_path).map_err(|error| AppError::io(db_path, error))?;
    let mut modified = metadata_modified_nanos(&metadata);
    let wal_path = PathBuf::from(format!("{}-wal", db_path.display()));
    if let Ok(wal_metadata) = fs::metadata(wal_path) {
        modified = modified.max(metadata_modified_nanos(&wal_metadata));
    }

    let (last_modified, _) = get_sync_state(db, &db_path_str)?;
    if modified <= last_modified {
        return Ok(SessionSyncResult {
            files_scanned: 1,
            ..SessionSyncResult::default()
        });
    }

    let cherry =
        rusqlite::Connection::open_with_flags(db_path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .map_err(|error| {
                AppError::Database(format!("无法只读打开 Cherry Studio 数据库: {error}"))
            })?;

    if !has_usage_table(&cherry)? {
        return Ok(SessionSyncResult {
            files_scanned: 1,
            errors: vec!["Cherry Studio 数据库中没有 ai_usage_record 表".to_string()],
            ..SessionSyncResult::default()
        });
    }

    let records = query_usage_records(&cherry)?;
    let mut result = SessionSyncResult {
        files_scanned: 1,
        ..SessionSyncResult::default()
    };
    for record in records {
        match insert_usage_record(db, &record) {
            Ok(true) => result.imported += 1,
            Ok(false) => result.skipped += 1,
            Err(error) => {
                result.skipped += 1;
                result.errors.push(format!(
                    "Cherry Studio 记录 {} 导入失败: {error}",
                    record.request_id
                ));
            }
        }
    }

    if result.errors.is_empty() {
        update_sync_state(db, &db_path_str, modified, 0)?;
    }
    Ok(result)
}

fn has_usage_table(conn: &rusqlite::Connection) -> Result<bool, AppError> {
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'ai_usage_record')",
        [],
        |row| row.get(0),
    )
    .map_err(|error| AppError::Database(format!("检查 Cherry Studio 用量表失败: {error}")))
}

fn nonnegative_u32(value: Option<i64>) -> u32 {
    value.unwrap_or(0).clamp(0, u32::MAX as i64) as u32
}

fn query_usage_records(conn: &rusqlite::Connection) -> Result<Vec<CherryUsageRecord>, AppError> {
    let mut stmt = conn
        .prepare(
            "SELECT request_id, provider_id, provider_name, model_id, model_name,
                    input_tokens, output_tokens, no_cache_tokens,
                    cache_read_tokens, cache_write_tokens, cost, cost_currency,
                    time_first_token_ms, time_completion_ms, created_at
             FROM ai_usage_record
             WHERE modality = 'language'
             ORDER BY created_at, id",
        )
        .map_err(|error| AppError::Database(format!("准备 Cherry Studio 用量查询失败: {error}")))?;

    let rows = stmt
        .query_map([], |row| {
            let all_input = nonnegative_u32(row.get(5)?);
            let cache_read = nonnegative_u32(row.get(8)?);
            let cache_write = nonnegative_u32(row.get(9)?);
            let no_cache: Option<i64> = row.get(7)?;
            let input_tokens = no_cache
                .map(|value| nonnegative_u32(Some(value)))
                .unwrap_or_else(|| {
                    all_input.saturating_sub(cache_read.saturating_add(cache_write))
                });
            let currency: Option<String> = row.get(11)?;
            let cost: Option<f64> = row.get(10)?;
            let first_token_ms: Option<i64> = row.get(12)?;
            let completion_ms: Option<i64> = row.get(13)?;
            let provider_id: Option<String> = row.get(1)?;
            let provider_name: Option<String> = row.get(2)?;
            let model_id: Option<String> = row.get(3)?;
            let model_name: Option<String> = row.get(4)?;

            Ok(CherryUsageRecord {
                request_id: row.get(0)?,
                provider_id: provider_id.unwrap_or_else(|| "unknown".to_string()),
                provider_name: provider_name.unwrap_or_else(|| "Cherry Studio".to_string()),
                model_id: model_name
                    .or(model_id)
                    .unwrap_or_else(|| "unknown".to_string()),
                input_tokens,
                output_tokens: nonnegative_u32(row.get(6)?),
                cache_read_tokens: cache_read,
                cache_write_tokens: cache_write,
                total_cost_usd: if currency.as_deref() == Some("USD") {
                    cost.filter(|value| value.is_finite() && *value >= 0.0)
                        .unwrap_or(0.0)
                } else {
                    0.0
                },
                first_token_ms: first_token_ms.filter(|value| *value >= 0),
                duration_ms: completion_ms.unwrap_or(0).max(0),
                created_at: row.get::<_, i64>(14)? / 1000,
            })
        })
        .map_err(|error| AppError::Database(format!("查询 Cherry Studio 用量失败: {error}")))?;

    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|error| AppError::Database(format!("读取 Cherry Studio 用量行失败: {error}")))
}

fn insert_usage_record(db: &Database, record: &CherryUsageRecord) -> Result<bool, AppError> {
    let conn = lock_conn!(db.conn);
    let request_id = format!("cherry_studio:{}", record.request_id);
    let inserted = conn.execute(
        "INSERT OR IGNORE INTO proxy_request_logs (
            request_id, provider_id, app_type, model, request_model,
            input_tokens, output_tokens, cache_read_tokens, cache_creation_tokens,
            input_cost_usd, output_cost_usd, cache_read_cost_usd,
            cache_creation_cost_usd, total_cost_usd, latency_ms, first_token_ms,
            duration_ms, status_code, error_message, session_id, provider_type,
            is_streaming, cost_multiplier, created_at, data_source
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, '0', '0', '0', '0',
                  ?10, ?11, ?12, ?13, 200, NULL, NULL, ?14, 1, '1.0', ?15, ?16)",
        rusqlite::params![
            request_id,
            record.provider_name,
            APP_TYPE,
            record.model_id,
            record.model_id,
            record.input_tokens,
            record.output_tokens,
            record.cache_read_tokens,
            record.cache_write_tokens,
            record.total_cost_usd.to_string(),
            record.duration_ms,
            record.first_token_ms,
            record.duration_ms,
            record.provider_id,
            record.created_at,
            DATA_SOURCE,
        ],
    )?;
    Ok(inserted > 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_cherry_database_is_a_noop() -> Result<(), AppError> {
        let db = Database::memory()?;
        let temp = tempfile::tempdir().unwrap();

        let result = sync_cherry_studio_usage_from_path(&db, &temp.path().join("missing.sqlite"))?;

        assert_eq!(result.imported, 0);
        assert_eq!(result.files_scanned, 0);
        assert!(result.errors.is_empty());
        let conn = lock_conn!(db.conn);
        let count: i64 = conn.query_row("SELECT COUNT(*) FROM proxy_request_logs", [], |row| {
            row.get(0)
        })?;
        assert_eq!(count, 0);
        Ok(())
    }

    #[test]
    fn import_is_scoped_to_cherry_usage_and_preserves_other_apps() -> Result<(), AppError> {
        let db = Database::memory()?;
        {
            let conn = lock_conn!(db.conn);
            conn.execute(
                "INSERT INTO proxy_request_logs (
                    request_id, provider_id, app_type, model, request_model,
                    input_tokens, output_tokens, cache_read_tokens, cache_creation_tokens,
                    total_cost_usd, latency_ms, status_code, created_at, data_source
                ) VALUES ('existing-claude', 'anthropic', 'claude', 'claude-test',
                          'claude-test', 7, 3, 0, 0, '0.01', 10, 200, 1, 'proxy')",
                [],
            )?;
        }

        let temp = tempfile::tempdir().unwrap();
        let cherry_path = temp.path().join("cherrystudio.sqlite");
        let cherry = rusqlite::Connection::open(&cherry_path)?;
        cherry.execute_batch(
            "CREATE TABLE ai_usage_record (
                id TEXT, request_id TEXT, provider_id TEXT, provider_name TEXT,
                model_id TEXT, model_name TEXT, modality TEXT, input_tokens INTEGER,
                output_tokens INTEGER, no_cache_tokens INTEGER, cache_read_tokens INTEGER,
                cache_write_tokens INTEGER, cost REAL, cost_currency TEXT,
                time_first_token_ms INTEGER, time_completion_ms INTEGER, created_at INTEGER
             );
             INSERT INTO ai_usage_record VALUES
                ('1', 'language', 'openai', 'OpenAI', 'gpt-test', 'GPT Test',
                 'language', 100, 20, 60, 30, 10, 0.5, 'USD', 12, 34, 2000),
                ('2', 'embedding', 'openai', 'OpenAI', 'embed-test', NULL,
                 'embedding', 50, 0, 50, 0, 0, 0.1, 'USD', NULL, 10, 3000);",
        )?;
        drop(cherry);

        let first = sync_cherry_studio_usage_from_path(&db, &cherry_path)?;
        assert_eq!(first.imported, 1);
        assert!(first.errors.is_empty());

        let second = sync_cherry_studio_usage_from_path(&db, &cherry_path)?;
        assert_eq!(second.imported, 0);
        assert!(second.errors.is_empty());

        let conn = lock_conn!(db.conn);
        let claude_count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM proxy_request_logs
             WHERE request_id = 'existing-claude' AND app_type = 'claude'
               AND input_tokens = 7 AND output_tokens = 3",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(claude_count, 1);

        let cherry_row: (i64, String, String, String, i64, i64, i64, i64) = conn.query_row(
            "SELECT COUNT(*), data_source, provider_id, provider_type, input_tokens,
                    output_tokens, cache_read_tokens, cache_creation_tokens
             FROM proxy_request_logs WHERE app_type = 'cherry-studio'",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                ))
            },
        )?;
        assert_eq!(
            cherry_row,
            (
                1,
                "cherry_studio".into(),
                "OpenAI".into(),
                "openai".into(),
                60,
                20,
                30,
                10
            )
        );
        Ok(())
    }

    #[test]
    fn query_normalizes_cherry_input_and_currency() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE ai_usage_record (
                id TEXT, request_id TEXT, provider_id TEXT, provider_name TEXT,
                model_id TEXT, model_name TEXT, modality TEXT, input_tokens INTEGER,
                output_tokens INTEGER, no_cache_tokens INTEGER, cache_read_tokens INTEGER,
                cache_write_tokens INTEGER, cost REAL, cost_currency TEXT,
                time_first_token_ms INTEGER, time_completion_ms INTEGER, created_at INTEGER
             );
             INSERT INTO ai_usage_record VALUES
                ('1', 'usd', 'p1', 'Provider', 'm1', 'Model', 'language', 100, 20, NULL, 30, 10, 0.5, 'USD', 12, 34, 2000),
                ('2', 'cny', 'p1', 'Provider', 'm1', NULL, 'language', 100, 20, 55, 30, 10, 3.0, 'CNY', NULL, NULL, 3000);",
        )
        .unwrap();

        let records = query_usage_records(&conn).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].input_tokens, 60);
        assert_eq!(records[0].model_id, "Model");
        assert_eq!(records[0].total_cost_usd, 0.5);
        assert_eq!(records[1].input_tokens, 55);
        assert_eq!(records[1].model_id, "m1");
        assert_eq!(records[1].total_cost_usd, 0.0);
    }
}
