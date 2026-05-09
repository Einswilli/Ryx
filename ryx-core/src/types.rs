use sqlx::{PgConnection, MySqlConnection, SqliteConnection, Transaction};

/// Unified connection enum to avoid dynamic dispatch in the hot path.
#[derive(Debug)]
pub enum RyxConnection {
    Postgres(PgConnection),
    MySql(MySqlConnection),
    Sqlite(SqliteConnection),
}

/// Unified transaction enum.
/// Uses 'static because transactions are held across PyO3 boundaries in Arc<Mutex<Option<...>>>.
#[derive(Debug)]
pub enum RyxTransaction {
    Postgres(Transaction<'static, sqlx::Postgres>),
    MySql(Transaction<'static, sqlx::MySql>),
    Sqlite(Transaction<'static, sqlx::Sqlite>),
}
