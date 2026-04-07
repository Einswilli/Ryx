//
// ###
// Ryx — JSON Lookups
// ###
//
// Contains JSON transforms and lookups (key, has_key, contains, etc.)
// These are used for chained lookups like `metadata__key__priority__exact="high"`
// ###

use crate::pool::Backend;
use crate::query::lookups::LookupContext;

pub use crate::query::lookups::LookupFn;

/// Apply a JSON field transformation.
/// Returns SQL like `(col->>'key')` or `JSON_UNQUOTE(JSON_EXTRACT(col, '$.key'))`
pub fn apply_json_transform(
    name: &str,
    column: &str,
    backend: Backend,
    key: Option<&str>,
) -> Option<String> {
    let sql = match (name, backend) {
        ("key", Backend::PostgreSQL) => {
            let k = key.unwrap_or("key");
            format!("({}->>'{}')", column, k)
        }
        ("key", Backend::MySQL) => {
            let k = key.unwrap_or("key");
            format!("JSON_UNQUOTE(JSON_EXTRACT({}, '$.{}'))", column, k)
        }
        ("key", Backend::SQLite) => {
            let k = key.unwrap_or("key");
            format!("json_extract({}, '$.{}')", column, k)
        }

        ("key_text", Backend::PostgreSQL) => {
            let k = key.unwrap_or("key");
            format!("({}->>'{}')::text", column, k)
        }
        ("key_text", Backend::MySQL) => {
            let k = key.unwrap_or("key");
            format!(
                "CAST(JSON_UNQUOTE(JSON_EXTRACT({}, '.{}')) AS CHAR)",
                column, k
            )
        }
        ("key_text", Backend::SQLite) => {
            let k = key.unwrap_or("key");
            format!("CAST(json_extract({}, '.{}') AS TEXT)", column, k)
        }

        ("json", Backend::PostgreSQL) => format!("({}::jsonb)", column),
        ("json", Backend::MySQL) => column.to_string(),
        ("json", Backend::SQLite) => column.to_string(),

        _ => return None,
    };
    Some(sql)
}

/// `field__key` → `(field->>'key')` or `JSON_UNQUOTE(JSON_EXTRACT(field, '$.key'))`
pub fn json_key_transform(ctx: &LookupContext) -> String {
    match ctx.backend {
        Backend::PostgreSQL => format!("({}->>'key')", ctx.column),
        Backend::MySQL => format!("JSON_UNQUOTE(JSON_EXTRACT({}, '$.key'))", ctx.column),
        Backend::SQLite => format!("json_extract({}, '$.key')", ctx.column),
    }
}

/// `field__key_text` → `(field->>'key')::text` (for text comparisons like icontains)
pub fn json_key_text_transform(ctx: &LookupContext) -> String {
    match ctx.backend {
        Backend::PostgreSQL => format!("({}->>'key')::text", ctx.column),
        Backend::MySQL => format!(
            "CAST(JSON_UNQUOTE(JSON_EXTRACT({}, '$.key')) AS CHAR)",
            ctx.column
        ),
        Backend::SQLite => format!("CAST(json_extract({}, '$.key') AS TEXT)", ctx.column),
    }
}

/// `field__json` → `field::jsonb` (PostgreSQL) or just field (MySQL/SQLite)
pub fn json_cast_transform(ctx: &LookupContext) -> String {
    match ctx.backend {
        Backend::PostgreSQL => format!("({}::jsonb)", ctx.column),
        Backend::MySQL => ctx.column.clone(),
        Backend::SQLite => ctx.column.clone(),
    }
}

/// `field__has_key="key"` → `field ? 'key'` (PostgreSQL) or `JSON_CONTAINS(field, '"key"')` (MySQL)
pub fn json_has_key(ctx: &LookupContext) -> String {
    match ctx.backend {
        Backend::PostgreSQL => format!("({} ? 'key')", ctx.column),
        Backend::MySQL => format!("JSON_CONTAINS({}, '\"key\"')", ctx.column),
        Backend::SQLite => format!("json_extract({}, '$.key') IS NOT NULL", ctx.column),
    }
}

/// `field__has_keys=['key1', 'key2']` → `field ?& array['key1', 'key2']`
pub fn json_has_keys(ctx: &LookupContext) -> String {
    match ctx.backend {
        Backend::PostgreSQL => format!("({} ?& array['key1', 'key2'])", ctx.column),
        Backend::MySQL => format!("JSON_CONTAINS({}, '[\"key1\", \"key2\"]')", ctx.column),
        Backend::SQLite => format!(
            "json_extract({}, '$.key1') IS NOT NULL AND json_extract({}, '$.key2') IS NOT NULL",
            ctx.column, ctx.column
        ),
    }
}

/// `field__contains={"key": "value"}` → `field @> ?` (PostgreSQL)
pub fn json_contains(ctx: &LookupContext) -> String {
    match ctx.backend {
        Backend::PostgreSQL => format!("({} @> ?)", ctx.column),
        Backend::MySQL => format!("JSON_CONTAINS({}, ?)", ctx.column),
        Backend::SQLite => ctx.column.clone(), // Limited support in SQLite
    }
}

/// `field__contained_by={"key": "value"}` → `field <@ ?` (PostgreSQL)
pub fn json_contained_by(ctx: &LookupContext) -> String {
    match ctx.backend {
        Backend::PostgreSQL => format!("({} <@ ?)", ctx.column),
        Backend::MySQL => format!("JSON_CONTAINS(?, {})", ctx.column),
        Backend::SQLite => ctx.column.clone(), // Limited support in SQLite
    }
}
