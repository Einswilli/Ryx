// Ryx — Model/Field registry in Rust
//
// This registry stores model metadata (options + fields) so the Rust side can
// answer questions about models/fields without bouncing back into Python.
// It is intentionally minimal for now and can be extended (indexes, constraints,
// relations, validators) as we migrate more ORM pieces.

use once_cell::sync::OnceCell;
use pyo3::prelude::*;
use std::collections::HashMap;
use std::sync::RwLock;

#[pyclass(from_py_object)]
#[derive(Clone, Debug)]
pub struct PyFieldSpec {
    #[pyo3(get)]
    pub name: String,
    #[pyo3(get)]
    pub column: String,
    #[pyo3(get)]
    pub primary_key: bool,
    #[pyo3(get)]
    pub data_type: String,
    #[pyo3(get)]
    pub nullable: bool,
    #[pyo3(get)]
    pub unique: bool,
}

#[pyclass(from_py_object)]
#[derive(Clone, Debug)]
pub struct PyModelOptions {
    #[pyo3(get)]
    pub table: String,
    #[pyo3(get)]
    pub app_label: Option<String>,
    #[pyo3(get)]
    pub database: Option<String>,
    #[pyo3(get)]
    pub ordering: Vec<String>,
    #[pyo3(get)]
    pub managed: bool,
    #[pyo3(get)]
    pub abstract_model: bool,
}

#[pyclass(from_py_object)]
#[derive(Clone, Debug)]
pub struct PyModelSpec {
    #[pyo3(get)]
    pub name: String,
    #[pyo3(get)]
    pub options: PyModelOptions,
    #[pyo3(get)]
    pub fields: Vec<PyFieldSpec>,
}

impl PyModelSpec {
    fn new(name: String, options: PyModelOptions, fields: Vec<PyFieldSpec>) -> Self {
        Self {
            name,
            options,
            fields,
        }
    }
}

static REGISTRY: OnceCell<RwLock<HashMap<String, PyModelSpec>>> = OnceCell::new();
static TABLE_INDEX: OnceCell<RwLock<HashMap<String, String>>> = OnceCell::new(); // table -> model name

fn registry() -> &'static RwLock<HashMap<String, PyModelSpec>> {
    REGISTRY.get_or_init(|| RwLock::new(HashMap::new()))
}

fn table_index() -> &'static RwLock<HashMap<String, String>> {
    TABLE_INDEX.get_or_init(|| RwLock::new(HashMap::new()))
}

#[pyfunction]
pub fn register_model_spec(
    name: String,
    table: String,
    app_label: Option<String>,
    database: Option<String>,
    ordering: Option<Vec<String>>,
    managed: Option<bool>,
    abstract_model: Option<bool>,
    // fields: list of (name, column, primary_key, data_type, nullable, unique)
    fields: Vec<(String, String, bool, String, bool, bool)>,
) -> PyResult<()> {
    let options = PyModelOptions {
        table,
        app_label,
        database,
        ordering: ordering.unwrap_or_default(),
        managed: managed.unwrap_or(true),
        abstract_model: abstract_model.unwrap_or(false),
    };
    let fields: Vec<PyFieldSpec> = fields
        .into_iter()
        .map(
            |(name, column, primary_key, data_type, nullable, unique)| PyFieldSpec {
                name,
                column,
                primary_key,
                data_type,
                nullable,
                unique,
            },
        )
        .collect();

    let spec = PyModelSpec::new(name.clone(), options.clone(), fields);
    let reg = registry();
    let mut guard = reg.write().map_err(|e| {
        pyo3::exceptions::PyRuntimeError::new_err(format!("Model registry poisoned: {e}"))
    })?;
    guard.insert(name.clone(), spec);

    let idx = table_index();
    let mut iguard = idx.write().map_err(|e| {
        pyo3::exceptions::PyRuntimeError::new_err(format!("Model registry poisoned: {e}"))
    })?;
    iguard.insert(options.table.clone(), name);
    Ok(())
}

#[pyfunction]
pub fn get_model_spec(name: String) -> PyResult<Option<PyModelSpec>> {
    let reg = registry();
    let guard = reg.read().map_err(|e| {
        pyo3::exceptions::PyRuntimeError::new_err(format!("Model registry poisoned: {e}"))
    })?;
    Ok(guard.get(&name).cloned())
}

/// Internal helper for Rust callers: find field spec by table+column.
pub fn lookup_field(table: &str, column: &str) -> Option<PyFieldSpec> {
    let idx = table_index().read().ok()?;
    let model = idx.get(table)?;
    let reg = registry().read().ok()?;
    let spec = reg.get(model)?;
    spec.fields
        .iter()
        .find(|f| f.column == column || f.name == column)
        .cloned()
}

/// Get full model spec by table name.
pub fn get_model_spec_for_table(table: &str) -> Option<PyModelSpec> {
    let idx = table_index().read().ok()?;
    let model = idx.get(table)?;
    let reg = registry().read().ok()?;
    reg.get(model).cloned()
}
