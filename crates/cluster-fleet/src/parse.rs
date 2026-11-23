//! Defensive JSON field extraction for herdr API responses.
//!
//! `api-client` deliberately returns `serde_json::Value` (it does not mirror
//! the upstream response schema). The fleet layer therefore reads responses
//! defensively: accept the common envelope shapes (`{"agents": [...]}`,
//! `{"agent": {...}}`, bare arrays/objects) and a small set of plausible
//! field-name aliases. This keeps herdcluster working across minor herdr
//! schema drift; live e2e validation is tracked separately (m5-spawn-e2e).

use serde_json::Value;

/// Return the "list" payload of a list-style response: a bare JSON array,
/// or the first array found under a well-known collection key.
pub(crate) fn items_array(value: &Value) -> &[Value] {
    if let Some(a) = value.as_array() {
        return a;
    }
    const COLLECTION_KEYS: &[&str] = &[
        "agents",
        "workspaces",
        "panes",
        "worktrees",
        "items",
        "results",
        "manifests",
    ];
    for key in COLLECTION_KEYS {
        if let Some(a) = value.get(*key).and_then(Value::as_array) {
            return a;
        }
    }
    &[]
}

/// First non-empty string among `keys` on a top-level JSON object.
pub(crate) fn str_field<'a>(value: &'a Value, keys: &[&str]) -> Option<&'a str> {
    let obj = value.as_object()?;
    for key in keys {
        if let Some(s) = obj.get(*key).and_then(Value::as_str) {
            if !s.is_empty() {
                return Some(s);
            }
        }
    }
    None
}

/// Depth-limited recursive search for an identifier string under `keys`.
///
/// Checks `value` itself first, then recurses into object/array children up
/// to `depth` levels. Used for `agent.start`/`pane.split`/`workspace.create`
/// responses whose id may sit at the top level or inside a nested
/// `{"pane": {...}}`-style wrapper.
pub(crate) fn find_id(value: &Value, keys: &[&str], depth: usize) -> Option<String> {
    if depth == 0 {
        return None;
    }
    if let Some(s) = str_field(value, keys) {
        return Some(s.to_string());
    }
    match value {
        Value::Object(map) => {
            for child in map.values() {
                if let Some(found) = find_id(child, keys, depth - 1) {
                    return Some(found);
                }
            }
            None
        }
        Value::Array(items) => {
            for child in items {
                if let Some(found) = find_id(child, keys, depth - 1) {
                    return Some(found);
                }
            }
            None
        }
        _ => None,
    }
}

/// Common identifier key aliases, most-specific first.
pub(crate) const ID_KEYS: &[&str] = &["pane_id", "workspace_id", "agent_id", "terminal_id", "id"];

/// Agent-status aliases.
pub(crate) const STATUS_KEYS: &[&str] = &["status", "state"];

/// Unwrap a single-entity response: `{"agent": {...}}` → inner object,
/// otherwise the value itself.
pub(crate) fn unwrap_entity<'a>(value: &'a Value, wrapper: &str) -> &'a Value {
    value.get(wrapper).unwrap_or(value)
}
