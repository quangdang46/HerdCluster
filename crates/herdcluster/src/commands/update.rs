//! `herdcluster update` / `upgrade` — stub while implementation lands.

/// Placeholder dispatch; replaced by the real self-update implementation.
pub fn run(
    _version: Option<String>,
    _yes: bool,
    _json: bool,
    verbose: bool,
) -> i32 {
    if _json {
        println!(
            "{}",
            serde_json::json!({"error": {"code": "unimplemented", "message": "update not yet available in this build"}})
        );
    } else {
        eprintln!("error: herdcluster update is not yet available in this build");
    }
    let _ = verbose;
    1
}
