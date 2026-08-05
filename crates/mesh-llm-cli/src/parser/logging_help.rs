/// Operator-facing logging details appended to `--help-advanced`.
///
/// This intentionally uses portable, user-relative locations. The runtime
/// resolves the actual root without printing a private absolute path.
pub fn logging_help() -> &'static str {
    concat!(
        "\n\nLogging and local capture:\n",
        "  --log-format pretty     Render operational rows/TUI on stderr (default).\n",
        "  --log-format json       Emit one stable JSON event per stdout line.\n",
        "  Local metadata store    ~/.mesh-llm/logging/store/log_store.db\n",
        "  Local artifact store    ~/.mesh-llm/logging/artifacts/\n",
        "  Path precedence         logging.application_state_root, then ",
        "MESH_LLM_DATA_DIR/logging, then the local default above.\n",
        "  Retention setting       logging.retention_ttl_secs (default: 129600 seconds).\n",
        "  TUI event navigation    / filter, f follow, PgUp/PgDn page, Home/End jump.\n",
        "  Query/follow API        /api/logs/requests and /api/logs/events?channel=requests\n\n",
        "Logging config (~/.mesh-llm/config.toml):\n",
        "  [logging]\n",
        "  enabled = true\n",
        "  retention_ttl_secs = 129600  # 36 hours\n",
        "  [logging.artifact]\n",
        "  capture_mode = \"metadata_only\"  # redacted_artifacts is explicit opt-in\n",
        "Ordinary terminal/JSON output never includes captured artifact payloads.\n"
    )
}

#[cfg(test)]
mod tests {
    use super::logging_help;

    #[test]
    fn logging_help_describes_storage_capture_retention_and_navigation() {
        let help = logging_help();

        assert!(help.contains("~/.mesh-llm/logging"));
        assert!(help.contains("MESH_LLM_DATA_DIR"));
        assert!(help.contains("logging.application_state_root"));
        assert!(help.contains("logging.retention_ttl_secs"));
        assert!(help.contains("metadata_only"));
        assert!(help.contains("--log-format json"));
        assert!(help.contains("/api/logs/requests"));
        assert!(help.contains("/ filter"));
        assert!(help.contains("PgUp/PgDn"));
    }

    #[test]
    fn logging_help_does_not_expand_the_private_home_directory() {
        let help = logging_help();
        let home = std::env::var("HOME").expect("HOME should be available");

        assert!(!help.contains(&home));
    }
}
