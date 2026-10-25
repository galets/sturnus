#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

pub mod auth;
pub mod body;
pub mod config;
pub mod gcp_auth;
pub mod metrics;
pub mod model_map;
pub mod proxy;
pub mod router;
pub mod server;
pub mod trace;
pub mod tracker;

/// The one version this build reports: `--version`, the startup log line, and
/// the interactive banner all read this, so none of them can drift from the
/// package manifest the release pipeline checks. Build metadata (`+fork`) is
/// part of it and is never trimmed.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Install ring as the process-wide rustls crypto provider. Idempotent.
pub fn init_crypto() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}
