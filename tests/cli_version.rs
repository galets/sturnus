// End-to-end CLI checks. These run the built binary on purpose: the point of
// `--version` is that it answers on a box whose config is what is broken, so
// asserting about a parsed `Cli` in-process would prove nothing.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Stdio};
use std::time::Duration;

use tempfile::TempDir;

/// The binary cargo just built for this test run.
const BIN: &str = env!("CARGO_BIN_EXE_sturnus");

/// A minimal config that starts cleanly. `127.0.0.1:0` binds an ephemeral port,
/// so concurrent test binaries never collide on it.
const MINIMAL_CONFIG: &str = "\
listen = \"127.0.0.1:0\"

[provider.local]
base_url = \"http://127.0.0.1:9/v1\"
api_key = \"unused\"

[model]
fast = [{ provider = \"local\", model = \"m\" }]
";

struct Run {
    #[allow(dead_code)]
    status: ExitStatus,
    stdout: String,
    stderr: String,
}

impl Run {
    /// stdout lines, in order.
    fn lines(&self) -> Vec<&str> {
        self.stdout.lines().collect()
    }

    fn first_line(&self) -> &str {
        self.lines()
            .first()
            .copied()
            .unwrap_or_else(|| panic!("no output on stdout; stderr {:?}", self.stderr))
    }
}

/// Command with the environment a bare host would have: no `HOME` (so the
/// default config path is unknown), no log-format or log-level overrides.
fn sturnus() -> Command {
    let mut cmd = Command::new(BIN);
    cmd.env_remove("HOME")
        .env_remove("STURNUS_LOG_FORMAT")
        .env_remove("RUST_LOG")
        .env_remove("NO_COLOR")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    cmd
}

/// Run to completion. For the paths that exit by themselves.
fn run_to_exit(cmd: &mut Command) -> Run {
    let out = cmd.output().expect("spawn sturnus");
    Run {
        status: out.status,
        stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
    }
}

/// Run a process that stays up, kill it after `grace`, and return everything it
/// wrote. The kill is what closes the pipes, so the reads cannot block forever.
fn run_until_killed(cmd: &mut Command, grace: Duration) -> Run {
    let mut child = cmd.spawn().expect("spawn sturnus");
    let mut stdout = child.stdout.take().expect("piped stdout");
    let mut stderr = child.stderr.take().expect("piped stderr");

    let killer = std::thread::spawn(move || {
        let mut child = child;
        std::thread::sleep(grace);
        // The startup path is what is under test, not the shutdown path, so
        // SIGKILL is enough: it ends the process and closes the pipes.
        let _ = child.kill();
        child.wait().expect("reap sturnus")
    });

    let mut out = String::new();
    let mut err = String::new();
    stdout.read_to_string(&mut out).expect("read stdout");
    stderr.read_to_string(&mut err).expect("read stderr");
    Run {
        status: killer.join().expect("killer thread"),
        stdout: out,
        stderr: err,
    }
}

fn write_config(dir: &Path, name: &str, body: &str) -> PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, body).expect("write config");
    path
}

/// The version from `Cargo.toml`, read at runtime. Reading the file rather than
/// `env!("CARGO_PKG_VERSION")` is what makes this a check: a hand-maintained
/// version literal anywhere in the tree fails against it.
fn manifest_version() -> String {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
    let text = std::fs::read_to_string(&manifest).expect("Cargo.toml readable");
    let mut in_package = false;
    for line in text.lines() {
        let line = line.trim();
        if line.starts_with('[') {
            in_package = line == "[package]";
            continue;
        }
        if in_package {
            if let Some(value) = line.strip_prefix("version") {
                return value
                    .trim()
                    .trim_start_matches('=')
                    .trim()
                    .trim_matches('"')
                    .to_string();
            }
        }
    }
    panic!("no version key in [package]");
}

/// A JSON log line's `fields` block, or a panic naming the line.
fn fields_of(line: &str) -> serde_json::Value {
    let record: serde_json::Value =
        serde_json::from_str(line).unwrap_or_else(|e| panic!("log line is not JSON: {e}\n{line}"));
    record["fields"].clone()
}

#[test]
fn version_flag_prints_the_version_and_exits_zero() {
    let declared = manifest_version();
    let dir = TempDir::new().expect("tempdir");
    for flag in ["--version", "-V"] {
        let run = run_to_exit(sturnus().arg(flag).current_dir(dir.path()));
        assert!(
            run.status.success(),
            "{flag} exited {} with stderr {:?}",
            run.status,
            run.stderr
        );
        // Whole, build metadata included: a fork stamped `5.3.0+galets2` has to
        // report `5.3.0+galets2`, so matching exactly is the point.
        assert_eq!(run.stdout, format!("sturnus {declared}\n"), "{flag} stdout");
    }
}

#[test]
fn version_flag_needs_no_config_and_logs_nothing() {
    // `HOME` is unset by `sturnus()`, so the default config path is unknown,
    // and the cwd is empty: a version query on a box with nothing configured.
    let declared = manifest_version();
    let dir = TempDir::new().expect("tempdir");
    let run = run_to_exit(sturnus().arg("--version").current_dir(dir.path()));
    assert!(run.status.success(), "stderr {:?}", run.stderr);
    assert_eq!(run.stdout, format!("sturnus {declared}\n"));
    assert!(
        run.stderr.is_empty(),
        "--version emitted log output: {:?}",
        run.stderr
    );
}

#[test]
fn version_flag_short_circuits_config_env_file_and_env_dir() {
    // Every one of these paths is broken on purpose: a version query must not
    // touch any of them.
    let declared = manifest_version();
    let dir = TempDir::new().expect("tempdir");
    let missing = dir.path().join("nope.toml");
    let missing = missing.to_str().expect("utf-8 path").to_string();
    let cases: [&[&str]; 3] = [
        &["--version"],
        &["--config", missing.as_str(), "--version"],
        &["--env-file", missing.as_str(), "--version"],
    ];
    for args in cases {
        let run = run_to_exit(sturnus().args(args).current_dir(dir.path()));
        assert!(
            run.status.success(),
            "{args:?} exited {} with stderr {:?}",
            run.status,
            run.stderr
        );
        assert_eq!(
            run.stdout,
            format!("sturnus {declared}\n"),
            "{args:?} stdout"
        );
    }
}

#[test]
fn version_flag_survives_the_containers_builtin_config() {
    // The image entrypoint is `sturnus --config /config.toml`, so
    // `docker run <image> --version` appends the flag after it.
    let declared = manifest_version();
    let run = run_to_exit(sturnus().args(["--config", "/config.toml", "--version"]));
    assert!(run.status.success(), "stderr {:?}", run.stderr);
    assert_eq!(run.stdout, format!("sturnus {declared}\n"));
}

#[test]
fn help_lists_the_version_flag_and_leaves_subcommands_alone() {
    let run = run_to_exit(sturnus().arg("--help"));
    assert!(run.status.success(), "stderr {:?}", run.stderr);
    assert!(run.stdout.contains("--version"), "--help:\n{}", run.stdout);
    assert!(
        run.stdout.contains("-V, --version"),
        "--help does not show -V:\n{}",
        run.stdout
    );

    // The existing subcommand still parses, with its own flag intact.
    let run = run_to_exit(sturnus().args(["healthcheck", "--help"]));
    assert!(run.status.success(), "stderr {:?}", run.stderr);
    assert!(
        run.stdout.contains("--port"),
        "healthcheck --help:\n{}",
        run.stdout
    );
}

#[test]
fn startup_logs_the_version_first_as_a_json_field() {
    let declared = manifest_version();
    let dir = TempDir::new().expect("tempdir");
    let config = write_config(dir.path(), "sturnus.toml", MINIMAL_CONFIG);
    let run = run_until_killed(
        sturnus()
            .args(["--log-format", "auto"])
            .arg("--config")
            .arg(&config)
            .current_dir(dir.path()),
        Duration::from_millis(1500),
    );

    // stdout piped, so `auto` renders JSON: exactly what systemd captures.
    let first = run.first_line();
    let fields = fields_of(first);
    assert_eq!(
        fields["version"].as_str(),
        Some(declared.as_str()),
        "first log line: {first}"
    );

    // The listen address is not knowable before the config is read, so it
    // belongs to the line that binds the socket — and it is still there.
    let binding = run
        .lines()
        .iter()
        .copied()
        .find(|line| line.contains("listening"))
        .unwrap_or_else(|| panic!("no listening line in:\n{}", run.stdout));
    let fields = fields_of(binding);
    assert!(
        fields["addr"].is_string(),
        "listening line lost the address: {binding}"
    );
}

#[test]
fn startup_logs_the_version_first_as_a_pretty_key() {
    let declared = manifest_version();
    let dir = TempDir::new().expect("tempdir");
    let config = write_config(dir.path(), "sturnus.toml", MINIMAL_CONFIG);
    let run = run_until_killed(
        sturnus()
            .args(["--log-format", "pretty"])
            .arg("--config")
            .arg(&config)
            .current_dir(dir.path()),
        Duration::from_millis(1500),
    );

    let first = run.first_line();
    assert!(
        first.contains(&format!("version={declared}"))
            || first.contains(&format!("version=\"{declared}\"")),
        "first pretty line has no version key: {first}"
    );
    assert!(
        !first.starts_with('{'),
        "pretty output rendered JSON: {first}"
    );
}

#[test]
fn a_startup_that_fails_at_config_load_has_still_logged_its_version() {
    let declared = manifest_version();
    let dir = TempDir::new().expect("tempdir");

    // Missing, and unreadable-by-content: both fail at config load, after the
    // version line has been written.
    let missing = dir.path().join("missing.toml");
    let invalid = write_config(dir.path(), "invalid.toml", "listen = 42\n");
    for config in [&missing, &invalid] {
        let run = run_to_exit(
            sturnus()
                .args(["--log-format", "auto"])
                .arg("--config")
                .arg(config),
        );
        assert!(!run.status.success(), "{config:?} should fail to start");
        let first = run.first_line();
        let fields = fields_of(first);
        assert_eq!(
            fields["version"].as_str(),
            Some(declared.as_str()),
            "{config:?} did not name the version first: {first}"
        );
    }
}
