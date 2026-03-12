//! Samo — self-driving Postgres agent and psql-compatible terminal.
//!
//! This is the CLI entry point. It parses psql-compatible flags and
//! samo-specific options, then dispatches to the appropriate subsystem.

use clap::Parser;

/// Build-time git commit hash injected by `build.rs`.
const GIT_HASH: &str = env!("SAMO_GIT_HASH");

/// Compile-time version string: `0.1.0-dev`.
const PKG_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Assemble a version string like `0.1.0-dev (abc1234)`.
fn version_long() -> String {
    format!("{PKG_VERSION} ({GIT_HASH})")
}

// ---------------------------------------------------------------------------
// Autonomy levels (samo-specific)
// ---------------------------------------------------------------------------

/// Autonomy level for the agent subsystem.
#[derive(Clone, Debug, Default, clap::ValueEnum)]
enum Autonomy {
    /// Read-only: observe, diagnose, report. Zero writes.
    #[default]
    Observe,
    /// Propose actions, human confirms before execution.
    Supervised,
    /// Act autonomously within policy and DB permissions.
    Auto,
}

// ---------------------------------------------------------------------------
// CLI definition
// ---------------------------------------------------------------------------

/// Samo — self-driving Postgres agent and psql-compatible terminal.
///
/// A psql-compatible interface with built-in AI and autonomous
/// database health management.
#[derive(Parser, Debug)]
#[command(
    name = "samo",
    version = env!("CARGO_PKG_VERSION"),
    about = "Self-driving Postgres agent and psql-compatible terminal",
    long_about = None,
    // Disable auto-generated -h so we can use it for --host (psql compat).
    disable_help_flag = true,
)]
#[allow(clippy::struct_excessive_bools)]
struct Cli {
    /// Print help information.
    #[arg(long, action = clap::ArgAction::Help)]
    help: Option<bool>,

    // -- Positional arguments (psql-compatible order) -----------------------
    /// Database name to connect to.
    #[arg(value_name = "DBNAME")]
    dbname_pos: Option<String>,

    /// Username (positional).
    #[arg(value_name = "USER")]
    user_pos: Option<String>,

    /// Hostname (positional).
    #[arg(value_name = "HOST")]
    host_pos: Option<String>,

    /// Port (positional).
    #[arg(value_name = "PORT")]
    port_pos: Option<String>,

    // -- Connection flags ---------------------------------------------------
    /// Database server host or socket directory.
    #[arg(short = 'h', long)]
    host: Option<String>,

    /// Database server port number.
    #[arg(short = 'p', long)]
    port: Option<u16>,

    /// Database user name.
    #[arg(short = 'U', long)]
    username: Option<String>,

    /// Database name.
    #[arg(short = 'd', long)]
    dbname: Option<String>,

    /// Force password prompt.
    #[arg(short = 'W', long)]
    password: bool,

    // -- Common psql flags (stubs) ------------------------------------------
    /// Run a single command (SQL or backslash) and exit.
    #[arg(short = 'c', long)]
    command: Option<String>,

    /// Execute commands from file, then exit.
    #[arg(short = 'f', long)]
    file: Option<String>,

    /// Do not read startup file (~/.psqlrc / ~/.samorc).
    #[arg(short = 'X', long = "no-psqlrc")]
    no_psqlrc: bool,

    /// Unaligned table output mode.
    #[arg(short = 'A', long = "no-align")]
    no_align: bool,

    /// Print rows only (tuples only).
    #[arg(short = 't', long = "tuples-only")]
    tuples_only: bool,

    /// Set printing option (like `\pset`).
    #[arg(short = 'P', long, value_name = "VAR[=ARG]")]
    pset: Option<String>,

    /// Send query results to file (or pipe).
    #[arg(short = 'o', long)]
    output: Option<String>,

    /// Echo queries that samo generates internally.
    #[arg(short = 'E', long = "echo-hidden")]
    echo_hidden: bool,

    /// Echo all input from script.
    #[arg(short = 'e', long = "echo-queries")]
    echo_queries: bool,

    /// Echo failed commands' error messages.
    #[arg(short = 'b', long = "echo-errors")]
    echo_errors: bool,

    /// Run in quiet mode (suppress informational messages).
    #[arg(short = 'q', long)]
    quiet: bool,

    /// Single-line mode: newline terminates a SQL command.
    #[arg(short = 'S', long = "single-line")]
    single_line: bool,

    /// Single-transaction mode: wrap all commands in BEGIN/COMMIT.
    #[arg(short = '1', long = "single-transaction")]
    single_transaction: bool,

    /// CSV output format.
    #[arg(long)]
    csv: bool,

    /// JSON output format.
    #[arg(long)]
    json: bool,

    /// Enable debug output.
    #[arg(short = 'D', long)]
    debug: bool,

    // -- Samo-specific flags ------------------------------------------------
    /// Enable text-to-SQL mode: translate natural language to SQL.
    #[arg(long)]
    text2sql: bool,

    /// Show query execution plan before running.
    #[arg(long)]
    plan: bool,

    /// Skip confirmation prompts for AI-generated queries (use with care).
    #[arg(long)]
    yolo: bool,

    /// Launch in observe-only mode (read-only monitoring).
    #[arg(long)]
    observe: bool,

    /// Set agent autonomy level.
    #[arg(long, value_enum, default_value_t = Autonomy::Observe)]
    autonomy: Autonomy,
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _cli = Cli::parse();

    // For now, just announce that the binary works.
    println!("samo {} - not yet connected", version_long());

    Ok(())
}
