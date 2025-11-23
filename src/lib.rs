pub mod config;
pub mod connectors;
pub mod indexer;
pub mod model;
pub mod search;
pub mod storage;
pub mod ui;

use anyhow::Result;
use clap::{CommandFactory, Parser, Subcommand};
use indexer::IndexOptions;
use std::path::PathBuf;

/// Command-line interface.
#[derive(Parser, Debug)]
#[command(
    name = "coding-agent-search",
    version,
    about = "Unified TUI search over coding agent histories"
)]
pub struct Cli {
    /// Path to the SQLite database (defaults to platform data dir)
    #[arg(long)]
    pub db: Option<PathBuf>,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Launch interactive TUI
    Tui {
        /// Render once and exit (headless-friendly)
        #[arg(long, default_value_t = false)]
        once: bool,

        /// Override data dir (matches index --data-dir)
        #[arg(long)]
        data_dir: Option<PathBuf>,
    },
    /// Run indexer
    Index {
        /// Perform full rebuild
        #[arg(long)]
        full: bool,

        /// Watch for changes and reindex automatically
        #[arg(long)]
        watch: bool,

        /// Override data dir (index + db). Defaults to platform data dir.
        #[arg(long)]
        data_dir: Option<PathBuf>,
    },
    /// Generate shell completions to stdout
    Completions {
        #[arg(value_enum)]
        shell: clap_complete::Shell,
    },
    /// Generate man page to stdout
    Man,
}

pub async fn run() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Tui { once, data_dir } => ui::tui::run_tui(data_dir, once),
        Commands::Index {
            full,
            watch,
            data_dir,
        } => run_index_with_data(cli.db, full, watch, data_dir),
        Commands::Completions { shell } => {
            let mut cmd = Cli::command();
            clap_complete::generate(
                shell,
                &mut cmd,
                "coding-agent-search",
                &mut std::io::stdout(),
            );
            Ok(())
        }
        Commands::Man => {
            let cmd = Cli::command();
            let man = clap_mangen::Man::new(cmd);
            let mut out = std::io::stdout();
            man.render(&mut out)?;
            Ok(())
        }
    }
}

fn run_index_with_data(
    db_override: Option<PathBuf>,
    full: bool,
    watch: bool,
    data_dir_override: Option<PathBuf>,
) -> Result<()> {
    let data_dir = data_dir_override.unwrap_or_else(default_data_dir);
    let db_path = db_override.unwrap_or_else(|| data_dir.join("agent_search.db"));
    let opts = IndexOptions {
        full,
        watch,
        db_path,
        data_dir,
    };
    indexer::run_index(opts)
}

pub fn default_db_path() -> PathBuf {
    default_data_dir().join("agent_search.db")
}

pub fn default_data_dir() -> PathBuf {
    directories::ProjectDirs::from("com", "coding-agent-search", "coding-agent-search")
        .expect("project dirs available")
        .data_dir()
        .to_path_buf()
}
