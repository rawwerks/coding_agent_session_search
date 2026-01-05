//! Remote sources management for cass.
//!
//! This module provides functionality for configuring and syncing agent session
//! data from remote machines via SSH. It enables cass to search across conversation
//! history from multiple machines.
//!
//! # Architecture
//!
//! - **config**: Configuration types for defining remote sources
//! - **provenance**: Types for tracking conversation origins
//! - **sync**: Sync engine for pulling sessions from remotes via rsync/SSH
//! - **status** (future): Sync status tracking
//!
//! # Configuration
//!
//! Sources are configured in `~/.config/cass/sources.toml`:
//!
//! ```toml
//! [[sources]]
//! name = "laptop"
//! type = "ssh"
//! host = "user@laptop.local"
//! paths = ["~/.claude/projects", "~/.cursor"]
//! ```
//!
//! # Provenance
//!
//! Each conversation tracks where it came from via [`provenance::Origin`]:
//!
//! ```rust,ignore
//! use coding_agent_search::sources::provenance::{Origin, SourceKind};
//!
//! // Local conversation
//! let local = Origin::local();
//!
//! // Remote conversation
//! let remote = Origin::remote("work-laptop");
//! ```
//!
//! # Syncing
//!
//! The sync engine uses rsync over SSH for efficient delta transfers:
//!
//! ```rust,ignore
//! use coding_agent_search::sources::sync::SyncEngine;
//! use coding_agent_search::sources::config::SourcesConfig;
//!
//! let config = SourcesConfig::load()?;
//! let engine = SyncEngine::new(&data_dir);
//!
//! for source in config.remote_sources() {
//!     let report = engine.sync_source(source)?;
//!     println!("Synced {}: {} files", source.name, report.total_files());
//! }
//! ```
//!
//! # Usage
//!
//! ```rust,ignore
//! use coding_agent_search::sources::config::SourcesConfig;
//!
//! // Load configuration
//! let config = SourcesConfig::load()?;
//!
//! // Iterate remote sources
//! for source in config.remote_sources() {
//!     println!("Source: {} ({})", source.name, source.host.as_deref().unwrap_or("-"));
//! }
//! ```

pub mod config;
pub mod install;
pub mod probe;
pub mod provenance;
pub mod sync;

// Re-export commonly used config types
pub use config::{
    ConfigError, DiscoveredHost, Platform, SourceDefinition, SourcesConfig, SyncSchedule,
    discover_ssh_hosts, get_preset_paths,
};

// Re-export commonly used provenance types
pub use provenance::{LOCAL_SOURCE_ID, Origin, Source, SourceFilter, SourceKind};

// Re-export commonly used sync types
pub use sync::{
    PathSyncResult, SourceSyncInfo, SyncEngine, SyncError, SyncMethod, SyncReport, SyncResult,
    SyncStatus,
};

// Re-export commonly used probe types
pub use probe::{
    CassStatus, DetectedAgent, HostProbeResult, ProbeCache, ResourceInfo, SystemInfo, probe_host,
    probe_hosts_parallel,
};

// Re-export commonly used install types
pub use install::{
    InstallError, InstallMethod, InstallProgress, InstallResult, InstallStage, RemoteInstaller,
};
