use anyhow::Result;
use console::{style, Term};
use dialoguer::{theme::ColorfulTheme, Confirm, Input, MultiSelect, Select};
use std::io::Write;
use std::path::PathBuf;

use crate::pages::export::{run_pages_export, PathMode};

pub struct PagesWizard {
    // State will go here
}

impl Default for PagesWizard {
    fn default() -> Self {
        Self::new()
    }
}

impl PagesWizard {
    pub fn new() -> Self {
        Self {}
    }

    pub fn run(&self) -> Result<()> {
        let mut term = Term::stdout();
        let theme = ColorfulTheme::default();

        term.clear_screen()?;
        writeln!(term, "{}", style("🔐 cass Pages Export Wizard").bold().cyan())?;
        writeln!(term, "Create an encrypted, searchable web archive of your AI coding agent conversations.")?;
        writeln!(term)?;

        // 1. Content Selection
        writeln!(term, "Step 1 of 7: Content Selection")?;
        
        // Agents
        // TODO: dynamic agent list from DB
        let agents = vec!["Claude Code", "Codex", "Gemini", "Cursor", "Aider"];
        let selected_agents = MultiSelect::with_theme(&theme)
            .with_prompt("Which agents would you like to include?")
            .items(&agents)
            .defaults(&vec![true; agents.len()])
            .interact()?;
        
        let selected_agent_names: Vec<String> = selected_agents
            .iter()
            .map(|&i| agents[i].to_lowercase().replace(" ", "_")) // basic slugify
            .collect();

        if selected_agent_names.is_empty() {
            writeln!(term, "{}", style("⚠ No agents selected. Export cancelled.").red())?;
            return Ok(());
        }

        // Time Range (Simplified for now)
        let time_options = vec!["All time", "Last 30 days", "Last 90 days"];
        let time_selection = Select::with_theme(&theme)
            .with_prompt("Time range:")
            .default(0)
            .items(&time_options)
            .interact()?;
        
        let since = match time_selection {
            1 => Some("-30d".to_string()),
            2 => Some("-90d".to_string()),
            _ => None,
        };

        // Output Directory
        let output_dir_str: String = Input::with_theme(&theme)
            .with_prompt("Output directory")
            .default("cass-export".to_string())
            .interact_text()?;
        
        let output_dir = PathBuf::from(&output_dir_str);
        if !output_dir.exists() {
            std::fs::create_dir_all(&output_dir)?;
        }
        
        // For Phase 1, we just generate the raw export DB inside the dir
        let export_db_path = output_dir.join("export.db");

        writeln!(term, "\n{}", style("Skipping advanced steps for Phase 1 scaffolding...").yellow())?;

        // Confirm
        if !Confirm::with_theme(&theme)
            .with_prompt("Proceed with export?")
            .default(true)
            .interact()?
        {
            return Ok(());
        }

        // Run Export
        run_pages_export(
            None, // Use default DB
            export_db_path,
            Some(selected_agent_names),
            None, // All workspaces
            since,
            None, // Until now
            PathMode::Relative,
            false, // Not dry run
        )?;

        Ok(())
    }
}