use coding_agent_search::ui::components::theme::ThemePalette;
use coding_agent_search::ui::tui::{footer_legend, help_lines};

#[test]
fn help_legend_has_hotkeys() {
    // Simplified footer shows essential keys only
    let short = footer_legend(false);
    assert!(
        short.contains("F1 help"),
        "short footer should show F1 help"
    );
    assert!(
        short.contains("Enter view"),
        "short footer should show Enter view"
    );

    // Expanded footer (when help shown) has more detail
    let long = footer_legend(true);
    assert!(
        long.contains("Esc quit"),
        "long footer should show Esc quit"
    );
    assert!(
        long.contains("Tab focus"),
        "long footer should show Tab focus"
    );
}

/// Convert help lines to a single string for easy searching
fn help_lines_to_string(palette: ThemePalette) -> String {
    help_lines(palette)
        .iter()
        .map(|line| {
            line.spans
                .iter()
                .map(|span| span.content.to_string())
                .collect::<Vec<_>>()
                .join("")
        })
        .collect::<Vec<_>>()
        .join("\n")
}

// =============================================================================
// Help Modal Content Verification Tests
// =============================================================================

#[test]
fn help_modal_has_sources_section() {
    let content = help_lines_to_string(ThemePalette::dark());

    assert!(
        content.contains("Sources (Multi-Machine)"),
        "Help modal should have Sources section header"
    );
    assert!(
        content.contains("F11 cycle source filter"),
        "Sources section should document F11 shortcut"
    );
    assert!(
        content.contains("Shift+F11"),
        "Sources section should document Shift+F11 shortcut"
    );
    assert!(
        content.contains("sources.toml"),
        "Sources section should mention config file"
    );
}

#[test]
fn help_modal_mentions_f11_shortcut() {
    let content = help_lines_to_string(ThemePalette::dark());

    // F11 should be mentioned for source filtering
    assert!(
        content.contains("F11"),
        "Help modal should mention F11 shortcut for source filtering"
    );

    // Should explain what F11 does
    assert!(
        content.contains("source filter") || content.contains("local → remote"),
        "Help modal should explain F11 cycles through source filters"
    );
}

#[test]
fn help_modal_mentions_all_agents() {
    let content = help_lines_to_string(ThemePalette::dark());

    // All 10 supported connectors should be mentioned
    let agents = [
        "Claude",
        "Codex",
        "Gemini",
        "Cline",
        "OpenCode",
        "Amp",
        "Cursor",
        "ChatGPT",
        "Aider",
        "Pi-Agent",
    ];

    for agent in agents {
        assert!(
            content.contains(agent),
            "Help modal should mention {} agent",
            agent
        );
    }
}

#[test]
fn help_modal_line_count_reasonable() {
    let lines = help_lines(ThemePalette::dark());
    let line_count = lines.len();

    // Help should be comprehensive but not overwhelming
    // Current implementation is around 90-120 lines
    assert!(
        line_count >= 50,
        "Help modal should have at least 50 lines for comprehensive help, got {}",
        line_count
    );
    assert!(
        line_count <= 200,
        "Help modal should have at most 200 lines to remain readable, got {}",
        line_count
    );
}

#[test]
fn help_modal_sections_order() {
    let content = help_lines_to_string(ThemePalette::dark());

    // Find positions of key sections to verify logical ordering
    let welcome_pos = content.find("Welcome to CASS");
    let data_locations_pos = content.find("Data Locations");
    let search_pos = content.find("\nSearch\n").or(content.find("Search\n"));
    let filters_pos = content.find("\nFilters\n").or(content.find("Filters\n"));
    let sources_pos = content.find("Sources (Multi-Machine)");
    let navigation_pos = content.find("\nNavigation\n").or(content.find("Navigation\n"));
    let actions_pos = content.find("\nActions\n").or(content.find("Actions\n"));

    // Welcome should come first
    assert!(welcome_pos.is_some(), "Help should have Welcome section");
    assert!(
        data_locations_pos.is_some(),
        "Help should have Data Locations section"
    );
    assert!(search_pos.is_some(), "Help should have Search section");
    assert!(filters_pos.is_some(), "Help should have Filters section");
    assert!(sources_pos.is_some(), "Help should have Sources section");
    assert!(navigation_pos.is_some(), "Help should have Navigation section");
    assert!(actions_pos.is_some(), "Help should have Actions section");

    // Verify logical ordering: Welcome → Data Locations → Search → Filters → Sources
    if let (Some(w), Some(d)) = (welcome_pos, data_locations_pos) {
        assert!(
            w < d,
            "Welcome should come before Data Locations"
        );
    }

    if let (Some(s), Some(f)) = (search_pos, filters_pos) {
        assert!(
            s < f,
            "Search should come before Filters"
        );
    }

    if let (Some(f), Some(src)) = (filters_pos, sources_pos) {
        assert!(
            f < src,
            "Filters should come before Sources"
        );
    }
}

#[test]
fn help_modal_has_layout_diagram() {
    let content = help_lines_to_string(ThemePalette::dark());

    // The help modal should include the ASCII layout diagram
    assert!(
        content.contains("┌─") || content.contains("│"),
        "Help modal should include layout diagram with box drawing characters"
    );
    assert!(
        content.contains("Results"),
        "Layout diagram should show Results pane"
    );
    assert!(
        content.contains("Detail Preview") || content.contains("Preview"),
        "Layout diagram should show Detail/Preview pane"
    );
}

#[test]
fn help_modal_has_search_wildcards() {
    let content = help_lines_to_string(ThemePalette::dark());

    // Search section should document wildcard patterns
    assert!(
        content.contains("foo*"),
        "Help should document prefix wildcard pattern"
    );
    assert!(
        content.contains("*foo"),
        "Help should document suffix wildcard pattern"
    );
}

#[test]
fn help_modal_has_keyboard_shortcuts() {
    let content = help_lines_to_string(ThemePalette::dark());

    // Essential shortcuts should be documented
    assert!(
        content.contains("F1") || content.contains("?"),
        "Help should mention help shortcut"
    );
    assert!(
        content.contains("Esc"),
        "Help should mention Esc key"
    );
    assert!(
        content.contains("Enter"),
        "Help should mention Enter key"
    );
    assert!(
        content.contains("Tab"),
        "Help should mention Tab key"
    );
}

#[test]
fn help_modal_has_theme_info() {
    let content = help_lines_to_string(ThemePalette::dark());

    // Theme switching should be documented
    assert!(
        content.contains("theme") || content.contains("dark/light"),
        "Help should document theme switching"
    );
}

#[test]
fn help_modal_consistent_across_themes() {
    // Content should be the same regardless of theme (only styling differs)
    let dark_content = help_lines_to_string(ThemePalette::dark());
    let light_content = help_lines_to_string(ThemePalette::light());

    // Line counts should be identical
    let dark_lines = help_lines(ThemePalette::dark()).len();
    let light_lines = help_lines(ThemePalette::light()).len();
    assert_eq!(
        dark_lines, light_lines,
        "Help modal should have same line count in dark and light themes"
    );

    // Key content should be present in both
    assert!(
        dark_content.contains("Welcome to CASS") && light_content.contains("Welcome to CASS"),
        "Welcome text should be in both themes"
    );
    assert!(
        dark_content.contains("Sources (Multi-Machine)")
            && light_content.contains("Sources (Multi-Machine)"),
        "Sources section should be in both themes"
    );
}
