use ratatui::layout::Alignment;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph};

use crate::ui::components::theme::ThemePalette;
use crate::ui::data::InputMode;
use ratatui::widgets::Wrap;

pub fn search_bar(
    query: &str,
    palette: ThemePalette,
    input_mode: InputMode,
    mode_label: &str,
    chips: Vec<Span<'static>>,
) -> Paragraph<'static> {
    let in_query_mode = matches!(input_mode, InputMode::Query);

    // Title changes based on input mode to clearly indicate modal state
    let (title_text, title_style) = match input_mode {
        InputMode::Query => (format!("Search · {mode_label}"), palette.title()),
        InputMode::Agent => (
            "▸ Filter: Agent (Tab=complete, Enter=apply, Esc=cancel)".to_string(),
            Style::default()
                .fg(palette.accent_alt)
                .add_modifier(Modifier::BOLD),
        ),
        InputMode::Workspace => (
            "▸ Filter: Workspace (Enter=apply, Esc=cancel)".to_string(),
            Style::default()
                .fg(palette.accent_alt)
                .add_modifier(Modifier::BOLD),
        ),
        InputMode::CreatedFrom => (
            "▸ Filter: From Date (-7d, yesterday, 2024-11-25)".to_string(),
            Style::default()
                .fg(palette.accent_alt)
                .add_modifier(Modifier::BOLD),
        ),
        InputMode::CreatedTo => (
            "▸ Filter: To Date (-7d, yesterday, now)".to_string(),
            Style::default()
                .fg(palette.accent_alt)
                .add_modifier(Modifier::BOLD),
        ),
    };
    let title = Span::styled(title_text, title_style);

    let style = if in_query_mode {
        Style::default().fg(palette.accent)
    } else {
        Style::default().fg(palette.accent_alt)
    };

    let border_style = match input_mode {
        InputMode::Query => Style::default().fg(palette.accent_alt),
        _ => Style::default()
            .fg(palette.accent)
            .add_modifier(Modifier::BOLD),
    };

    let mut first_line = chips;
    if !first_line.is_empty() {
        first_line.push(Span::raw(" "));
    }
    // Add cursor indicator (visible in all modes)
    let cursor = "▎";
    first_line.push(Span::styled(format!("/ {}{}", query, cursor), style));

    // Tips line changes based on mode
    let tips_line = if in_query_mode {
        Line::from(vec![
            Span::styled("F1", Style::default().fg(palette.accent)),
            Span::raw(" help  "),
            Span::styled("F3", Style::default().fg(palette.hint)),
            Span::raw(" agent  "),
            Span::styled("F4", Style::default().fg(palette.hint)),
            Span::raw(" workspace  "),
            Span::styled("F5", Style::default().fg(palette.hint)),
            Span::raw(" time  "),
            Span::styled("Ctrl+Del", Style::default().fg(palette.hint)),
            Span::raw(" clear filters"),
        ])
    } else {
        // Simplified tips when in filter mode
        Line::from(vec![
            Span::styled("Enter", Style::default().fg(palette.accent)),
            Span::raw(" apply  "),
            Span::styled("Esc", Style::default().fg(palette.hint)),
            Span::raw(" cancel  "),
            Span::styled("Backspace", Style::default().fg(palette.hint)),
            Span::raw(" delete"),
        ])
    };

    let body = vec![Line::from(first_line), tips_line];

    Paragraph::new(body)
        .block(
            Block::default()
                .title(title)
                .borders(Borders::ALL)
                .border_style(border_style),
        )
        .style(Style::default())
        .alignment(Alignment::Left)
        .wrap(Wrap { trim: true })
}
