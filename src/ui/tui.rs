//! Ratatui-based interface wired to Tantivy search.

use anyhow::Result;
use chrono::{DateTime, Utc};
use crossterm::event::{self, Event, KeyCode};
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use crossterm::{ExecutableCommand, execute};
use ratatui::prelude::*;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, List, ListItem, ListState, Paragraph, Tabs, Wrap};
use std::io;
use std::process::Command as StdCommand;
use std::time::{Duration, Instant};

use crate::default_data_dir;
use crate::model::types::MessageRole;
use crate::search::query::{SearchClient, SearchFilters, SearchHit};
use crate::search::tantivy::index_dir;
use crate::ui::components::theme::ThemePalette;
use crate::ui::components::widgets::search_bar;
use crate::ui::data::{ConversationView, load_conversation, role_style};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum InputMode {
    Query,
    Agent,
    Workspace,
    CreatedFrom,
    CreatedTo,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DetailTab {
    Messages,
    Snippets,
    Raw,
}

fn render_help_overlay(frame: &mut Frame, palette: ThemePalette) {
    let area = frame.area();
    let popup_area = centered_rect(60, 50, area);
    let sections: Vec<Line> = vec![
        Line::from(vec![
            Span::styled("Search", palette.title()),
            Span::raw(": type to live-search; / focuses query"),
        ]),
        Line::from(vec![
            Span::styled("Filters", palette.title()),
            Span::raw(
                ": a agent | w workspace | f from | t to | x clear; pills show below the bar",
            ),
        ]),
        Line::from(vec![
            Span::styled("Navigate", palette.title()),
            Span::raw(": j/k or arrows move • PgUp/PgDn paginate • Tab cycles detail tabs"),
        ]),
        Line::from(vec![
            Span::styled("Detail", palette.title()),
            Span::raw(
                ": Messages show full thread with role colors • Snippets / Raw tabs available",
            ),
        ]),
        Line::from(vec![
            Span::styled("Actions", palette.title()),
            Span::raw(": o open hit in $EDITOR • h toggle theme • ? toggle this help"),
        ]),
        Line::from(vec![
            Span::styled("Quit", palette.title()),
            Span::raw(": q or esc"),
        ]),
        Line::from(""),
        Line::from(vec![
            Span::styled("Tip", palette.title()),
            Span::raw(
                ": start with a query then narrow with filters; watch row shows active filters.",
            ),
        ]),
    ];

    let block = Block::default()
        .title(Span::styled("Help / Onboarding", palette.title()))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(palette.accent));
    frame.render_widget(
        Paragraph::new(sections)
            .block(block)
            .wrap(Wrap { trim: true }),
        popup_area,
    );
}

fn centered_rect(percent_x: u16, percent_y: u16, r: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints(
            [
                Constraint::Percentage((100 - percent_y) / 2),
                Constraint::Percentage(percent_y),
                Constraint::Percentage((100 - percent_y) / 2),
            ]
            .as_ref(),
        )
        .split(r);

    let horizontal = Layout::default()
        .direction(Direction::Horizontal)
        .constraints(
            [
                Constraint::Percentage((100 - percent_x) / 2),
                Constraint::Percentage(percent_x),
                Constraint::Percentage((100 - percent_x) / 2),
            ]
            .as_ref(),
        )
        .split(popup_layout[1]);

    horizontal[1]
}

fn highlight_terms_owned_with_style(
    text: String,
    query: &str,
    palette: ThemePalette,
    base: Style,
) -> Line<'static> {
    let owned = text;
    let mut spans = Vec::new();
    if query.trim().is_empty() {
        spans.push(Span::styled(owned, base));
        let line: Line = spans.into();
        return unsafe { std::mem::transmute::<Line, Line<'static>>(line) };
    }
    let lower = owned.to_lowercase();
    let q = query.to_lowercase();
    let mut idx = 0;
    while let Some(pos) = lower[idx..].find(&q) {
        let start = idx + pos;
        if start > idx {
            spans.push(Span::styled(owned[idx..start].to_string(), base));
        }
        let end = start + q.len();
        spans.push(Span::styled(
            owned[start..end].to_string(),
            base.patch(
                Style::default()
                    .fg(palette.accent)
                    .add_modifier(Modifier::BOLD),
            ),
        ));
        idx = end;
    }
    if idx < owned.len() {
        spans.push(Span::styled(owned[idx..].to_string(), base));
    }
    let line: Line = spans.into();
    unsafe { std::mem::transmute::<Line, Line<'static>>(line) }
}

fn format_ts(ms: i64) -> String {
    DateTime::<Utc>::from_timestamp_millis(ms)
        .map(|dt: DateTime<Utc>| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        .unwrap_or_else(|| ms.to_string())
}

pub fn footer_legend(show_help: bool) -> &'static str {
    if show_help {
        "q/esc quit • arrows/PgUp/PgDn navigate • / focus query • a agent • w workspace • f from • t to • x clear • A/W/F clear-one • tab detail • h theme • o open"
    } else {
        "?/hide help | a agent | w workspace | f from | t to | x clear | tab detail | h theme | o open | q quit"
    }
}

pub fn run_tui(data_dir_override: Option<std::path::PathBuf>, once: bool) -> Result<()> {
    if once
        && std::env::var("TUI_HEADLESS")
            .map(|v| v == "1")
            .unwrap_or(false)
    {
        return run_tui_headless(data_dir_override);
    }

    let mut stdout = io::stdout();
    enable_raw_mode()?;
    stdout.execute(EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let data_dir = data_dir_override.unwrap_or_else(default_data_dir);
    let index_path = index_dir(&data_dir)?;
    let db_path = default_db_path_for(&data_dir);
    let search_client = SearchClient::open(&index_path, Some(&db_path))?;
    let index_ready = search_client.is_some();
    let mut status = if index_ready {
        format!(
            "Index ready at {} - type to search (q/esc to quit)",
            index_path.display()
        )
    } else {
        format!(
            "Index not present at {}. Run `coding-agent-search index --full` then reopen TUI.",
            index_path.display()
        )
    };

    let mut query = String::new();
    let mut filters = SearchFilters::default();
    let mut input_mode = InputMode::Query;
    let mut input_buffer = String::new();
    let page_size: usize = 20;
    let mut page: usize = 0;
    let mut results: Vec<SearchHit> = Vec::new();
    let mut last_tick = Instant::now();
    let tick_rate = Duration::from_millis(200);
    let debounce = Duration::from_millis(150);
    let mut dirty_since: Option<Instant> = None;

    let mut selected: Option<usize> = None;
    let mut detail_tab = DetailTab::Messages;
    let mut theme_dark = true;
    // Show onboarding overlay on first launch; user can dismiss with '?'.
    let mut show_help = true;
    let mut last_error: Option<String> = None;
    let mut cached_detail: Option<(String, ConversationView)> = None;
    let mut last_query = String::new();
    let mut needs_draw = true;
    let editor_cmd = std::env::var("EDITOR").unwrap_or_else(|_| "vi".into());
    let editor_line_flag = std::env::var("EDITOR_LINE_FLAG").unwrap_or_else(|_| "+".into());

    loop {
        if needs_draw {
            terminal.draw(|f| {
                let palette = if theme_dark {
                    ThemePalette::dark()
                } else {
                    ThemePalette::light()
                };

                let chunks = Layout::default()
                    .direction(Direction::Vertical)
                    .margin(1)
                    .constraints(
                        [
                            Constraint::Length(3), // search bar
                            Constraint::Length(1), // filter pills
                            Constraint::Min(0),    // results + detail
                            Constraint::Length(1), // footer
                        ]
                        .as_ref(),
                    )
                    .split(f.area());

                let bar_text = match input_mode {
                    InputMode::Query => query.as_str().to_string(),
                    InputMode::Agent => format!("[agent] {}", input_buffer),
                    InputMode::Workspace => format!("[workspace] {}", input_buffer),
                    InputMode::CreatedFrom => format!("[from ts ms] {}", input_buffer),
                    InputMode::CreatedTo => format!("[to ts ms] {}", input_buffer),
                };
                let sb = search_bar(&bar_text, palette, matches!(input_mode, InputMode::Query));
                f.render_widget(sb, chunks[0]);

                // Filter pills row
                let mut pill_spans = Vec::new();
                if !filters.agents.is_empty() {
                    pill_spans.push(Span::styled(
                        format!(
                            "[a] agent:{}",
                            filters.agents.iter().cloned().collect::<Vec<_>>().join("|")
                        ),
                        Style::default()
                            .fg(palette.accent_alt)
                            .add_modifier(Modifier::BOLD),
                    ));
                    pill_spans.push(Span::raw("  "));
                }
                if !filters.workspaces.is_empty() {
                    pill_spans.push(Span::styled(
                        format!(
                            "[w] ws:{}",
                            filters
                                .workspaces
                                .iter()
                                .cloned()
                                .collect::<Vec<_>>()
                                .join("|")
                        ),
                        Style::default().fg(palette.accent_alt),
                    ));
                    pill_spans.push(Span::raw("  "));
                }
                if filters.created_from.is_some() || filters.created_to.is_some() {
                    pill_spans.push(Span::styled(
                        format!(
                            "[f/t] time:{:?}->{:?}",
                            filters.created_from, filters.created_to
                        ),
                        Style::default().fg(palette.accent_alt),
                    ));
                }
                if pill_spans.is_empty() {
                    pill_spans.push(Span::styled(
                        "filters: none (press a/w/f/t)",
                        Style::default().fg(palette.hint),
                    ));
                }
                let pill_para = Paragraph::new(Line::from(pill_spans));
                f.render_widget(pill_para, chunks[1]);

                let items: Vec<ListItem> = if results.is_empty() {
                    vec![ListItem::new("No results yet - start typing to search.")]
                } else {
                    results
                        .iter()
                        .map(|hit| {
                            let title = if hit.title.is_empty() {
                                "(untitled)"
                            } else {
                                hit.title.as_str()
                            };
                            let header = Line::from(vec![
                                Span::raw(format!("{:.2} ", hit.score)),
                                Span::styled(title, Style::default().fg(palette.accent)),
                                Span::raw(format!(" [{}]", hit.agent)),
                            ]);
                            let location = if hit.workspace.is_empty() {
                                hit.source_path.clone()
                            } else {
                                format!("{} ({})", hit.source_path, hit.workspace)
                            };
                            let body = Line::from(format!("{location} • {}", hit.snippet));
                            ListItem::new(vec![header, body])
                        })
                        .collect()
                };
                let list_block = Block::default().title("Results").borders(Borders::ALL);

                let mut list_state = ListState::default();
                list_state.select(selected);

                let split = Layout::default()
                    .direction(Direction::Horizontal)
                    .constraints([Constraint::Percentage(60), Constraint::Percentage(40)].as_ref())
                    .split(chunks[2]);

                f.render_stateful_widget(
                    List::new(items).block(list_block),
                    split[0],
                    &mut list_state,
                );

                let detail_area = split[1];
                if let Some(hit) = selected.and_then(|idx| results.get(idx)) {
                    let tabs = ["Messages", "Snippets", "Raw"];
                    let tab_titles: Vec<Line> = tabs
                        .iter()
                        .map(|t| Line::from(Span::styled(*t, palette.title())))
                        .collect();
                    let tab_widget = Tabs::new(tab_titles)
                        .select(match detail_tab {
                            DetailTab::Messages => 0,
                            DetailTab::Snippets => 1,
                            DetailTab::Raw => 2,
                        })
                        .highlight_style(Style::default().fg(palette.accent));

                    let mut meta_lines = Vec::new();
                    meta_lines.push(Line::from(vec![
                        Span::styled("Title: ", palette.title()),
                        Span::raw(hit.title.clone()),
                    ]));
                    meta_lines.push(Line::from(vec![
                        Span::styled("Agent: ", Style::default().fg(palette.hint)),
                        Span::raw(hit.agent.clone()),
                    ]));
                    meta_lines.push(Line::from(vec![
                        Span::styled("Workspace: ", Style::default().fg(palette.hint)),
                        Span::raw(if hit.workspace.is_empty() {
                            "(none)".into()
                        } else {
                            hit.workspace.clone()
                        }),
                    ]));
                    meta_lines.push(Line::from(vec![
                        Span::styled("Source: ", Style::default().fg(palette.hint)),
                        Span::raw(hit.source_path.clone()),
                    ]));
                    meta_lines.push(Line::from(format!("Score: {:.2}", hit.score)));
                    meta_lines.push(Line::from(""));

                    let detail = if cached_detail
                        .as_ref()
                        .map(|(p, _)| p == &hit.source_path)
                        .unwrap_or(false)
                    {
                        cached_detail.as_ref().map(|(_, d)| d.clone())
                    } else {
                        let loaded = load_conversation(&db_path, &hit.source_path).ok().flatten();
                        if let Some(d) = &loaded {
                            cached_detail = Some((hit.source_path.clone(), d.clone()));
                        }
                        loaded
                    };

                    let content_para = match detail_tab {
                        DetailTab::Messages => {
                            if let Some(full) = detail {
                                let mut lines = Vec::new();
                                for msg in full.messages {
                                    let role_label = match msg.role {
                                        MessageRole::User => "you",
                                        MessageRole::Agent => "agent",
                                        MessageRole::Tool => "tool",
                                        MessageRole::System => "system",
                                        MessageRole::Other(ref r) => r.as_str(),
                                    };
                                    let ts = msg
                                        .created_at
                                        .map(|t| format!(" ({})", format_ts(t)))
                                        .unwrap_or_default();
                                    lines.push(Line::from(vec![
                                        Span::styled(
                                            format!("[{role_label}]"),
                                            role_style(&msg.role, palette),
                                        ),
                                        Span::raw(ts),
                                    ]));
                                    let mut in_code = false;
                                    for line_text in msg.content.lines() {
                                        if line_text.trim_start().starts_with("```") {
                                            in_code = !in_code;
                                            lines.push(Line::from(Span::styled(
                                                line_text.to_string(),
                                                Style::default().fg(palette.hint),
                                            )));
                                            continue;
                                        }
                                        let base = if in_code {
                                            Style::default().bg(palette.surface)
                                        } else {
                                            Style::default()
                                        };
                                        let rendered = highlight_terms_owned_with_style(
                                            line_text.to_string(),
                                            &last_query,
                                            palette,
                                            base,
                                        );
                                        lines.push(rendered);
                                    }
                                    lines.push(Line::from(""));
                                }
                                if lines.is_empty() {
                                    Paragraph::new("No messages")
                                        .style(Style::default().fg(palette.hint))
                                } else {
                                    Paragraph::new(lines).wrap(Wrap { trim: true })
                                }
                            } else {
                                Paragraph::new(hit.content.clone()).wrap(Wrap { trim: true })
                            }
                        }
                        DetailTab::Snippets => {
                            if let Some(full) = detail {
                                let mut lines = Vec::new();
                                for (msg_idx, msg) in full.messages.iter().enumerate() {
                                    for snip in &msg.snippets {
                                        let file = snip
                                            .file_path
                                            .as_ref()
                                            .map(|p| p.to_string_lossy().to_string())
                                            .unwrap_or_else(|| "<unknown file>".into());
                                        let range = match (snip.start_line, snip.end_line) {
                                            (Some(s), Some(e)) => format!("{s}-{e}"),
                                            (Some(s), None) => s.to_string(),
                                            _ => "-".into(),
                                        };
                                        lines.push(Line::from(vec![
                                            Span::styled(file, palette.title()),
                                            Span::raw(format!(":{range} ")),
                                            Span::styled(
                                                format!("msg#{msg_idx} "),
                                                role_style(&msg.role, palette),
                                            ),
                                        ]));
                                        if let Some(text) = &snip.snippet_text {
                                            for l in text.lines() {
                                                lines.push(Line::from(Span::raw(format!("  {l}"))));
                                            }
                                        }
                                        lines.push(Line::from(""));
                                    }
                                }
                                if lines.is_empty() {
                                    Paragraph::new("No snippets attached.")
                                        .style(Style::default().fg(palette.hint))
                                } else {
                                    Paragraph::new(lines).wrap(Wrap { trim: true })
                                }
                            } else {
                                Paragraph::new("No snippets loaded")
                                    .style(Style::default().fg(palette.hint))
                            }
                        }
                        DetailTab::Raw => {
                            if let Some(full) = detail {
                                let meta = serde_json::to_string_pretty(&full.convo.metadata_json)
                                    .unwrap_or_else(|_| "<invalid metadata>".into());
                                let mut text = String::new();
                                text.push_str(&format!(
                                    "Path: {}\n",
                                    full.convo.source_path.display()
                                ));
                                if let Some(ws) = &full.workspace {
                                    text.push_str(&format!("Workspace: {}\n", ws.path.display()));
                                }
                                if let Some(ext) = &full.convo.external_id {
                                    text.push_str(&format!("External ID: {ext}\n"));
                                }
                                text.push_str("Metadata:\n");
                                text.push_str(&meta);
                                Paragraph::new(text).wrap(Wrap { trim: true })
                            } else {
                                Paragraph::new(format!("Path: {}", hit.source_path))
                                    .wrap(Wrap { trim: true })
                            }
                        }
                    }
                    .block(Block::default().title("Detail").borders(Borders::ALL));

                    let layout = Layout::default()
                        .direction(Direction::Vertical)
                        .constraints(
                            [
                                Constraint::Length(2),
                                Constraint::Length(meta_lines.len() as u16 + 2),
                                Constraint::Min(3),
                            ]
                            .as_ref(),
                        )
                        .split(detail_area);

                    f.render_widget(tab_widget, layout[0]);
                    f.render_widget(Paragraph::new(meta_lines), layout[1]);
                    f.render_widget(content_para, layout[2]);
                } else {
                    f.render_widget(
                        Paragraph::new("Select a result to view details")
                            .block(Block::default().title("Detail").borders(Borders::ALL)),
                        detail_area,
                    );
                }

                let footer = Paragraph::new(format!("{} | {}", status, footer_legend(show_help)));
                f.render_widget(footer, chunks[3]);

                if show_help {
                    render_help_overlay(f, palette);
                }
            })?;
            needs_draw = false;
        }

        let timeout = if needs_draw {
            Duration::from_millis(0)
        } else {
            tick_rate
                .checked_sub(last_tick.elapsed())
                .unwrap_or_else(|| Duration::from_millis(0))
        };

        if crossterm::event::poll(timeout)?
            && let Event::Key(key) = event::read()?
        {
            needs_draw = true;
            match input_mode {
                InputMode::Query => match key.code {
                    KeyCode::Char('q') | KeyCode::Esc => break,
                    KeyCode::Down | KeyCode::Char('j') => {
                        if let Some(idx) = selected {
                            if idx + 1 < results.len() {
                                selected = Some(idx + 1);
                            }
                        } else if !results.is_empty() {
                            selected = Some(0);
                        }
                    }
                    KeyCode::Up | KeyCode::Char('k') => {
                        if let Some(idx) = selected
                            && idx > 0
                        {
                            selected = Some(idx - 1);
                        }
                    }
                    KeyCode::Char('a') => {
                        input_mode = InputMode::Agent;
                        input_buffer.clear();
                        status = "Agent filter: type slug, Enter=apply, Esc=cancel".to_string();
                    }
                    KeyCode::Char('w') => {
                        input_mode = InputMode::Workspace;
                        input_buffer.clear();
                        status = "Workspace filter: type path fragment, Enter=apply, Esc=cancel"
                            .to_string();
                    }
                    KeyCode::Char('f') => {
                        input_mode = InputMode::CreatedFrom;
                        input_buffer.clear();
                        status =
                            "Created-from (ms since epoch): Enter=apply, Esc=cancel".to_string();
                    }
                    KeyCode::Char('t') => {
                        input_mode = InputMode::CreatedTo;
                        input_buffer.clear();
                        status = "Created-to (ms since epoch): Enter=apply, Esc=cancel".to_string();
                    }
                    KeyCode::Char('x') => {
                        filters = SearchFilters::default();
                        page = 0;
                        status = "Filters cleared".to_string();
                        dirty_since = Some(Instant::now());
                        needs_draw = true;
                    }
                    KeyCode::Char('A') => {
                        filters.agents.clear();
                        page = 0;
                        status = "Agent filter cleared".to_string();
                        dirty_since = Some(Instant::now());
                        needs_draw = true;
                    }
                    KeyCode::Char('W') => {
                        filters.workspaces.clear();
                        page = 0;
                        status = "Workspace filter cleared".to_string();
                        dirty_since = Some(Instant::now());
                        needs_draw = true;
                    }
                    KeyCode::Char('F') => {
                        filters.created_from = None;
                        filters.created_to = None;
                        page = 0;
                        status = "Time filter cleared".to_string();
                        dirty_since = Some(Instant::now());
                        needs_draw = true;
                    }
                    KeyCode::PageDown | KeyCode::Char(']') => {
                        page = page.saturating_add(1);
                    }
                    KeyCode::PageUp | KeyCode::Char('[') => {
                        page = page.saturating_sub(1);
                    }
                    KeyCode::Char('h') => {
                        theme_dark = !theme_dark;
                        status = if theme_dark {
                            "Theme: dark".to_string()
                        } else {
                            "Theme: light".to_string()
                        };
                    }
                    KeyCode::Char('o') => {
                        if let Some(hit) = selected.and_then(|idx| results.get(idx)) {
                            let path = &hit.source_path;
                            let line_hint = hit
                                .snippet
                                .find("line ")
                                .and_then(|i| hit.snippet[i + 5..].split_whitespace().next())
                                .and_then(|s| s.parse::<usize>().ok());
                            let mut cmd = StdCommand::new(&editor_cmd);
                            if let Some(line) = line_hint {
                                cmd.arg(format!("{}{}", editor_line_flag, line));
                            }
                            let _ = cmd.arg(path).status();
                        }
                    }
                    KeyCode::Char('E') => {
                        input_mode = InputMode::Query;
                        status = "Set editor command (current: $EDITOR) not implemented; set env EDITOR/EDITOR_LINE_FLAG before launch."
                            .to_string();
                    }
                    KeyCode::Char('?') => {
                        show_help = !show_help;
                    }
                    KeyCode::Tab => {
                        detail_tab = match detail_tab {
                            DetailTab::Messages => DetailTab::Snippets,
                            DetailTab::Snippets => DetailTab::Raw,
                            DetailTab::Raw => DetailTab::Messages,
                        };
                    }
                    KeyCode::Char(c) => {
                        query.push(c);
                        page = 0;
                        selected = None;
                        dirty_since = Some(Instant::now());
                    }
                    KeyCode::Backspace => {
                        query.pop();
                        page = 0;
                        selected = None;
                        dirty_since = Some(Instant::now());
                    }
                    _ => {}
                },
                InputMode::Agent => match key.code {
                    KeyCode::Esc => {
                        input_mode = InputMode::Query;
                        input_buffer.clear();
                        status = "Agent filter cancelled".to_string();
                    }
                    KeyCode::Enter => {
                        filters.agents.clear();
                        if !input_buffer.trim().is_empty() {
                            filters.agents.insert(input_buffer.trim().to_string());
                        }
                        page = 0;
                        input_mode = InputMode::Query;
                        selected = None;
                        status = format!(
                            "Agent filter set to {}",
                            filters
                                .agents
                                .iter()
                                .cloned()
                                .collect::<Vec<_>>()
                                .join(", ")
                        );
                        input_buffer.clear();
                        dirty_since = Some(Instant::now());
                    }
                    KeyCode::Backspace => {
                        input_buffer.pop();
                    }
                    KeyCode::Char(c) => input_buffer.push(c),
                    _ => {}
                },
                InputMode::Workspace => match key.code {
                    KeyCode::Esc => {
                        input_mode = InputMode::Query;
                        input_buffer.clear();
                        status = "Workspace filter cancelled".to_string();
                    }
                    KeyCode::Enter => {
                        filters.workspaces.clear();
                        if !input_buffer.trim().is_empty() {
                            filters.workspaces.insert(input_buffer.trim().to_string());
                        }
                        page = 0;
                        input_mode = InputMode::Query;
                        selected = None;
                        status = format!(
                            "Workspace filter set to {}",
                            filters
                                .workspaces
                                .iter()
                                .cloned()
                                .collect::<Vec<_>>()
                                .join(", ")
                        );
                        input_buffer.clear();
                        dirty_since = Some(Instant::now());
                    }
                    KeyCode::Backspace => {
                        input_buffer.pop();
                    }
                    KeyCode::Char(c) => input_buffer.push(c),
                    _ => {}
                },
                InputMode::CreatedFrom => match key.code {
                    KeyCode::Esc => {
                        input_mode = InputMode::Query;
                        input_buffer.clear();
                        status = "From timestamp cancelled".to_string();
                    }
                    KeyCode::Enter => {
                        filters.created_from = input_buffer.trim().parse::<i64>().ok();
                        page = 0;
                        input_mode = InputMode::Query;
                        selected = None;
                        status = format!(
                            "created_from={:?}, created_to={:?}",
                            filters.created_from, filters.created_to
                        );
                        input_buffer.clear();
                        dirty_since = Some(Instant::now());
                    }
                    KeyCode::Backspace => {
                        input_buffer.pop();
                    }
                    KeyCode::Char(c) => input_buffer.push(c),
                    _ => {}
                },
                InputMode::CreatedTo => match key.code {
                    KeyCode::Esc => {
                        input_mode = InputMode::Query;
                        input_buffer.clear();
                        status = "To timestamp cancelled".to_string();
                    }
                    KeyCode::Enter => {
                        filters.created_to = input_buffer.trim().parse::<i64>().ok();
                        page = 0;
                        input_mode = InputMode::Query;
                        selected = None;
                        status = format!(
                            "created_from={:?}, created_to={:?}",
                            filters.created_from, filters.created_to
                        );
                        input_buffer.clear();
                        dirty_since = Some(Instant::now());
                    }
                    KeyCode::Backspace => {
                        input_buffer.pop();
                    }
                    KeyCode::Char(c) => input_buffer.push(c),
                    _ => {}
                },
            }
        }

        if last_tick.elapsed() >= tick_rate {
            if let Some(client) = &search_client {
                let should_search = dirty_since
                    .map(|t| t.elapsed() >= debounce)
                    .unwrap_or(false);

                if should_search && !query.trim().is_empty() {
                    last_query = query.clone();
                    match client.search(&query, filters.clone(), page_size, page * page_size) {
                        Ok(hits) => {
                            dirty_since = None;
                            last_error = None;
                            if hits.is_empty() && page > 0 {
                                page = page.saturating_sub(1);
                                selected = None;
                                needs_draw = true;
                            } else {
                                results = hits;
                                status = format!(
                                    "Page {} | filters: a={:?} w={:?} t=({:?},{:?})",
                                    page + 1,
                                    filters.agents,
                                    filters.workspaces,
                                    filters.created_from,
                                    filters.created_to
                                );
                                if !results.is_empty() && selected.is_none() {
                                    selected = Some(0);
                                } else if let Some(idx) = selected
                                    && idx >= results.len()
                                {
                                    selected = Some(results.len() - 1);
                                }
                                needs_draw = true;
                            }
                        }
                        Err(err) => {
                            dirty_since = None;
                            last_error = Some(format!("Search error: {err}"));
                            status = "Search error (see footer).".to_string();
                            results.clear();
                            needs_draw = true;
                        }
                    }
                } else if query.trim().is_empty() {
                    results.clear();
                    status =
                        "Type to search. Hotkeys: a agent, w workspace, f from, t to, x clear, PgUp/PgDn paginate, q quit."
                            .to_string();
                    needs_draw = true;
                }
            }
            last_tick = Instant::now();
        }
    }

    teardown_terminal()
}

fn default_db_path_for(data_dir: &std::path::Path) -> std::path::PathBuf {
    data_dir.join("agent_search.db")
}

fn run_tui_headless(data_dir_override: Option<std::path::PathBuf>) -> Result<()> {
    let data_dir = data_dir_override.unwrap_or_else(default_data_dir);
    let index_path = index_dir(&data_dir)?;
    let db_path = default_db_path_for(&data_dir);
    let client = SearchClient::open(&index_path, Some(&db_path))?
        .ok_or_else(|| anyhow::anyhow!("index/db not found"))?;
    let _ = client.search("", SearchFilters::default(), 5, 0)?;
    Ok(())
}

fn teardown_terminal() -> Result<()> {
    let mut stdout = io::stdout();
    disable_raw_mode()?;
    execute!(stdout, LeaveAlternateScreen)?;
    Ok(())
}
