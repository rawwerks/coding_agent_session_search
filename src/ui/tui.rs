//! Ratatui-based interface wired to Tantivy search.

use anyhow::Result;
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

use crate::model::types::MessageRole;
use crate::search::query::{SearchClient, SearchFilters, SearchHit};
use crate::search::tantivy::index_dir;
use crate::ui::components::theme::ThemePalette;
use crate::ui::components::widgets::search_bar;
use crate::ui::data::{ConversationView, load_conversation, role_style};
use crate::{default_data_dir, default_db_path};

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
    let help_lines = vec![
        "q/esc: quit",
        "/: focus query",
        "a: agent filter",
        "w: workspace filter",
        "f/t: time from/to",
        "x: clear filters",
        "Tab: cycle detail tabs",
        "h: toggle theme",
        "PgUp/PgDn or [ ]: paginate",
        "j/k or arrows: navigate",
        "?: toggle help",
    ];

    let text: Vec<Line> = help_lines
        .into_iter()
        .map(|l| Line::from(Span::styled(l, Style::default().fg(palette.fg))))
        .collect();

    let block = Block::default()
        .title(Span::styled("Help", palette.title()))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(palette.accent));
    frame.render_widget(
        Paragraph::new(text).block(block).wrap(Wrap { trim: true }),
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

fn highlight_terms_owned(text: String, query: &str, palette: ThemePalette) -> Line<'static> {
    let owned = text;
    let mut spans = Vec::new();
    if query.trim().is_empty() {
        spans.push(Span::raw(owned));
        let line: Line = spans.into();
        return unsafe { std::mem::transmute::<Line, Line<'static>>(line) };
    }
    let lower = owned.to_lowercase();
    let q = query.to_lowercase();
    let mut idx = 0;
    while let Some(pos) = lower[idx..].find(&q) {
        let start = idx + pos;
        if start > idx {
            spans.push(Span::raw(owned[idx..start].to_string()));
        }
        let end = start + q.len();
        spans.push(Span::styled(
            owned[start..end].to_string(),
            Style::default()
                .fg(palette.accent)
                .add_modifier(Modifier::BOLD),
        ));
        idx = end;
    }
    if idx < owned.len() {
        spans.push(Span::raw(owned[idx..].to_string()));
    }
    let line: Line = spans.into();
    unsafe { std::mem::transmute::<Line, Line<'static>>(line) }
}

pub fn footer_legend(show_help: bool) -> &'static str {
    if show_help {
        "q/esc quit • arrows/PgUp/PgDn navigate • / focus query • a agent • w workspace • f from • t to • x clear • tab detail • h theme"
    } else {
        "?/hide help | a agent | w workspace | f from | t to | x clear | tab detail | h theme | q quit"
    }
}

pub fn run_tui() -> Result<()> {
    let mut stdout = io::stdout();
    enable_raw_mode()?;
    stdout.execute(EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let data_dir = default_data_dir();
    let index_path = index_dir(&data_dir)?;
    let db_path = default_db_path();
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
    let mut show_help = false;
    let mut last_error: Option<String> = None;
    let mut cached_detail: Option<(String, ConversationView)> = None;
    let mut last_query = String::new();

    loop {
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
            let sb = search_bar(&bar_text);
            f.render_widget(sb, chunks[0]);

            // Filter pills row
            let mut pill_spans = Vec::new();
            if !filters.agents.is_empty() {
                pill_spans.push(Span::styled(
                    format!(
                        "agent:{}",
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
                        "ws:{}",
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
                    format!("time:{:?}->{:?}", filters.created_from, filters.created_to),
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
                                    .map(|t| format!(" ({t})"))
                                    .unwrap_or_default();
                                lines.push(Line::from(vec![
                                    Span::styled(format!("[{role_label}]"), role_style(&msg.role)),
                                    Span::raw(ts),
                                ]));
                                let owned = msg.content.clone();
                                let content_line =
                                    highlight_terms_owned(owned, &last_query, palette);
                                lines.push(content_line);
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
                    DetailTab::Snippets => Paragraph::new("No snippets indexed yet.")
                        .style(Style::default().fg(palette.hint)),
                    DetailTab::Raw => Paragraph::new(format!("Path: {}", hit.source_path))
                        .wrap(Wrap { trim: true }),
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

        let timeout = tick_rate
            .checked_sub(last_tick.elapsed())
            .unwrap_or_else(|| Duration::from_millis(0));

        if crossterm::event::poll(timeout)?
            && let Event::Key(key) = event::read()?
        {
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
                            let editor = std::env::var("EDITOR").unwrap_or_else(|_| "vi".into());
                            let _ = StdCommand::new(editor).arg(path).status();
                        }
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
                            }
                        }
                        Err(err) => {
                            dirty_since = None;
                            last_error = Some(format!("Search error: {err}"));
                            status = "Search error (see footer).".to_string();
                            results.clear();
                        }
                    }
                } else if query.trim().is_empty() {
                    results.clear();
                    status =
                        "Type to search. Hotkeys: a agent, w workspace, f from, t to, x clear, PgUp/PgDn paginate, q quit."
                            .to_string();
                }
            }
            last_tick = Instant::now();
        }
    }

    teardown_terminal()
}

fn teardown_terminal() -> Result<()> {
    let mut stdout = io::stdout();
    disable_raw_mode()?;
    execute!(stdout, LeaveAlternateScreen)?;
    Ok(())
}
