//! Ratatui-based interface wired to Tantivy search.

use anyhow::Result;
use chrono::{DateTime, Utc};
use crossterm::event::{self, Event, KeyCode, KeyModifiers};
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use crossterm::{ExecutableCommand, execute};
use ratatui::prelude::*;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, List, ListItem, ListState, Paragraph, Tabs, Wrap};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::io;
use std::process::Command as StdCommand;
use std::time::{Duration, Instant};

use crate::default_data_dir;
use crate::model::types::MessageRole;
use crate::search::query::{SearchClient, SearchFilters, SearchHit};
use crate::search::tantivy::index_dir;
use crate::ui::components::theme::ThemePalette;
use crate::ui::components::widgets::search_bar;
use crate::ui::data::{ConversationView, InputMode, load_conversation, role_style};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DetailTab {
    Messages,
    Snippets,
    Raw,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MatchMode {
    Standard,
    Prefix,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RankingMode {
    RecentHeavy,
    Balanced,
    RelevanceHeavy,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ContextWindow {
    Small,
    Medium,
    Large,
    XLarge,
}

impl ContextWindow {
    fn next(self) -> Self {
        match self {
            ContextWindow::Small => ContextWindow::Medium,
            ContextWindow::Medium => ContextWindow::Large,
            ContextWindow::Large => ContextWindow::XLarge,
            ContextWindow::XLarge => ContextWindow::Small,
        }
    }

    fn size(self) -> usize {
        match self {
            ContextWindow::Small => 80,
            ContextWindow::Medium => 160,
            ContextWindow::Large => 320,
            ContextWindow::XLarge => 640,
        }
    }

    fn label(self) -> &'static str {
        match self {
            ContextWindow::Small => "S",
            ContextWindow::Medium => "M",
            ContextWindow::Large => "L",
            ContextWindow::XLarge => "XL",
        }
    }
}

#[derive(Serialize, Deserialize, Default)]
struct TuiStatePersisted {
    match_mode: Option<String>,
    context_window: Option<String>,
}

#[derive(Clone, Debug)]
struct AgentPane {
    agent: String,
    hits: Vec<SearchHit>,
    selected: usize,
}

fn help_lines(palette: ThemePalette) -> Vec<Line<'static>> {
    let mut lines: Vec<Line<'static>> = Vec::new();

    let add_section = |title: &str, items: &[&str]| -> Vec<Line<'static>> {
        let mut v = Vec::new();
        v.push(Line::from(Span::styled(title.to_string(), palette.title())));
        for item in items {
            v.push(Line::from(format!("  {item}")));
        }
        v.push(Line::from(""));
        v
    };

    lines.extend(add_section(
        "Search",
        &["type to live-search; / focuses query; Ctrl-R cycles history"],
    ));
    lines.extend(add_section(
        "Filters",
        &[
            "F3 agent | F4 workspace | F5 from | F6 to | Ctrl+Del clear all",
            "Shift+F3 scope to active agent | Shift+F4 clear scope | Shift+F5 cycle time presets (24h/7d/30d/all)",
            "Chips in search bar; Backspace removes last; Enter (query empty) edits last chip",
        ],
    ));
    lines.extend(add_section(
        "Modes",
        &[
            "F9 match mode: prefix (default) ⇄ standard",
            "F12 ranking: recent-heavy → balanced → relevance-heavy",
            "F2 theme: dark/light",
        ],
    ));
    lines.extend(add_section(
        "Context",
        &[
            "F7 cycles S/M/L/XL context window",
            "Space: peek XL for current hit, tap again to restore",
        ],
    ));
    lines.extend(add_section(
        "Density",
        &["Shift+=/+ increase pane items; - decrease (min 4, max 50)"],
    ));
    lines.extend(add_section(
        "Navigation",
        &[
            "Arrows move; Left/Right pane; PgUp/PgDn page",
            "Alt+NumPad 1-9 jump pane; g/G jump first/last item",
            "Tab toggles focus (Results ⇄ Detail)",
            "[ / ] cycle detail tabs (Messages/Snippets/Raw)",
        ],
    ));
    lines.extend(add_section(
        "Actions",
        &[
            "Enter/F8 open hit in $EDITOR; y copy path/content",
            "F1 toggle this help; Esc/F10 quit (or back from detail)",
        ],
    ));
    lines.extend(add_section(
        "States",
        &["match mode + context persist in tui_state.json (data dir); delete to reset"],
    ));
    lines.extend(add_section(
        "Empty state",
        &[
            "Shows recent per-agent hits before typing",
            "Recent query suggestions appear when query is empty",
        ],
    ));

    lines
}

fn render_help_overlay(frame: &mut Frame, palette: ThemePalette, scroll: u16) {
    let area = frame.area();
    let popup_area = centered_rect(70, 70, area);
    let lines = help_lines(palette);
    let block = Block::default()
        .title(Span::styled("Help / Shortcuts", palette.title()))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(palette.accent));

    frame.render_widget(ratatui::widgets::Clear, popup_area);

    frame.render_widget(
        Paragraph::new(lines)
            .block(block)
            .wrap(Wrap { trim: true })
            .scroll((scroll, 0)),
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

fn build_agent_panes(results: &[SearchHit], per_pane_limit: usize) -> Vec<AgentPane> {
    let mut panes: Vec<AgentPane> = Vec::new();
    for hit in results {
        if let Some(pane) = panes.iter_mut().find(|p| p.agent == hit.agent) {
            if pane.hits.len() < per_pane_limit {
                pane.hits.push(hit.clone());
            }
        } else {
            panes.push(AgentPane {
                agent: hit.agent.clone(),
                hits: vec![hit.clone()],
                selected: 0,
            });
        }
    }
    panes
}

fn active_hit(panes: &[AgentPane], active_idx: usize) -> Option<&SearchHit> {
    panes
        .get(active_idx)
        .and_then(|pane| pane.hits.get(pane.selected))
}

fn agent_display_name(agent: &str) -> String {
    agent
        .replace(['_', '-'], " ")
        .split_whitespace()
        .map(|w| {
            let mut chars = w.chars();
            if let Some(first) = chars.next() {
                format!("{}{}", first.to_uppercase(), chars.as_str())
            } else {
                String::new()
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn contextual_snippet(text: &str, query: &str, window: ContextWindow) -> String {
    let size = window.size();
    if text.is_empty() {
        return String::new();
    }
    let lowercase = text.to_lowercase();
    let q = query.to_lowercase();

    let byte_pos = if q.is_empty() {
        Some(0)
    } else {
        lowercase.find(&q)
    }
    .or_else(|| {
        q.split_whitespace()
            .next()
            .and_then(|first| lowercase.find(first))
    });

    let chars: Vec<char> = text.chars().collect();
    let char_pos = byte_pos.map(|b| text[..b].chars().count()).unwrap_or(0);
    let len = chars.len();
    let start = char_pos.saturating_sub(size / 2);
    let end = (start + size).min(len);
    let slice: String = chars[start..end].iter().collect();
    let prefix = if start > 0 { "…" } else { "" };
    let suffix = if end < len { "…" } else { "" };
    format!("{prefix}{slice}{suffix}")
}

fn state_path_for(data_dir: &std::path::Path) -> std::path::PathBuf {
    // Persist lightweight, non-secret UI preferences (match mode, context window).
    data_dir.join("tui_state.json")
}

fn chips_for_filters(filters: &SearchFilters, palette: ThemePalette) -> Vec<Span<'static>> {
    let mut spans: Vec<Span> = Vec::new();
    if !filters.agents.is_empty() {
        spans.push(Span::styled(
            format!(
                "[agent:{}]",
                filters.agents.iter().cloned().collect::<Vec<_>>().join("|")
            ),
            Style::default()
                .fg(palette.accent_alt)
                .add_modifier(Modifier::BOLD),
        ));
        spans.push(Span::raw(" "));
    }
    if !filters.workspaces.is_empty() {
        spans.push(Span::styled(
            format!(
                "[ws:{}]",
                filters
                    .workspaces
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>()
                    .join("|")
            ),
            Style::default().fg(palette.accent_alt),
        ));
        spans.push(Span::raw(" "));
    }
    if filters.created_from.is_some() || filters.created_to.is_some() {
        spans.push(Span::styled(
            format!(
                "[time:{:?}->{:?}]",
                filters.created_from, filters.created_to
            ),
            Style::default().fg(palette.accent_alt),
        ));
        spans.push(Span::raw(" "));
    }
    spans
        .into_iter()
        .map(|s| unsafe { std::mem::transmute::<Span<'_>, Span<'static>>(s) })
        .collect()
}

fn load_state(path: &std::path::Path) -> TuiStatePersisted {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default()
}

fn save_state(path: &std::path::Path, state: &TuiStatePersisted) {
    if let Ok(body) = serde_json::to_string_pretty(state) {
        let _ = std::fs::write(path, body);
    }
}

fn apply_match_mode(query: &str, mode: MatchMode) -> String {
    match mode {
        MatchMode::Standard => query.to_string(),
        MatchMode::Prefix => query
            .split_whitespace()
            .filter(|s| !s.is_empty())
            .map(|term| {
                if term.ends_with('*') {
                    term.to_string()
                } else {
                    format!("{term}*")
                }
            })
            .collect::<Vec<_>>()
            .join(" "),
    }
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FocusRegion {
    Results,
    Detail,
}

pub fn footer_legend(show_help: bool) -> &'static str {
    if show_help {
        "Esc/F10 quit • arrows + Left/Right pane • PgUp/PgDn page • Tab Focus • [ / ] Tabs • F3/F4/F5/F6 filters • Ctrl+Del clear • F7 context • F9 mode • F2 theme • Enter/F8 open • Alt+NumPad 1-9 pane • Ctrl-R history • y copy"
    } else {
        "F1 help | F3 agent | F4 workspace | F5/F6 time | F7 context | Ctrl+Del clear | F9 mode | F2 theme | Enter/F8 open | Alt+NumPad pane | y copy | Esc/F10 quit"
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
    let state_path = state_path_for(&data_dir);
    let persisted = load_state(&state_path);
    let search_client = SearchClient::open(&index_path, Some(&db_path))?;
    let index_ready = search_client.is_some();
    let mut status = if index_ready {
        format!(
            "Index ready at {} - type to search (Esc/F10 quit, F1 help)",
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
    let page_size: usize = 120;
    let mut per_pane_limit: usize = 12;
    let mut page: usize = 0;
    let mut results: Vec<SearchHit> = Vec::new();
    let mut panes: Vec<AgentPane> = Vec::new();
    let mut active_pane: usize = 0;
    let mut focus_region = FocusRegion::Results;
    let mut detail_scroll: u16 = 0;
    let mut focus_flash_until: Option<Instant> = None;
    let mut last_tick = Instant::now();
    let tick_rate = Duration::from_millis(30);
    let debounce = Duration::from_millis(60);
    let mut dirty_since: Option<Instant> = Some(Instant::now());

    let mut detail_tab = DetailTab::Messages;
    let mut theme_dark = true;
    // Show onboarding overlay on first launch; user can dismiss with F1.
    let mut show_help = true;
    let mut cached_detail: Option<(String, ConversationView)> = None;
    let mut last_query = String::new();
    let mut needs_draw = true;
    let mut query_history: VecDeque<String> = VecDeque::new();
    let history_cap: usize = 50;
    let mut history_cursor: Option<usize> = None;
    let mut suggestion_idx: Option<usize> = None;
    let mut match_mode = match persisted.match_mode.as_deref() {
        Some("standard") => MatchMode::Standard,
        _ => MatchMode::Prefix,
    };
    let mut ranking_mode = RankingMode::Balanced;
    let mut context_window = match persisted.context_window.as_deref() {
        Some("S") => ContextWindow::Small,
        Some("M") => ContextWindow::Medium,
        Some("L") => ContextWindow::Large,
        Some("XL") => ContextWindow::XLarge,
        _ => ContextWindow::Medium,
    };
    let mut peek_window_saved: Option<ContextWindow> = None;
    let mut peek_badge_until: Option<Instant> = None;
    let mut help_scroll: u16 = 0;
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
                let mode_label = match match_mode {
                    MatchMode::Standard => "standard",
                    MatchMode::Prefix => "prefix",
                };
                let chips = chips_for_filters(&filters, palette);
                let sb = search_bar(&bar_text, palette, input_mode, mode_label, chips);
                f.render_widget(sb, chunks[0]);

                // Filter pills row
                let mut pill_spans = Vec::new();
                if !filters.agents.is_empty() {
                    pill_spans.push(Span::styled(
                        format!(
                            "[F3] agent:{}",
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
                            "[F4] ws:{}",
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
                            "[F5/F6] time:{:?}->{:?}",
                            filters.created_from, filters.created_to
                        ),
                        Style::default().fg(palette.accent_alt),
                    ));
                }
                if pill_spans.is_empty() {
                    pill_spans.push(Span::styled(
                        "filters: none (press F3/F4/F5/F6)",
                        Style::default().fg(palette.hint),
                    ));
                }
                let pill_para = Paragraph::new(Line::from(pill_spans));
                f.render_widget(pill_para, chunks[1]);

                let main_split = Layout::default()
                    .direction(Direction::Vertical)
                    .constraints([Constraint::Percentage(70), Constraint::Percentage(30)].as_ref())
                    .split(chunks[2]);

                let results_area = main_split[0];
                let detail_area = main_split[1];

                if panes.is_empty() {
                    let mut lines: Vec<Line> = Vec::new();
                    if query.trim().is_empty() && !query_history.is_empty() {
                        lines.push(Line::from(Span::styled(
                            "Recent queries (Enter to load):",
                            palette.title(),
                        )));
                        for (idx, q) in query_history.iter().take(5).enumerate() {
                            let selected = suggestion_idx == Some(idx);
                            lines.push(Line::from(Span::styled(
                                format!("{} {}", if selected { "▶" } else { " " }, q),
                                if selected {
                                    Style::default()
                                        .fg(palette.accent)
                                        .add_modifier(Modifier::BOLD)
                                } else {
                                    Style::default().fg(palette.hint)
                                },
                            )));
                        }
                    } else {
                        lines.push(Line::from("No results found."));

                        // Zero-hit suggestions
                        let mut suggestions = Vec::new();
                        if !filters.agents.is_empty() {
                            suggestions.push("Clear agent filter (Shift+F3)");
                        }
                        if !filters.workspaces.is_empty() {
                            suggestions.push("Clear workspace filter (Shift+F4)");
                        }
                        if matches!(match_mode, MatchMode::Standard) {
                            suggestions.push("Try prefix mode (F9)");
                        }
                        if !suggestions.is_empty() {
                            lines.push(Line::from(""));
                            lines.push(Line::from(Span::styled("Suggestions:", palette.title())));
                            for s in suggestions {
                                lines.push(Line::from(format!("• {s}")));
                            }
                        }

                        lines.push(Line::from(""));
                        lines.push(Line::from(Span::raw(
                            "Tip: toggle F9 prefix mode or clear all filters with Ctrl+Del",
                        )));
                    }

                    let block = Block::default().title("Results").borders(Borders::ALL);
                    f.render_widget(Paragraph::new(lines).block(block), results_area);
                } else {
                    let pane_width = (100 / std::cmp::max(panes.len(), 1)) as u16;
                    let pane_constraints: Vec<Constraint> = panes
                        .iter()
                        .map(|_| Constraint::Percentage(pane_width))
                        .collect();
                    let pane_chunks = Layout::default()
                        .direction(Direction::Horizontal)
                        .constraints(pane_constraints)
                        .split(results_area);

                    for (idx, pane) in panes.iter().enumerate() {
                        let theme = ThemePalette::agent_pane(&pane.agent);
                        let mut state = ListState::default();
                        state.select(Some(pane.selected));

                        let items: Vec<ListItem> = pane
                            .hits
                            .iter()
                            .map(|hit| {
                                let title = if hit.title.is_empty() {
                                    "(untitled)"
                                } else {
                                    hit.title.as_str()
                                };
                                let header = Line::from(vec![
                                    Span::styled(
                                        format!("{:.1}", hit.score),
                                        Style::default().fg(theme.accent),
                                    ),
                                    Span::raw(" "),
                                    Span::styled(
                                        title.to_string(),
                                        Style::default().fg(theme.fg).add_modifier(Modifier::BOLD),
                                    ),
                                ]);
                                let location = if hit.workspace.is_empty() {
                                    hit.source_path.clone()
                                } else {
                                    format!("{} ({})", hit.source_path, hit.workspace)
                                };
                                let raw_snippet =
                                    contextual_snippet(&hit.content, &last_query, context_window);
                                let body_line = highlight_terms_owned_with_style(
                                    format!("{location} • {raw_snippet}"),
                                    &last_query,
                                    palette,
                                    Style::default().fg(theme.fg),
                                );
                                ListItem::new(vec![header, body_line])
                            })
                            .collect();

                        let flash_active = focus_flash_until
                            .map(|t| t > Instant::now())
                            .unwrap_or(false)
                            && idx == active_pane;

                        let is_focused_pane = match focus_region {
                            FocusRegion::Results => idx == active_pane,
                            FocusRegion::Detail => false,
                        };

                        let block = Block::default()
                            .title(Span::styled(
                                format!(
                                    "{} ({})",
                                    agent_display_name(&pane.agent),
                                    pane.hits.len()
                                ),
                                Style::default().fg(theme.accent).add_modifier(
                                    if is_focused_pane {
                                        Modifier::BOLD
                                    } else {
                                        Modifier::empty()
                                    },
                                ),
                            ))
                            .borders(Borders::ALL)
                            .border_style(Style::default().fg(if is_focused_pane {
                                theme.accent
                            } else {
                                palette.hint
                            }))
                            .style(
                                Style::default()
                                    .bg(if flash_active { theme.accent } else { theme.bg })
                                    .fg(if flash_active { theme.bg } else { theme.fg }),
                            );

                        let list = List::new(items)
                            .block(block)
                            .highlight_style(
                                Style::default()
                                    .bg(if is_focused_pane {
                                        theme.accent
                                    } else {
                                        palette.hint
                                    })
                                    .fg(theme.bg)
                                    .add_modifier(Modifier::BOLD),
                            )
                            .style(Style::default().bg(theme.bg).fg(theme.fg));

                        if let Some(area) = pane_chunks.get(idx) {
                            f.render_stateful_widget(list, *area, &mut state);
                        }
                    }
                }

                if let Some(hit) = active_hit(&panes, active_pane) {
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
                    let agent_theme = ThemePalette::agent_pane(&hit.agent);
                    meta_lines.push(Line::from(vec![
                        Span::styled("Title: ", palette.title()),
                        Span::raw(hit.title.clone()),
                    ]));
                    meta_lines.push(Line::from(vec![
                        Span::styled("Agent: ", Style::default().fg(agent_theme.accent)),
                        Span::styled(
                            agent_display_name(&hit.agent),
                            Style::default().fg(agent_theme.fg),
                        ),
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
                            // Reset scroll when loading new conversation
                            detail_scroll = 0;
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
                                    Paragraph::new(lines)
                                        .wrap(Wrap { trim: true })
                                        .scroll((detail_scroll, 0))
                                }
                            } else {
                                Paragraph::new(hit.content.clone())
                                    .wrap(Wrap { trim: true })
                                    .scroll((detail_scroll, 0))
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
                                    Paragraph::new(lines)
                                        .wrap(Wrap { trim: true })
                                        .scroll((detail_scroll, 0))
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
                                Paragraph::new(text)
                                    .wrap(Wrap { trim: true })
                                    .scroll((detail_scroll, 0))
                            } else {
                                Paragraph::new(format!("Path: {}", hit.source_path))
                                    .wrap(Wrap { trim: true })
                                    .scroll((detail_scroll, 0))
                            }
                        }
                    };

                    let is_focused_detail = matches!(focus_region, FocusRegion::Detail);
                    let block = Block::default()
                        .title("Detail")
                        .borders(Borders::ALL)
                        .border_style(Style::default().fg(if is_focused_detail {
                            palette.accent
                        } else {
                            palette.hint
                        }));

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
                    f.render_widget(content_para.block(block), layout[2]);
                } else {
                    f.render_widget(
                        Paragraph::new("Select a result to view details")
                            .block(Block::default().title("Detail").borders(Borders::ALL)),
                        detail_area,
                    );
                }

                let mut footer_line = format!(
                    "{} | mode:{} | rank:{} | ctx:{}({}) | {}",
                    status,
                    match match_mode {
                        MatchMode::Standard => "standard",
                        MatchMode::Prefix => "prefix",
                    },
                    match ranking_mode {
                        RankingMode::RecentHeavy => "recent",
                        RankingMode::Balanced => "balanced",
                        RankingMode::RelevanceHeavy => "relevance",
                    },
                    context_window.label(),
                    context_window.size(),
                    footer_legend(show_help)
                );
                if peek_badge_until
                    .map(|t| t > Instant::now())
                    .unwrap_or(false)
                {
                    footer_line.push_str(" | PEEK");
                }
                let footer = Paragraph::new(footer_line);
                f.render_widget(footer, chunks[3]);

                if show_help {
                    render_help_overlay(f, palette, help_scroll);
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

            // Global quit override
            if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
                break;
            }

            // While help is open, keys scroll the help modal and do not affect panes.
            if show_help {
                match key.code {
                    KeyCode::Esc | KeyCode::F(1) => {
                        show_help = false;
                        help_scroll = 0;
                    }
                    KeyCode::Up => {
                        help_scroll = help_scroll.saturating_sub(1);
                    }
                    KeyCode::Down => {
                        help_scroll = help_scroll.saturating_add(1);
                    }
                    KeyCode::PageUp => {
                        help_scroll = help_scroll.saturating_sub(5);
                    }
                    KeyCode::PageDown => {
                        help_scroll = help_scroll.saturating_add(5);
                    }
                    KeyCode::Home => help_scroll = 0,
                    KeyCode::End => help_scroll = help_lines(ThemePalette::dark()).len() as u16,
                    _ => {}
                }
                continue;
            }
            match input_mode {
                InputMode::Query => {
                    if key.modifiers.contains(KeyModifiers::CONTROL) {
                        if let KeyCode::Char('r') = key.code {
                            if query_history.is_empty() {
                                status = "No query history yet".to_string();
                            } else {
                                let next = history_cursor
                                    .map(|idx| (idx + 1) % query_history.len())
                                    .unwrap_or(0);
                                if let Some(saved) = query_history.get(next) {
                                    history_cursor = Some(next);
                                    query = saved.clone();
                                    page = 0;
                                    dirty_since = Some(Instant::now());
                                    status = format!("Loaded query #{next} from history");
                                    cached_detail = None;
                                    detail_scroll = 0;
                                }
                            }
                        }
                        continue;
                    }

                    match key.code {
                        KeyCode::Esc | KeyCode::F(10) => {
                            // If in Detail, Esc goes back to Results.
                            if matches!(focus_region, FocusRegion::Detail) {
                                focus_region = FocusRegion::Results;
                                status = "Focus: Results".to_string();
                            } else {
                                break;
                            }
                        }
                        KeyCode::Down => {
                            match focus_region {
                                FocusRegion::Results => {
                                    if panes.is_empty()
                                        && query.trim().is_empty()
                                        && !query_history.is_empty()
                                    {
                                        let max_idx = query_history.len().min(5).saturating_sub(1);
                                        let next = suggestion_idx.unwrap_or(0).saturating_add(1);
                                        suggestion_idx = Some(std::cmp::min(next, max_idx));
                                        status = "Enter to load selected recent query".to_string();
                                    } else if let Some(pane) = panes.get_mut(active_pane)
                                        && pane.selected + 1 < pane.hits.len()
                                    {
                                        pane.selected += 1;
                                        // Re-load details for new selection
                                        cached_detail = None;
                                        detail_scroll = 0;
                                    }
                                }
                                FocusRegion::Detail => {
                                    detail_scroll = detail_scroll.saturating_add(1);
                                }
                            }
                        }
                        KeyCode::Up => {
                            match focus_region {
                                FocusRegion::Results => {
                                    if panes.is_empty()
                                        && query.trim().is_empty()
                                        && !query_history.is_empty()
                                    {
                                        let next = suggestion_idx.unwrap_or(0).saturating_sub(1);
                                        suggestion_idx = Some(next);
                                        status = "Enter to load selected recent query".to_string();
                                    } else if let Some(pane) = panes.get_mut(active_pane)
                                        && pane.selected > 0
                                    {
                                        pane.selected -= 1;
                                        // Re-load details for new selection
                                        cached_detail = None;
                                        detail_scroll = 0;
                                    }
                                }
                                FocusRegion::Detail => {
                                    detail_scroll = detail_scroll.saturating_sub(1);
                                }
                            }
                        }
                        KeyCode::Left => match focus_region {
                            FocusRegion::Results => {
                                active_pane = active_pane.saturating_sub(1);
                                focus_flash_until =
                                    Some(Instant::now() + Duration::from_millis(220));
                                cached_detail = None;
                                detail_scroll = 0;
                            }
                            FocusRegion::Detail => {
                                focus_region = FocusRegion::Results;
                                status = "Focus: Results".to_string();
                            }
                        },
                        KeyCode::Right => {
                            match focus_region {
                                FocusRegion::Results => {
                                    if active_pane + 1 < panes.len() {
                                        active_pane += 1;
                                        focus_flash_until =
                                            Some(Instant::now() + Duration::from_millis(220));
                                        cached_detail = None;
                                        detail_scroll = 0;
                                    } else if !panes.is_empty() {
                                        // At last pane, switch focus to detail
                                        focus_region = FocusRegion::Detail;
                                        status =
                                            "Focus: Detail (arrows scroll, Left back)".to_string();
                                    }
                                }
                                FocusRegion::Detail => {
                                    // Already at rightmost
                                }
                            }
                        }
                        KeyCode::PageDown => match focus_region {
                            FocusRegion::Results => {
                                page = page.saturating_add(1);
                                dirty_since = Some(Instant::now());
                            }
                            FocusRegion::Detail => {
                                detail_scroll = detail_scroll.saturating_add(20);
                            }
                        },
                        KeyCode::PageUp => match focus_region {
                            FocusRegion::Results => {
                                page = page.saturating_sub(1);
                                dirty_since = Some(Instant::now());
                            }
                            FocusRegion::Detail => {
                                detail_scroll = detail_scroll.saturating_sub(20);
                            }
                        },
                        KeyCode::Char('y') => {
                            if let Some(hit) = active_hit(&panes, active_pane) {
                                let text_to_copy = if matches!(focus_region, FocusRegion::Detail) {
                                    if let Some((_, _)) = &cached_detail {
                                        hit.content.clone()
                                    } else {
                                        hit.content.clone()
                                    }
                                } else {
                                    hit.source_path.clone()
                                };

                                #[cfg(any(target_os = "linux", target_os = "macos"))]
                                {
                                    use std::process::Stdio;
                                    let child = std::process::Command::new("sh")
                                        .arg("-c")
                                        .arg("if command -v wl-copy >/dev/null; then wl-copy; elif command -v pbcopy >/dev/null; then pbcopy; elif command -v xclip >/dev/null; then xclip -selection clipboard; fi")
                                        .stdin(Stdio::piped())
                                        .spawn();
                                    if let Ok(mut child) = child
                                        && let Some(mut stdin) = child.stdin.take()
                                    {
                                        use std::io::Write;
                                        let _ = stdin.write_all(text_to_copy.as_bytes());
                                        drop(stdin); // Ensure EOF
                                        let _ = child.wait();
                                        status = "Copied to clipboard".to_string();
                                    } else {
                                        status =
                                            "Clipboard copy failed (missing tool?)".to_string();
                                    }
                                }
                                #[cfg(target_os = "windows")]
                                {
                                    let child = std::process::Command::new("powershell")
                                        .arg("-command")
                                        .arg("$Input | Set-Clipboard")
                                        .stdin(std::process::Stdio::piped())
                                        .spawn();
                                    if let Ok(mut child) = child
                                        && let Some(mut stdin) = child.stdin.take()
                                    {
                                        use std::io::Write;
                                        let _ = stdin.write_all(text_to_copy.as_bytes());
                                        drop(stdin);
                                        let _ = child.wait();
                                        status = "Copied to clipboard".to_string();
                                    }
                                }
                            }
                        }
                        KeyCode::F(1) => {
                            show_help = !show_help;
                            help_scroll = 0;
                        }
                        KeyCode::F(2) => {
                            theme_dark = !theme_dark;
                            status = format!(
                                "Theme: {}, mode: {}",
                                if theme_dark { "dark" } else { "light" },
                                match match_mode {
                                    MatchMode::Standard => "standard",
                                    MatchMode::Prefix => "prefix",
                                }
                            );
                        }
                        KeyCode::F(3) if key.modifiers.contains(KeyModifiers::SHIFT) => {
                            if let Some(hit) = active_hit(&panes, active_pane) {
                                filters.agents.clear();
                                filters.agents.insert(hit.agent.clone());
                                status = format!("Scoped to agent {}", hit.agent);
                                page = 0;
                                dirty_since = Some(Instant::now());
                                focus_region = FocusRegion::Results;
                                cached_detail = None;
                                detail_scroll = 0;
                            }
                        }
                        KeyCode::F(3) => {
                            input_mode = InputMode::Agent;
                            input_buffer.clear();
                            status = "Agent filter: type slug, Enter=apply, Esc=cancel".to_string();
                        }
                        KeyCode::F(4) if key.modifiers.contains(KeyModifiers::SHIFT) => {
                            filters.agents.clear();
                            status = "Scope: all agents".to_string();
                            page = 0;
                            dirty_since = Some(Instant::now());
                            focus_region = FocusRegion::Results;
                            cached_detail = None;
                            detail_scroll = 0;
                        }
                        KeyCode::F(4) => {
                            input_mode = InputMode::Workspace;
                            input_buffer.clear();
                            status =
                                "Workspace filter: type path fragment, Enter=apply, Esc=cancel"
                                    .to_string();
                        }
                        KeyCode::F(5) if key.modifiers.contains(KeyModifiers::SHIFT) => {
                            let now = chrono::Utc::now().timestamp_millis();
                            let presets = [
                                Some(now - 86_400_000),    // 24h
                                Some(now - 604_800_000),   // 7d
                                Some(now - 2_592_000_000), // 30d
                                None,
                            ];
                            let current = filters.created_from;
                            let idx = presets
                                .iter()
                                .position(|p| *p == current)
                                .unwrap_or(usize::MAX);
                            let next = presets.get((idx + 1) % presets.len()).copied().flatten();
                            filters.created_from = next;
                            filters.created_to = None;
                            page = 0;
                            status = match next {
                                Some(_) => "Time preset: since recent".to_string(),
                                None => "Time preset: all".to_string(),
                            };
                            dirty_since = Some(Instant::now());
                            focus_region = FocusRegion::Results;
                            cached_detail = None;
                            detail_scroll = 0;
                        }
                        KeyCode::F(5) => {
                            input_mode = InputMode::CreatedFrom;
                            input_buffer.clear();
                            status = "Created-from (ms since epoch): Enter=apply, Esc=cancel"
                                .to_string();
                        }
                        KeyCode::F(6) => {
                            input_mode = InputMode::CreatedTo;
                            input_buffer.clear();
                            status =
                                "Created-to (ms since epoch): Enter=apply, Esc=cancel".to_string();
                        }
                        KeyCode::F(7) => {
                            context_window = context_window.next();
                            status = format!(
                                "Context window: {} ({} chars)",
                                context_window.label(),
                                context_window.size()
                            );
                            dirty_since = Some(Instant::now());
                        }
                        KeyCode::F(12) => {
                            ranking_mode = match ranking_mode {
                                RankingMode::RecentHeavy => RankingMode::Balanced,
                                RankingMode::Balanced => RankingMode::RelevanceHeavy,
                                RankingMode::RelevanceHeavy => RankingMode::RecentHeavy,
                            };
                            status = format!(
                                "Ranking: {}",
                                match ranking_mode {
                                    RankingMode::RecentHeavy => "recent-heavy",
                                    RankingMode::Balanced => "balanced",
                                    RankingMode::RelevanceHeavy => "relevance-heavy",
                                }
                            );
                            dirty_since = Some(Instant::now());
                        }
                        KeyCode::Delete if key.modifiers.contains(KeyModifiers::CONTROL) => {
                            filters = SearchFilters::default();
                            page = 0;
                            status = format!(
                                "Filters cleared | mode: {}",
                                match match_mode {
                                    MatchMode::Standard => "standard",
                                    MatchMode::Prefix => "prefix",
                                }
                            );
                            dirty_since = Some(Instant::now());
                            focus_region = FocusRegion::Results;
                            cached_detail = None;
                            detail_scroll = 0;
                        }
                        KeyCode::F(8) => {
                            if let Some(hit) = active_hit(&panes, active_pane) {
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
                        KeyCode::F(9) => {
                            match_mode = match match_mode {
                                MatchMode::Standard => MatchMode::Prefix,
                                MatchMode::Prefix => MatchMode::Standard,
                            };
                            status = format!(
                                "Match mode: {}",
                                match match_mode {
                                    MatchMode::Standard => "standard",
                                    MatchMode::Prefix => "prefix",
                                }
                            );
                            dirty_since = Some(Instant::now());
                        }
                        KeyCode::Tab => {
                            // Toggle focus
                            focus_region = match focus_region {
                                FocusRegion::Results => FocusRegion::Detail,
                                FocusRegion::Detail => FocusRegion::Results,
                            };
                            status = match focus_region {
                                FocusRegion::Results => "Focus: Results".to_string(),
                                FocusRegion::Detail => "Focus: Detail".to_string(),
                            };
                        }
                        KeyCode::Char(']') => {
                            detail_tab = match detail_tab {
                                DetailTab::Messages => DetailTab::Snippets,
                                DetailTab::Snippets => DetailTab::Raw,
                                DetailTab::Raw => DetailTab::Messages,
                            };
                            detail_scroll = 0;
                        }
                        KeyCode::Char('[') => {
                            detail_tab = match detail_tab {
                                DetailTab::Messages => DetailTab::Raw,
                                DetailTab::Snippets => DetailTab::Messages,
                                DetailTab::Raw => DetailTab::Snippets,
                            };
                            detail_scroll = 0;
                        }
                        KeyCode::Char(c) => {
                            // Reset focus to results if typing
                            if matches!(focus_region, FocusRegion::Detail) {
                                focus_region = FocusRegion::Results;
                            }

                            if key.modifiers.contains(KeyModifiers::ALT) {
                                if ('1'..='9').contains(&c) {
                                    let target = c.to_digit(10).unwrap_or(1) as usize - 1;
                                    if target < panes.len() {
                                        active_pane = target;
                                        focus_flash_until =
                                            Some(Instant::now() + Duration::from_millis(220));
                                        cached_detail = None;
                                        detail_scroll = 0;
                                    }
                                }
                                continue;
                            }
                            if key.modifiers.contains(KeyModifiers::SHIFT) && matches!(c, '+' | '=')
                            {
                                per_pane_limit = (per_pane_limit + 2).min(50);
                                status = format!("Pane size: {} items", per_pane_limit);
                                panes = build_agent_panes(&results, per_pane_limit);
                                dirty_since = Some(Instant::now());
                                continue;
                            }
                            if key.modifiers.is_empty() && c == '-' {
                                per_pane_limit = per_pane_limit.saturating_sub(2).max(4);
                                status = format!("Pane size: {} items", per_pane_limit);
                                panes = build_agent_panes(&results, per_pane_limit);
                                dirty_since = Some(Instant::now());
                                continue;
                            }
                            if key.modifiers.is_empty()
                                && c == ' '
                                && !panes.is_empty()
                                && active_hit(&panes, active_pane).is_some()
                            {
                                // Space acts as a momentary zoom: swap to XL context, tap again to restore.
                                if let Some(saved) = peek_window_saved.take() {
                                    context_window = saved;
                                    status = format!(
                                        "Context window: {} ({} chars)",
                                        context_window.label(),
                                        context_window.size()
                                    );
                                    peek_badge_until = None;
                                } else {
                                    peek_window_saved = Some(context_window);
                                    context_window = ContextWindow::XLarge;
                                    status = "Peek: XL context (Space to toggle back)".to_string();
                                    peek_badge_until =
                                        Some(Instant::now() + Duration::from_millis(600));
                                }
                                dirty_since = Some(Instant::now());
                                continue;
                            }
                            if c == 'g' {
                                if let Some(pane) = panes.get_mut(active_pane) {
                                    pane.selected = 0;
                                    cached_detail = None;
                                    detail_scroll = 0;
                                }
                                continue;
                            }
                            if c == 'G' {
                                if let Some(pane) = panes.get_mut(active_pane)
                                    && !pane.hits.is_empty()
                                {
                                    pane.selected = pane.hits.len() - 1;
                                    cached_detail = None;
                                    detail_scroll = 0;
                                }
                                continue;
                            }
                            query.push(c);
                            page = 0;
                            history_cursor = None;
                            suggestion_idx = None;
                            dirty_since = Some(Instant::now());
                            cached_detail = None;
                            detail_scroll = 0;
                        }
                        KeyCode::Backspace => {
                            if query.is_empty() {
                                if filters.created_to.take().is_some() {
                                    status = "Cleared to-timestamp filter".into();
                                } else if filters.created_from.take().is_some() {
                                    status = "Cleared from-timestamp filter".into();
                                } else if let Some(ws) = filters.workspaces.iter().next().cloned() {
                                    filters.workspaces.remove(&ws);
                                    status = format!("Removed workspace filter {ws}");
                                } else if let Some(agent) = filters.agents.iter().next().cloned() {
                                    filters.agents.remove(&agent);
                                    status = format!("Removed agent filter {agent}");
                                } else {
                                    status = "Nothing to delete".into();
                                }
                            } else {
                                query.pop();
                            }
                            page = 0;
                            history_cursor = None;
                            suggestion_idx = None;
                            dirty_since = Some(Instant::now());
                            cached_detail = None;
                            detail_scroll = 0;
                        }
                        KeyCode::Enter => {
                            if panes.is_empty() && query.trim().is_empty() {
                                if let Some(idx) = suggestion_idx
                                    .and_then(|i| query_history.get(i))
                                    .or_else(|| query_history.front())
                                {
                                    query = idx.clone();
                                    status = format!("Loaded recent query: {idx}");
                                    dirty_since = Some(Instant::now());
                                    continue;
                                }
                                if !filters.agents.is_empty() {
                                    input_mode = InputMode::Agent;
                                    if let Some(last) = filters.agents.iter().next() {
                                        input_buffer = last.clone();
                                    }
                                    status =
                                        "Edit agent filter (Enter apply, Esc cancel)".to_string();
                                    continue;
                                }
                                if !filters.workspaces.is_empty() {
                                    input_mode = InputMode::Workspace;
                                    if let Some(last) = filters.workspaces.iter().next() {
                                        input_buffer = last.clone();
                                    }
                                    status = "Edit workspace filter (Enter apply, Esc cancel)"
                                        .to_string();
                                    continue;
                                }
                                if filters.created_from.is_some() {
                                    input_mode = InputMode::CreatedFrom;
                                    input_buffer =
                                        filters.created_from.unwrap_or_default().to_string();
                                    status =
                                        "Edit from timestamp (Enter apply, Esc cancel)".to_string();
                                    continue;
                                }
                                if filters.created_to.is_some() {
                                    input_mode = InputMode::CreatedTo;
                                    input_buffer =
                                        filters.created_to.unwrap_or_default().to_string();
                                    status =
                                        "Edit to timestamp (Enter apply, Esc cancel)".to_string();
                                    continue;
                                }
                            } else if let Some(hit) = active_hit(&panes, active_pane) {
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
                        _ => {}
                    }
                }
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
                        active_pane = 0;
                        cached_detail = None;
                        detail_scroll = 0;
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
                        focus_region = FocusRegion::Results;
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
                        active_pane = 0;
                        cached_detail = None;
                        detail_scroll = 0;
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
                        focus_region = FocusRegion::Results;
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
                        active_pane = 0;
                        cached_detail = None;
                        detail_scroll = 0;
                        status = format!(
                            "created_from={:?}, created_to={:?}",
                            filters.created_from, filters.created_to
                        );
                        input_buffer.clear();
                        dirty_since = Some(Instant::now());
                        focus_region = FocusRegion::Results;
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
                        active_pane = 0;
                        cached_detail = None;
                        detail_scroll = 0;
                        status = format!(
                            "created_from={:?}, created_to={:?}",
                            filters.created_from, filters.created_to
                        );
                        input_buffer.clear();
                        dirty_since = Some(Instant::now());
                        focus_region = FocusRegion::Results;
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

                if should_search {
                    last_query = query.clone();
                    let prev_agent = active_hit(&panes, active_pane)
                        .map(|h| h.agent.clone())
                        .or_else(|| panes.get(active_pane).map(|p| p.agent.clone()));
                    let prev_path = active_hit(&panes, active_pane).map(|h| h.source_path.clone());
                    let q = apply_match_mode(&query, match_mode);
                    match client.search(&q, filters.clone(), page_size, page * page_size) {
                        Ok(hits) => {
                            dirty_since = None;
                            if hits.is_empty() && page > 0 {
                                page = page.saturating_sub(1);
                                active_pane = 0;
                                dirty_since = Some(Instant::now());
                                needs_draw = true;
                            } else {
                                results = hits;
                                let max_created = results
                                    .iter()
                                    .filter_map(|h| h.created_at)
                                    .max()
                                    .unwrap_or(0)
                                    as f32;
                                let alpha = match ranking_mode {
                                    RankingMode::RecentHeavy => 1.0,
                                    RankingMode::Balanced => 0.4,
                                    RankingMode::RelevanceHeavy => 0.1,
                                };
                                results.sort_by(|a, b| {
                                    let recency = |h: &SearchHit| -> f32 {
                                        if max_created <= 0.0 {
                                            return 0.0;
                                        }
                                        h.created_at.map(|v| v as f32 / max_created).unwrap_or(0.0)
                                    };
                                    let score_a = a.score + alpha * recency(a);
                                    let score_b = b.score + alpha * recency(b);
                                    score_b
                                        .partial_cmp(&score_a)
                                        .unwrap_or(std::cmp::Ordering::Equal)
                                });
                                panes = build_agent_panes(&results, per_pane_limit);
                                if !panes.is_empty()
                                    && let Some(agent) = prev_agent
                                {
                                    if let Some(idx) =
                                        panes.iter().position(|pane| pane.agent == agent)
                                    {
                                        active_pane = idx;
                                        if let Some(path) = prev_path
                                            && let Some(hit_idx) = panes[idx]
                                                .hits
                                                .iter()
                                                .position(|h| h.source_path == path)
                                        {
                                            panes[idx].selected = hit_idx;
                                        }
                                    } else {
                                        active_pane = 0;
                                    }
                                }
                                if panes.is_empty() {
                                    active_pane = 0;
                                }
                                status = format!(
                                    "Page {} | agents:{} | mode:{} | filters a={:?} w={:?} t=({:?},{:?})",
                                    page + 1,
                                    panes.len(),
                                    match match_mode {
                                        MatchMode::Standard => "standard",
                                        MatchMode::Prefix => "prefix",
                                    },
                                    filters.agents,
                                    filters.workspaces,
                                    filters.created_from,
                                    filters.created_to
                                );
                                if !query.trim().is_empty()
                                    && query_history.front().map(|q| q != &query).unwrap_or(true)
                                {
                                    query_history.push_front(query.clone());
                                    if query_history.len() > history_cap {
                                        query_history.pop_back();
                                    }
                                }
                                history_cursor = None;
                                needs_draw = true;
                            }
                        }
                        Err(err) => {
                            dirty_since = None;
                            status = "Search error (see footer).".to_string();
                            tracing::warn!("search error: {err}");
                            results.clear();
                            panes.clear();
                            active_pane = 0;
                            needs_draw = true;
                        }
                    }
                }
            }
            last_tick = Instant::now();
        }
    }

    if let Some(saved) = peek_window_saved.take() {
        context_window = saved;
    }

    let persisted_out = TuiStatePersisted {
        match_mode: Some(match match_mode {
            MatchMode::Standard => "standard".into(),
            MatchMode::Prefix => "prefix".into(),
        }),
        context_window: Some(context_window.label().into()),
    };
    save_state(&state_path, &persisted_out);

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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn state_roundtrip_persists_mode_and_context() {
        let dir = TempDir::new().unwrap();
        let path = state_path_for(dir.path());

        let state = TuiStatePersisted {
            match_mode: Some("prefix".into()),
            context_window: Some("XL".into()),
        };
        save_state(&path, &state);

        let loaded = load_state(&path);
        assert_eq!(loaded.match_mode.as_deref(), Some("prefix"));
        assert_eq!(loaded.context_window.as_deref(), Some("XL"));
    }

    #[test]
    fn contextual_snippet_handles_multibyte_and_short_text() {
        let text = "こんにちは世界"; // 5+2 chars in Japanese
        let out = contextual_snippet(text, "世界", ContextWindow::Small);
        assert!(out.contains("世界"));

        let short = "hi";
        let out_short = contextual_snippet(short, "hi", ContextWindow::XLarge);
        assert_eq!(out_short, "hi");

        let empty_q = contextual_snippet(text, "", ContextWindow::Small);
        assert!(!empty_q.is_empty());
    }
}
