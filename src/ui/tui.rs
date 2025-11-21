//! Ratatui-based interface wired to Tantivy search.

use anyhow::Result;
use crossterm::event::{self, Event, KeyCode};
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use crossterm::{ExecutableCommand, execute};
use ratatui::prelude::*;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, List, ListItem, ListState, Paragraph, Wrap};
use std::io;
use std::time::{Duration, Instant};

use crate::search::query::{SearchClient, SearchFilters, SearchHit};
use crate::search::tantivy::index_dir;
use crate::ui::components::widgets::search_bar;
use crate::{default_data_dir, default_db_path};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum InputMode {
    Query,
    Agent,
    Workspace,
    CreatedFrom,
    CreatedTo,
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

    let mut selected: Option<usize> = None;

    loop {
        terminal.draw(|f| {
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .margin(1)
                .constraints(
                    [
                        Constraint::Length(3),
                        Constraint::Min(0),
                        Constraint::Length(1),
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
                            Span::styled(title, Style::default().fg(Color::Cyan)),
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
                .split(chunks[1]);

            f.render_stateful_widget(
                List::new(items).block(list_block),
                split[0],
                &mut list_state,
            );

            let detail = selected.and_then(|idx| results.get(idx)).map(|hit| {
                let mut lines = Vec::new();
                lines.push(Line::from(vec![
                    Span::styled("Title: ", Style::default().fg(Color::Yellow)),
                    Span::raw(hit.title.clone()),
                ]));
                lines.push(Line::from(format!("Agent: {}", hit.agent)));
                lines.push(Line::from(format!("Workspace: {}", hit.workspace)));
                lines.push(Line::from(format!("Source: {}", hit.source_path)));
                lines.push(Line::from(format!("Score: {:.2}", hit.score)));
                lines.push(Line::from(""));
                lines.push(Line::from(hit.snippet.clone()));
                Paragraph::new(lines)
                    .block(Block::default().title("Detail").borders(Borders::ALL))
                    .wrap(Wrap { trim: true })
            });
            if let Some(widget) = detail {
                f.render_widget(widget, split[1]);
            } else {
                f.render_widget(
                    Paragraph::new("Select a result to view details")
                        .block(Block::default().title("Detail").borders(Borders::ALL)),
                    split[1],
                );
            }

            let footer = Paragraph::new(status.as_str());
            f.render_widget(footer, chunks[2]);
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
                    }
                    KeyCode::PageDown | KeyCode::Char(']') => {
                        page = page.saturating_add(1);
                    }
                    KeyCode::PageUp | KeyCode::Char('[') => {
                        page = page.saturating_sub(1);
                    }
                    KeyCode::Char(c) => {
                        query.push(c);
                        page = 0;
                        selected = None;
                    }
                    KeyCode::Backspace => {
                        query.pop();
                        page = 0;
                        selected = None;
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
                if !query.trim().is_empty() {
                    match client.search(&query, filters.clone(), page_size, page * page_size) {
                        Ok(hits) => {
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
                            status = format!("Search error: {err}");
                            results.clear();
                        }
                    }
                } else {
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
