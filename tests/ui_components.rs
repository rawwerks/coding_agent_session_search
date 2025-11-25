use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::widgets::Widget;

use coding_agent_search::ui::components::theme::ThemePalette;
use coding_agent_search::ui::components::widgets::search_bar;
use coding_agent_search::ui::data::InputMode;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Tabs;

#[test]
fn search_bar_tips_include_clear_hotkeys() {
    let palette = ThemePalette::dark();
    let widget = search_bar(
        "test",
        palette,
        InputMode::Query,
        "standard",
        vec![Span::raw("[agent:codex] ")],
    );
    let rect = Rect::new(0, 0, 100, 4);
    let mut buf = Buffer::empty(rect);
    widget.render(rect, &mut buf);

    let lines: Vec<String> = (0..rect.height)
        .map(|y| {
            (0..rect.width)
                .map(|x| buf[(x, y)].symbol().to_string())
                .collect::<Vec<_>>()
                .join("")
        })
        .collect();
    let joined = lines.join("\n");
    eprintln!("bar={joined}");
    // Simplified tips line now shows F1 help, F3-F5 filters, and Ctrl+Del
    assert!(joined.contains("F1"));
    assert!(joined.contains("help"));
    assert!(joined.contains("F3"));
    assert!(joined.contains("agent"));
    assert!(joined.contains("F5"));
    assert!(joined.contains("time"));
    assert!(joined.contains("Ctrl+Del"));
    assert!(joined.contains("clear"));
}

#[test]
fn filter_pills_render_selected_filters() {
    let palette = ThemePalette::dark();
    let chips = vec![
        Span::styled(
            "[agent:codex] ",
            Style::default()
                .fg(palette.accent_alt)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled("[ws:/ws/demo] ", Style::default().fg(palette.accent_alt)),
        Span::styled(
            "[time:Some(100)->Some(200)] ",
            Style::default().fg(palette.accent_alt),
        ),
    ];

    let widget = search_bar("test", palette, InputMode::Query, "standard", chips);
    let rect = Rect::new(0, 0, 100, 4);
    let mut buf = Buffer::empty(rect);
    widget.render(rect, &mut buf);
    let lines: Vec<String> = (0..rect.height)
        .map(|y| {
            (0..rect.width)
                .map(|x| buf[(x, y)].symbol().to_string())
                .collect::<Vec<_>>()
                .join("")
        })
        .collect();
    let joined = lines.join("\n");
    assert!(joined.contains("[agent:codex]"));
    assert!(joined.contains("[ws:/ws/demo]"));
    assert!(joined.contains("[time:Some(100)->Some(200)]"));
}

#[test]
fn detail_tabs_labels_present() {
    let palette = ThemePalette::dark();
    let tabs = ["Messages", "Snippets", "Raw"];
    let tab_titles: Vec<Line> = tabs
        .iter()
        .map(|t| Line::from(Span::styled(*t, palette.title())))
        .collect();
    let widget = Tabs::new(tab_titles);

    let mut buf = Buffer::empty(Rect::new(0, 0, 40, 1));
    widget.render(Rect::new(0, 0, 40, 1), &mut buf);
    let line: String = (0..40).map(|x| buf[(x, 0)].symbol().to_string()).collect();
    eprintln!("tabs={line}");
    assert!(line.contains("Messages"));
    assert!(line.contains("Snippets"));
    assert!(line.contains("Raw"));
}
