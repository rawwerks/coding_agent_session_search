//! Theme definitions.

use ratatui::style::{Color, Modifier, Style};

#[derive(Clone, Copy)]
pub struct ThemePalette {
    pub accent: Color,
    pub accent_alt: Color,
    pub bg: Color,
    pub fg: Color,
    pub surface: Color,
    pub hint: Color,
    pub user: Color,
    pub agent: Color,
    pub tool: Color,
    pub system: Color,
}

impl ThemePalette {
    pub fn light() -> Self {
        Self {
            accent: Color::Cyan,
            accent_alt: Color::LightBlue,
            bg: Color::White,
            fg: Color::Black,
            surface: Color::Gray,
            hint: Color::DarkGray,
            user: Color::Green,
            agent: Color::Blue,
            tool: Color::Magenta,
            system: Color::Yellow,
        }
    }

    pub fn dark() -> Self {
        Self {
            accent: Color::Cyan,
            accent_alt: Color::Blue,
            bg: Color::Black,
            fg: Color::White,
            surface: Color::DarkGray,
            hint: Color::Gray,
            user: Color::Green,
            agent: Color::Cyan,
            tool: Color::Magenta,
            system: Color::Yellow,
        }
    }

    pub fn title(self) -> Style {
        Style::default()
            .fg(self.accent)
            .add_modifier(Modifier::BOLD)
    }
}
