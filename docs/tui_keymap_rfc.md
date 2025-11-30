# TUI Interaction Model & Keymap RFC (cass)

Status: draft  
Owner: RedHill  
Related issues: coding_agent_session_search-002 (depends on style spec coding_agent_session_search-001)

## Principles
- Keyboard-first; mouse optional, never required.
- Consistency: same chord does the same thing across panes; avoid mode confusion.
- Discoverability: contextual help strip + command palette make actions findable.
- Safety: no destructive actions without confirmation; ESC always exits modals/search.
- Terminal resilience: graceful degradation on limited key support (esp. Ctrl+arrows, function keys).

## Global Keymap
- `Ctrl+P` — Command palette (fuzzy actions)
- `?` — Quick tour / help overlay
- `F1` — Toggle help strip pin/unpin (if available)
- `Esc` — Close modal/overlay; clear inline searches; cancel prompts
- `Tab` / `Shift+Tab` — Cycle focus panes (search → results → detail → footer/help)
- `Ctrl+S` — Save view (see Saved Views)
- `Ctrl+R` — Reload index/view state (non-destructive)
- `F12` — Cycle ranking mode (recent / balanced / relevance / quality)
- `Ctrl+B` — Toggle rounded/plain borders
- `D` — Cycle density (Compact/Cozy/Spacious)
- `g` / `G` — Jump to top/bottom of list
- `PageUp/PageDown` or `Ctrl+U/Ctrl+D` — Scroll results pane by page

## Search Bar
- Direct typing — live query
- `Up/Down` — Navigate query history
- `Ctrl+L` — Clear search query
- `Ctrl+W` — Delete last token
- `Enter` — Run search immediately (forces query even during debounce)
- `Ctrl+F` — Toggle wildcard fallback indicator (UI only; does not change query)

## Filters & Pills
- `F3` — Agent filter picker
- `F4` — Workspace filter picker
- `F5` — Time filter cycle (today/week/30d/all)
- `F6` — Custom date range prompt
- `F10` — Ranking mode cycle (alias of F12 if function keys limited)
- In pills: `Enter` to edit, `Backspace` to remove, `Left/Right` to move between pills
- Mouse: click pill to edit/remove

## Results Pane
- `Up/Down` — Move selection
- `Enter` — Open drill-in modal (thread view)
- `m` — Toggle multi-select
- `Space` — Peek XL context (tap again to restore)
- `A` — Bulk actions menu (open all, copy paths, export JSON, tag)
- `/` — In-pane quick filter; `Esc` clears
- `y` — Copy current snippet to clipboard
- `o` — Open source file in $EDITOR
- `v` — View raw source (non-interactive)
- `r` — Refresh results (re-run query)

## Detail Pane (Drill-In Modal)
- `Left/Right` — Switch tabs (Messages / Snippets / Raw)
- `c` — Copy path
- `y` — Copy selected snippet
- `o` — Open in $EDITOR
- `f` — Toggle wrap in detail view
- `Esc` — Close modal, return focus to results

## Saved Views
- `Ctrl+1..9` — Save current filters/ranking to slot
- `Shift+1..9` — Recall slot
- Toast confirms save/load; errors surface in footer

## Density & Theme
- `D` — Cycle density presets
- `Ctrl+T` — Toggle theme (dark/light)
- `F2` — Theme toggle (legacy alias)

## Update Assistant
- When banner shown: `U` upgrade, `s` skip this version, `d` view notes, `Esc` dismiss

## Minimal / Robot Mode Behavior
- When `TUI_HEADLESS=1` or stdout non-TTY: disable command palette, animation, icons; only allow `search`/`stats`/`view` via CLI.
- Shortcut hints hidden; actions reachable via flags.

## Fallbacks for Limited Key Support
- If function keys unavailable: map F3/F4/F5/F6 to `Ctrl+3/4/5/6`.
- If Ctrl+P conflicts: palette alias `Alt+P`.
- If clipboard unsupported: `y` writes to temporary file path displayed in footer.

## Mouse (optional)
- Click focus between panes.
- Scroll wheel scrolls results/detail.
- Click pill to edit/remove; click breadcrumb to change scope.
- Drag not required anywhere.

## Safety / Destructive Actions
- Bulk operations that open files only; no delete actions exist.
- Any future destructive command must confirm via y/N prompt; default No.

## Acceptance (coding_agent_session_search-002)
- Keymap is conflict-free, discoverable (help strip + palette), and defined for degraded terminals.
- ESC always backs out safely; no orphaned modal states.
- Saved view, density, and theme toggles have keybindings and documented fallbacks.
- Update assistant keys defined.
