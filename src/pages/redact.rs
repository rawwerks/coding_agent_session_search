use regex::Regex;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug, Clone)]
pub struct RedactionConfig {
    /// Redact home directory paths (e.g., /Users/alice -> ~).
    pub redact_home_paths: bool,
    /// Redact usernames in path contexts.
    pub redact_usernames: bool,
    /// Username mappings (real -> fake).
    pub username_map: HashMap<String, String>,
    /// Path prefix replacements.
    pub path_replacements: Vec<(String, String)>,
    /// Custom regex patterns.
    pub custom_patterns: Vec<CustomPattern>,
    /// Preserve structure but anonymize project directory names.
    pub anonymize_project_names: bool,
}

impl Default for RedactionConfig {
    fn default() -> Self {
        Self {
            redact_home_paths: true,
            redact_usernames: true,
            username_map: HashMap::new(),
            path_replacements: Vec::new(),
            custom_patterns: Vec::new(),
            anonymize_project_names: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CustomPattern {
    pub name: String,
    pub pattern: Regex,
    pub replacement: String,
    pub enabled: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RedactionKind {
    HomePath,
    Username,
    PathReplacement,
    CustomPattern,
    ProjectName,
}

impl RedactionKind {
    pub fn label(self) -> &'static str {
        match self {
            RedactionKind::HomePath => "home_path",
            RedactionKind::Username => "username",
            RedactionKind::PathReplacement => "path_replace",
            RedactionKind::CustomPattern => "custom_pattern",
            RedactionKind::ProjectName => "project_name",
        }
    }
}

#[derive(Debug, Clone)]
pub struct RedactionChange {
    pub kind: RedactionKind,
    pub original: String,
    pub redacted: String,
}

#[derive(Debug, Clone)]
pub struct RedactedString {
    pub output: String,
    pub changes: Vec<RedactionChange>,
}

#[derive(Debug, Clone, Default)]
pub struct RedactionReport {
    pub total_redactions: usize,
    pub by_kind: HashMap<RedactionKind, usize>,
    pub samples: Vec<RedactionSample>,
    pub scanned_conversations: usize,
    pub scanned_messages: usize,
    pub truncated: bool,
    max_samples: usize,
}

#[derive(Debug, Clone)]
pub struct RedactionSample {
    pub location: String,
    pub before: String,
    pub after: String,
    pub kinds: Vec<RedactionKind>,
}

pub struct RedactionEngine {
    config: RedactionConfig,
    home_str: Option<String>,
    username_patterns: Vec<(Regex, String)>,
    project_map: Mutex<HashMap<String, String>>,
    project_counter: AtomicUsize,
}

impl RedactionEngine {
    pub fn new(config: RedactionConfig) -> Self {
        let home_dir = directories::UserDirs::new().map(|u| u.home_dir().to_path_buf());
        let home_str = home_dir.as_ref().map(|p| p.to_string_lossy().to_string());

        let username_patterns = build_username_patterns(
            config.redact_usernames,
            &config.username_map,
            home_dir.as_ref(),
        );

        Self {
            config,
            home_str,
            username_patterns,
            project_map: Mutex::new(HashMap::new()),
            project_counter: AtomicUsize::new(0),
        }
    }

    pub fn redact_text(&self, input: &str) -> RedactedString {
        self.redact_internal(input, false)
    }

    pub fn redact_path(&self, input: &str) -> RedactedString {
        self.redact_internal(input, false)
    }

    pub fn redact_workspace(&self, input: &str) -> RedactedString {
        self.redact_internal(input, true)
    }

    fn redact_internal(&self, input: &str, anonymize_project: bool) -> RedactedString {
        let mut output = input.to_string();
        let mut changes = Vec::new();

        if self.config.redact_home_paths
            && let Some(home_str) = &self.home_str
            && output.contains(home_str)
        {
            output = output.replace(home_str, "~");
            changes.push(RedactionChange {
                kind: RedactionKind::HomePath,
                original: home_str.clone(),
                redacted: "~".to_string(),
            });
        }

        if self.config.redact_usernames {
            for (pattern, replacement) in &self.username_patterns {
                if pattern.is_match(&output) {
                    let replaced = pattern.replace_all(&output, |caps: &regex::Captures| {
                        format!("{}{}{}", &caps["prefix"], replacement, &caps["suffix"])
                    });
                    output = replaced.to_string();
                    changes.push(RedactionChange {
                        kind: RedactionKind::Username,
                        original: pattern.as_str().to_string(),
                        redacted: replacement.clone(),
                    });
                }
            }
        }

        for (from, to) in &self.config.path_replacements {
            if output.contains(from) {
                output = output.replace(from, to);
                changes.push(RedactionChange {
                    kind: RedactionKind::PathReplacement,
                    original: from.clone(),
                    redacted: to.clone(),
                });
            }
        }

        for pattern in &self.config.custom_patterns {
            if pattern.enabled && pattern.pattern.is_match(&output) {
                output = pattern
                    .pattern
                    .replace_all(&output, pattern.replacement.as_str())
                    .to_string();
                changes.push(RedactionChange {
                    kind: RedactionKind::CustomPattern,
                    original: pattern.name.clone(),
                    redacted: pattern.replacement.clone(),
                });
            }
        }

        if anonymize_project
            && self.config.anonymize_project_names
            && let Some(redacted) =
                anonymize_last_segment(&output, |name| self.map_project_name(name))
            && redacted != output
        {
            changes.push(RedactionChange {
                kind: RedactionKind::ProjectName,
                original: output.clone(),
                redacted: redacted.clone(),
            });
            output = redacted;
        }

        RedactedString { output, changes }
    }

    fn map_project_name(&self, name: &str) -> String {
        let mut map = self
            .project_map
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(existing) = map.get(name) {
            return existing.clone();
        }

        let next = self.project_counter.fetch_add(1, Ordering::Relaxed) + 1;
        let anonymized = format!("project-{}", next);
        map.insert(name.to_string(), anonymized.clone());
        anonymized
    }
}

impl RedactionReport {
    pub fn new(max_samples: usize) -> Self {
        Self {
            max_samples,
            ..Default::default()
        }
    }

    pub fn record(
        &mut self,
        location: &str,
        before: &str,
        after: &str,
        changes: &[RedactionChange],
    ) {
        if changes.is_empty() {
            return;
        }

        self.total_redactions += changes.len();
        for change in changes {
            *self.by_kind.entry(change.kind).or_insert(0) += 1;
        }

        if self.samples.len() < self.max_samples {
            let mut kinds = Vec::new();
            for change in changes {
                if !kinds.contains(&change.kind) {
                    kinds.push(change.kind);
                }
            }
            self.samples.push(RedactionSample {
                location: location.to_string(),
                before: truncate_for_report(before, 140),
                after: truncate_for_report(after, 140),
                kinds,
            });
        }
    }
}

fn truncate_for_report(input: &str, max: usize) -> String {
    if input.chars().count() <= max {
        return input.to_string();
    }
    let mut out = String::new();
    for (idx, ch) in input.chars().enumerate() {
        if idx + 1 >= max {
            out.push('…');
            break;
        }
        out.push(ch);
    }
    out
}

fn build_username_patterns(
    redact_usernames: bool,
    username_map: &HashMap<String, String>,
    home_dir: Option<&PathBuf>,
) -> Vec<(Regex, String)> {
    if !redact_usernames {
        return Vec::new();
    }

    let mut patterns = Vec::new();

    for (from, to) in username_map {
        if let Some(pattern) = build_username_pattern(from, to) {
            patterns.push(pattern);
        }
    }

    if let Some(home) = home_dir
        && let Some(username) = home.file_name().and_then(|s| s.to_str())
        && let Some(pattern) = build_username_pattern(username, "user")
    {
        patterns.push(pattern);
    }

    patterns
}

fn build_username_pattern(username: &str, replacement: &str) -> Option<(Regex, String)> {
    if username.is_empty() {
        return None;
    }
    let escaped = regex::escape(username);
    let pattern = format!(
        r"(?P<prefix>/Users/|/home/|\\Users\\){}(?P<suffix>[/\\])",
        escaped
    );
    let regex = Regex::new(&pattern).ok()?;
    Some((regex, replacement.to_string()))
}

fn anonymize_last_segment<F>(path: &str, map_name: F) -> Option<String>
where
    F: FnOnce(&str) -> String,
{
    let (sep, idx) = find_last_separator(path)?;
    let last = &path[idx + sep.len_utf8()..];
    if last.is_empty() {
        return None;
    }
    let replacement = map_name(last);
    Some(format!("{}{}", &path[..idx + sep.len_utf8()], replacement))
}

fn find_last_separator(path: &str) -> Option<(char, usize)> {
    let slash_idx = path.rfind('/');
    let backslash_idx = path.rfind('\\');

    match (slash_idx, backslash_idx) {
        (Some(slash), Some(backslash)) => {
            if slash > backslash {
                Some(('/', slash))
            } else {
                Some(('\\', backslash))
            }
        }
        (Some(slash), None) => Some(('/', slash)),
        (None, Some(backslash)) => Some(('\\', backslash)),
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn engine_with_context(home: &str) -> RedactionEngine {
        let config = RedactionConfig::default();
        let home_dir = PathBuf::from(home);
        let home_str = Some(home.to_string());
        let username_patterns = build_username_patterns(
            config.redact_usernames,
            &config.username_map,
            Some(&home_dir),
        );

        RedactionEngine {
            config,
            home_str,
            username_patterns,
            project_map: Mutex::new(HashMap::new()),
            project_counter: AtomicUsize::new(0),
        }
    }

    #[test]
    fn test_home_path_redaction() {
        let engine = engine_with_context("/home/alice");
        let result = engine.redact_text("/home/alice/projects/cass/src/main.rs");
        assert!(result.output.contains("~/projects"));
    }

    #[test]
    fn test_username_redaction_in_paths() {
        let mut engine = engine_with_context("/home/alice");
        engine.config.redact_home_paths = false;
        let result = engine.redact_text("Error in /home/alice/projects/app.rs");
        assert!(result.output.contains("/home/user/"));
    }

    #[test]
    fn test_custom_pattern_redaction() {
        let mut config = RedactionConfig::default();
        config.custom_patterns.push(CustomPattern {
            name: "codename".to_string(),
            pattern: Regex::new(r"Project\s+Falcon").unwrap(),
            replacement: "Project X".to_string(),
            enabled: true,
        });
        let engine = RedactionEngine::new(config);
        let result = engine.redact_text("Working on Project Falcon");
        assert_eq!(result.output, "Working on Project X");
    }

    #[test]
    fn test_project_anonymization() {
        let config = RedactionConfig {
            anonymize_project_names: true,
            ..Default::default()
        };
        let engine = RedactionEngine::new(config);

        let result1 = engine.redact_workspace("/home/alice/project-alpha");
        let result2 = engine.redact_workspace("/home/alice/project-alpha");
        assert!(result1.output.contains("project-1"));
        assert!(result2.output.contains("project-1"));
    }

    #[test]
    fn test_report_records_changes() {
        let engine = engine_with_context("/home/alice");
        let result = engine.redact_text("/home/alice/projects/app.rs");
        let mut report = RedactionReport::new(2);

        report.record(
            "message.content",
            "/home/alice/projects/app.rs",
            &result.output,
            &result.changes,
        );

        assert!(report.total_redactions > 0);
        assert!(!report.samples.is_empty());
    }
}
