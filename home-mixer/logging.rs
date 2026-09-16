//! Process logging setup shared by the server and the projection job.
//!
//! Filtering stays on `RUST_LOG` as before. `HOME_MIXER_LOG_FORMAT` chooses
//! between the human-readable `env_logger` layout (`text`, the default) and
//! one JSON object per line (`json`) for log collectors. The JSON layout keeps
//! the message verbatim: pipeline lines already carry `request_id=...` and
//! `elapsed_ms=...` pairs, and collectors can extract those from `msg`
//! without a second format on the Rust side.

use std::io::Write;

const FORMAT_ENV: &str = "HOME_MIXER_LOG_FORMAT";

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum LogFormat {
    #[default]
    Text,
    Json,
}

impl LogFormat {
    fn parse(value: Option<&str>) -> anyhow::Result<Self> {
        match value
            .map(|value| value.trim().to_ascii_lowercase())
            .as_deref()
        {
            None | Some("") | Some("text") => Ok(Self::Text),
            Some("json") => Ok(Self::Json),
            Some(other) => anyhow::bail!("invalid {FORMAT_ENV}={other:?}; expected text or json"),
        }
    }
}

/// Install the global logger from `RUST_LOG` and `HOME_MIXER_LOG_FORMAT`.
pub fn init_from_env() -> anyhow::Result<()> {
    let format = LogFormat::parse(std::env::var(FORMAT_ENV).ok().as_deref())?;
    init(format)
}

pub fn init(format: LogFormat) -> anyhow::Result<()> {
    let mut builder = env_logger::Builder::from_default_env();
    if format == LogFormat::Json {
        builder.format(|buffer, record| {
            writeln!(
                buffer,
                "{}",
                json_line(record, &buffer.timestamp_millis().to_string())
            )
        });
    }
    builder
        .try_init()
        .map_err(|error| anyhow::anyhow!("failed to install the logger: {error}"))
}

/// One JSON object for a log record: `ts` (RFC 3339, millisecond precision),
/// `level`, `target`, `msg`, plus `module`, `file` and `line` when the record
/// carries them.
fn json_line(record: &log::Record<'_>, timestamp: &str) -> String {
    let mut line = serde_json::Map::new();
    line.insert("ts".into(), timestamp.into());
    line.insert("level".into(), record.level().as_str().into());
    line.insert("target".into(), record.target().into());
    line.insert("msg".into(), record.args().to_string().into());
    if let Some(module) = record.module_path() {
        line.insert("module".into(), module.into());
    }
    if let Some(file) = record.file() {
        line.insert("file".into(), file.into());
    }
    if let Some(line_number) = record.line() {
        line.insert("line".into(), line_number.into());
    }
    serde_json::Value::Object(line).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_defaults_to_text_and_rejects_unknown_values() {
        assert_eq!(LogFormat::parse(None).unwrap(), LogFormat::Text);
        assert_eq!(LogFormat::parse(Some("")).unwrap(), LogFormat::Text);
        assert_eq!(LogFormat::parse(Some(" TEXT ")).unwrap(), LogFormat::Text);
        assert_eq!(LogFormat::parse(Some("json")).unwrap(), LogFormat::Json);
        assert!(LogFormat::parse(Some("logfmt")).is_err());
    }

    #[test]
    fn json_lines_escape_the_message_and_keep_the_location() {
        // Built and consumed in one statement: `Record` borrows the
        // `format_args!` temporary.
        let line = json_line(
            &log::Record::builder()
                .level(log::Level::Warn)
                .target("home_mixer::rpc_policy")
                .module_path(Some("home_mixer::rpc_policy"))
                .file(Some("rpc_policy.rs"))
                .line(Some(42))
                .args(format_args!(
                    "request_id=r-1 \"quoted\" deadline_exceeded budget_ms=10"
                ))
                .build(),
            "2026-09-16T08:00:00.000Z",
        );
        let parsed: serde_json::Value = serde_json::from_str(&line).expect("valid JSON");
        assert_eq!(parsed["ts"], "2026-09-16T08:00:00.000Z");
        assert_eq!(parsed["level"], "WARN");
        assert_eq!(parsed["target"], "home_mixer::rpc_policy");
        assert_eq!(
            parsed["msg"],
            "request_id=r-1 \"quoted\" deadline_exceeded budget_ms=10"
        );
        assert_eq!(parsed["file"], "rpc_policy.rs");
        assert_eq!(parsed["line"], 42);
        assert!(!line.contains('\n'));
    }
}
