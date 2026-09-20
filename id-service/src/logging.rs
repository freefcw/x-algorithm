//! Process logging setup, aligned with Home Mixer.
//!
//! Filtering stays on `RUST_LOG`. `ID_REGISTRY_LOG_FORMAT` chooses between the
//! human-readable `env_logger` layout (`text`, the default) and one JSON
//! object per line (`json`) for log collectors.

use std::io::Write;

const FORMAT_ENV: &str = "ID_REGISTRY_LOG_FORMAT";

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

/// Install the global logger from `RUST_LOG` and `ID_REGISTRY_LOG_FORMAT`.
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
/// `level`, `target`, `msg`, plus `module`, `file` and `line` when present.
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

/// Hide the userinfo (`user:password@`) of a connection URL so it can be
/// logged. Anything without a `scheme://userinfo@host` shape is returned
/// unchanged.
pub fn redact_url(url: &str) -> String {
    let Some(scheme_end) = url.find("://") else {
        return url.to_string();
    };
    let authority_start = scheme_end + 3;
    let authority_end = url[authority_start..]
        .find('/')
        .map_or(url.len(), |offset| authority_start + offset);
    let authority = &url[authority_start..authority_end];
    match authority.rfind('@') {
        Some(at) => format!(
            "{}***@{}{}",
            &url[..authority_start],
            &authority[at + 1..],
            &url[authority_end..]
        ),
        None => url.to_string(),
    }
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
        let line = json_line(
            &log::Record::builder()
                .level(log::Level::Warn)
                .target("id_service::redis_registry")
                .module_path(Some("id_service::redis_registry"))
                .file(Some("redis_registry.rs"))
                .line(Some(42))
                .args(format_args!(
                    "orphan reverse mapping \"quoted\" snowflake=1"
                ))
                .build(),
            "2026-09-20T08:00:00.000Z",
        );
        let parsed: serde_json::Value = serde_json::from_str(&line).expect("valid JSON");
        assert_eq!(parsed["ts"], "2026-09-20T08:00:00.000Z");
        assert_eq!(parsed["level"], "WARN");
        assert_eq!(parsed["target"], "id_service::redis_registry");
        assert_eq!(
            parsed["msg"],
            "orphan reverse mapping \"quoted\" snowflake=1"
        );
        assert_eq!(parsed["line"], 42);
        assert!(!line.contains('\n'));
    }

    #[test]
    fn redaction_hides_userinfo_only() {
        assert_eq!(
            redact_url("redis://user:secret@redis.internal:6379/0"),
            "redis://***@redis.internal:6379/0"
        );
        assert_eq!(
            redact_url("rediss://:p%40ss@host:6380"),
            "rediss://***@host:6380"
        );
        assert_eq!(
            redact_url("redis://redis.internal:6379/"),
            "redis://redis.internal:6379/"
        );
        assert_eq!(redact_url("not a url"), "not a url");
        // A password containing `/` after the userinfo is still hidden as
        // long as the userinfo precedes the first path separator.
        assert_eq!(redact_url("redis://u:p@h:1/db@2"), "redis://***@h:1/db@2");
    }
}
