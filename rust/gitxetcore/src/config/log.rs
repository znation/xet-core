use crate::config;
use crate::config::ConfigError;
use crate::config::ConfigError::{LogPathNotFile, LogPathReadOnly};
use atty::Stream;
use std::path::{Path, PathBuf};
use tracing::{warn, Level};
use xet_config::Log;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogFormat {
    Compact,
    Json,
}

impl From<Option<&str>> for LogFormat {
    fn from(fmt: Option<&str>) -> Self {
        fmt.map(|s| match s.to_lowercase().as_str() {
            "json" => LogFormat::Json,
            "compact" | "" => LogFormat::Compact,
            _ => {
                warn!(
                    "log.format: {} is not supported, defaulting to `compact`",
                    s
                );
                LogFormat::Compact
            }
        })
        .unwrap_or(if atty::is(Stream::Stderr) {
            LogFormat::Compact
        } else {
            LogFormat::Json
        })
    }
}

#[derive(Debug, Clone)]
pub struct LogSettings {
    pub level: Level,
    pub path: Option<PathBuf>,
    pub format: LogFormat,
    pub with_tracer: bool,
}

impl Default for LogSettings {
    fn default() -> Self {
        Self {
            level: Level::WARN,
            path: None,
            format: LogFormat::Compact,
            with_tracer: false,
        }
    }
}

impl TryFrom<Option<&Log>> for LogSettings {
    type Error = ConfigError;

    fn try_from(log: Option<&Log>) -> Result<Self, Self::Error> {
        fn validate_path(path: &Path) -> Result<(), ConfigError> {
            if path.exists() {
                if !path.is_file() {
                    return Err(LogPathNotFile(path.to_path_buf()));
                } else if !config::util::can_write(path) {
                    return Err(LogPathReadOnly(path.to_path_buf()));
                }
            }
            Ok(())
        }

        Ok(match log {
            Some(log) => {
                let level = match log.level.as_ref() {
                    Some(level) => parse_level(level),
                    None => Level::WARN,
                };
                let path = match log.path.as_ref() {
                    Some(path) if !config::util::is_empty(path) => {
                        validate_path(path)?;
                        Some(path.clone())
                    }
                    _ => None,
                };
                let format = log.format.as_deref().into();
                let with_tracer = log.tracing.unwrap_or(false);
                LogSettings {
                    level,
                    path,
                    format,
                    with_tracer,
                }
            }
            None => LogSettings::default(),
        })
    }
}

fn parse_level(level: &str) -> Level {
    match level.to_lowercase().as_str() {
        "error" => Level::ERROR,
        "warn" => Level::WARN,
        "info" => Level::INFO,
        "debug" => Level::DEBUG,
        "trace" => Level::TRACE,
        _ => Level::WARN,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::{NamedTempFile, TempDir};
    use tokio_test::assert_err;

    #[test]
    fn test_log_format_parse() {
        let mut x: LogFormat = Some("JSON").into();
        assert_eq!(x, LogFormat::Json);
        x = Some("comPact").into();
        assert_eq!(x, LogFormat::Compact);
        x = Some("").into();
        assert_eq!(x, LogFormat::Compact);
        x = Some("other").into();
        assert_eq!(x, LogFormat::Compact);
        x = None.into();
        assert_eq!(x, LogFormat::Compact);
    }

    #[test]
    fn test_log_level_parse() {
        fn check(s: &str, expected_level: Level) {
            assert_eq!(expected_level, parse_level(s));
        }

        check("trace", Level::TRACE);
        check("TRACE", Level::TRACE);
        check("tRaCe", Level::TRACE);
        check("debug", Level::DEBUG);
        check("DEBUG", Level::DEBUG);
        check("dEbUg", Level::DEBUG);
        check("info", Level::INFO);
        check("INFO", Level::INFO);
        check("iNfO", Level::INFO);
        check("warn", Level::WARN);
        check("WARN", Level::WARN);
        check("wArN", Level::WARN);
        check("error", Level::ERROR);
        check("ERROR", Level::ERROR);
        check("eRRoR", Level::ERROR);

        check("default", Level::WARN);
        check("something", Level::WARN);
        check("", Level::WARN);
    }

    #[test]
    fn test_parse() {
        let tmpfile = NamedTempFile::new().unwrap();
        let log_file_path = tmpfile.path().to_path_buf();
        let log_cfg = Log {
            path: Some(log_file_path.clone()),
            level: Some("info".to_string()),
            format: Some("json".to_string()),
            tracing: None,
        };

        let log_settings = LogSettings::try_from(Some(&log_cfg)).unwrap();
        assert_eq!(log_file_path, log_settings.path.unwrap());
        assert_eq!(Level::INFO, log_settings.level);
        assert_eq!(LogFormat::Json, log_settings.format);
        assert!(!log_settings.with_tracer);
    }

    #[test]
    fn test_parse_no_path() {
        let log_cfg = Log {
            path: None,
            format: Some("json".to_string()),
            ..Default::default()
        };

        let log_settings = LogSettings::try_from(Some(&log_cfg)).unwrap();
        assert!(log_settings.path.is_none());
        assert_eq!(Level::WARN, log_settings.level);
        assert_eq!(LogFormat::Json, log_settings.format);
        assert!(!log_settings.with_tracer);
    }

    #[test]
    fn test_parse_none() {
        let log_settings = LogSettings::try_from(None).unwrap();
        assert!(log_settings.path.is_none());
        assert_eq!(Level::WARN, log_settings.level);
        assert_eq!(LogFormat::Compact, log_settings.format);
        assert!(!log_settings.with_tracer);
    }

    #[test]
    fn test_invalid_path() {
        let tmpdir = TempDir::new().unwrap();
        let log_file_path = tmpdir.path().to_path_buf();
        let log_cfg = Log {
            path: Some(log_file_path),
            level: Some("info".to_string()),
            format: Some("json".to_string()),
            tracing: None,
        };

        assert_err!(LogSettings::try_from(Some(&log_cfg)));
    }
}
