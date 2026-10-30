use serde_json::json;
use std::fmt;
use std::io::{self, Write};
use std::sync::OnceLock;
use tracing::{Event, Subscriber};
use tracing_subscriber::fmt::format::{Format, JsonFields, Writer};
use tracing_subscriber::fmt::time::SystemTime;
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields, MakeWriter};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::EnvFilter;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const GIT_REVISION: Option<&str> = option_env!("BTFIND_GIT_REVISION");

static RUN_ID: OnceLock<String> = OnceLock::new();

#[derive(Clone, Copy)]
pub enum LogOutput {
    Stdout,
    Stderr,
}

pub enum LogWriter {
    Stdout(io::Stdout),
    Stderr(io::Stderr),
}

impl Write for LogWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        match self {
            LogWriter::Stdout(writer) => writer.write(buffer),
            LogWriter::Stderr(writer) => writer.write(buffer),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            LogWriter::Stdout(writer) => writer.flush(),
            LogWriter::Stderr(writer) => writer.flush(),
        }
    }
}

impl<'a> MakeWriter<'a> for LogOutput {
    type Writer = LogWriter;

    fn make_writer(&'a self) -> Self::Writer {
        match self {
            LogOutput::Stdout => LogWriter::Stdout(io::stdout()),
            LogOutput::Stderr => LogWriter::Stderr(io::stderr()),
        }
    }
}

struct ServiceFormatter {
    inner: Format<tracing_subscriber::fmt::format::Json, SystemTime>,
    service: &'static str,
    run_id: &'static str,
}

impl<S, N> FormatEvent<S, N> for ServiceFormatter
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        context: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let mut buffer = String::new();
        self.inner
            .format_event(context, Writer::new(&mut buffer), event)?;
        let trimmed = buffer.trim_end_matches(['\r', '\n']);

        match serde_json::from_str::<serde_json::Value>(trimmed) {
            Ok(serde_json::Value::Object(mut object)) => {
                object.insert("service".to_string(), json!(self.service));
                object.insert("version".to_string(), json!(VERSION));
                object.insert("run_id".to_string(), json!(self.run_id));
                if let Some(revision) = GIT_REVISION {
                    object.insert("revision".to_string(), json!(revision));
                }
                let rendered = serde_json::to_string(&serde_json::Value::Object(object))
                    .map_err(|_| fmt::Error)?;
                writeln!(writer, "{rendered}")
            }
            _ => writeln!(writer, "{trimmed}"),
        }
    }
}

pub fn init(service: &'static str, output: LogOutput) {
    let run_id: &'static str = RUN_ID
        .get_or_init(|| uuid::Uuid::new_v4().to_string())
        .as_str();
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("btfind=info"));
    let formatter = ServiceFormatter {
        inner: Format::default().json(),
        service,
        run_id,
    };

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(output)
        .fmt_fields(JsonFields::new())
        .event_format(formatter)
        .init();
}

pub fn install_panic_hook() {
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let location = info
            .location()
            .map(|location| format!("{}:{}", location.file(), location.line()))
            .unwrap_or_else(|| "unknown".to_string());
        tracing::error!(
            event = "process_panic",
            panic = %info,
            location = %location,
            "process panicked"
        );
        previous(info);
    }));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_is_available() {
        assert_eq!(VERSION, env!("CARGO_PKG_VERSION"));
    }
}
