//! The `webtronix` command line: a thin front end over the [`webtronix`]
//! library that reads the arguments and config file, routes logging through
//! a progress bar and runs one [`Index::update`].
//!
//! Every option can also be set in a TOML config file (`--config`, or
//! `webtronix.toml` in the working directory when it exists) under the name
//! of its long flag. The command line wins over the file and the file over
//! the built-in default; environment variables are never consulted.

use std::{
    io::{self, Write},
    panic,
    path::{Path, PathBuf},
    time::Duration,
};

use clap::Parser;
use eyre::{Context, Result, bail};
use indicatif::{ProgressBar, ProgressStyle};
use serde::Deserialize;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;
use webtronix::{Format, Index, Options, Render, read_urls};

/// Extract the text of a list of web pages and PDFs into a SQLite index.
///
/// Every option below can also be set in the config file under the name of
/// its long flag. An option given here wins over the file, and the file
/// over the default shown.
#[derive(Parser)]
#[command(version, about)]
struct Args {
    /// File containing one URL per line. Blank lines and `#` comments are ignored.
    urls: PathBuf,
    /// TOML file of defaults for the options below. [default:
    /// webtronix.toml in the working directory, when it exists]
    #[arg(long, value_name = "PATH")]
    config: Option<PathBuf>,
    /// SQLite database to write. An existing index is reused to make
    /// conditional requests so unchanged pages are not re-downloaded.
    /// [default: index.db]
    #[arg(short, long)]
    output: Option<PathBuf>,
    /// Maximum number of in-flight requests. [default: 16]
    #[arg(short, long)]
    concurrency: Option<usize>,
    /// Per-request timeout in seconds. Also bounds each browser render.
    /// [default: 30]
    #[arg(short, long)]
    timeout: Option<u64>,
    /// What to store for each HTML page: its extracted text, or the HTML
    /// itself (as served, or as rendered when `--render` applies). PDFs are
    /// converted to text either way. [default: text]
    #[arg(short, long, value_enum)]
    format: Option<Format>,
    /// When to load an HTML page in a headless browser so its JavaScript
    /// runs before the page is stored or its text extracted. `auto` renders
    /// only pages whose served HTML looks like a script shell. Needs Chrome
    /// or Chromium. [default: auto]
    #[arg(long, value_enum)]
    render: Option<Render>,
    /// Chrome or Chromium binary to render with. [default: the usual names
    /// on the path and installation directories are tried]
    #[arg(long, value_name = "PATH")]
    chrome: Option<PathBuf>,
    /// Log filter, as directives such as `info` or `webtronix=debug,warn`.
    /// Replaces the level chosen by `--verbose` and `--quiet`.
    #[arg(long, value_name = "FILTER")]
    log: Option<String>,
    /// Log each request.
    #[arg(short, long)]
    verbose: bool,
    /// Suppress the progress bar and all logging.
    #[arg(short, long, conflicts_with = "verbose")]
    quiet: bool,
}

/// Where the config file is looked for when `--config` is not given.
const CONFIG_FILE: &str = "webtronix.toml";

/// The config file. Each key is the long name of the option it sets, as in
/// [`Args`], and takes the same values. Unknown keys are an error so a
/// misspelt one is not silently ignored.
#[derive(Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Config {
    output: Option<PathBuf>,
    concurrency: Option<usize>,
    timeout: Option<u64>,
    format: Option<Format>,
    render: Option<Render>,
    chrome: Option<PathBuf>,
    log: Option<String>,
    verbose: Option<bool>,
    quiet: Option<bool>,
}

/// Read the config file at `path`. A missing file is an error only when
/// `required`, which is the case for a path named on the command line but
/// not for the implicit [`CONFIG_FILE`].
async fn load_config(path: &Path, required: bool) -> Result<Config> {
    let text = match tokio::fs::read_to_string(path).await {
        Ok(text) => text,
        Err(e) if e.kind() == io::ErrorKind::NotFound && !required => {
            return Ok(Config::default());
        }
        Err(e) => {
            return Err(e)
                .wrap_err_with(|| format!("reading {}", path.display()));
        }
    };
    parse_config(&text).wrap_err_with(|| format!("parsing {}", path.display()))
}

/// Parse the text of a config file.
fn parse_config(text: &str) -> Result<Config> {
    Ok(toml::from_str(text)?)
}

/// What one run uses, each value resolved from the command line, then the
/// config file, then the built-in default.
#[derive(Debug, PartialEq, Eq)]
struct Settings {
    output: PathBuf,
    options: Options,
    /// Filter directives for the logger.
    log: String,
    /// Hide the progress bar.
    quiet: bool,
}

impl Settings {
    /// Combine the command line with the config file.
    fn resolve(args: &Args, config: Config) -> Result<Self> {
        if config.verbose == Some(true) && config.quiet == Some(true) {
            bail!("config sets both verbose and quiet");
        }
        // Either flag on the command line overrides both keys in the file,
        // since the two conflict and the command line decides.
        let (verbose, quiet) = if args.verbose || args.quiet {
            (args.verbose, args.quiet)
        } else {
            (
                config.verbose.unwrap_or(false),
                config.quiet.unwrap_or(false),
            )
        };
        let log_level = match (verbose, quiet) {
            (true, _) => "info",
            (_, true) => "off",
            _ => "error",
        };
        let mut log = args
            .log
            .clone()
            .or(config.log)
            .unwrap_or_else(|| log_level.to_owned());
        // chromiumoxide warns about every protocol message newer than its
        // generated types, which a current Chrome sends constantly;
        // harmless, so it is quietened unless the filter says otherwise.
        if !log.contains("chromiumoxide") {
            log.push_str(",chromiumoxide=error");
        }
        let defaults = Options::default();
        Ok(Self {
            output: args
                .output
                .clone()
                .or(config.output)
                .unwrap_or_else(|| "index.db".into()),
            options: Options {
                concurrency: args
                    .concurrency
                    .or(config.concurrency)
                    .unwrap_or(defaults.concurrency),
                timeout: args
                    .timeout
                    .or(config.timeout)
                    .map_or(defaults.timeout, Duration::from_secs),
                format: args.format.or(config.format).unwrap_or_default(),
                render: args.render.or(config.render).unwrap_or_default(),
                chrome: args.chrome.clone().or(config.chrome),
                ..defaults
            },
            log,
            quiet,
        })
    }
}

/// Routes log lines through the progress bar so they do not tear it. When
/// the bar is hidden (not a terminal, or `--quiet`) they go straight to stderr.
struct BarWriter(ProgressBar);

impl Write for BarWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.0.is_hidden() {
            return io::stderr().write(buf);
        }
        self.0.println(String::from_utf8_lossy(buf).trim_end());
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let config = match &args.config {
        Some(path) => load_config(path, true).await?,
        None => load_config(Path::new(CONFIG_FILE), false).await?,
    };
    let settings = Settings::resolve(&args, config)?;
    let urls = read_urls(&args.urls).await?;

    let bar = if settings.quiet {
        ProgressBar::hidden()
    } else {
        ProgressBar::new(urls.len() as u64)
    };
    bar.set_style(ProgressStyle::with_template(
        "{wide_bar} {pos}/{len} [{elapsed_precise}, eta {eta}]",
    )?);
    let log_filter = EnvFilter::try_new(&settings.log)
        .wrap_err_with(|| format!("log filter {:?}", settings.log))?;
    let log_writer = bar.clone();
    tracing_subscriber::fmt()
        .with_env_filter(log_filter)
        .with_writer(move || BarWriter(log_writer.clone()))
        .init();
    // Panics (the PDF parser has a few on malformed input) are reported
    // through the logger so they do not tear the bar; the library catches
    // the one that raised them so only that URL is skipped.
    panic::set_hook(Box::new(|info| {
        let location = info
            .location()
            .map(|l| format!("{}:{}", l.file(), l.line()))
            .unwrap_or_default();
        let message =
            info.payload_as_str().unwrap_or("non-string panic payload");
        error!(location, "panic: {message}");
    }));

    let index = Index::open(&settings.output)?;
    let summary = index
        .update(&urls, &settings.options, |_| bar.inc(1))
        .await?;
    bar.finish_and_clear();
    info!(
        written = summary.written,
        pruned = summary.pruned,
        path = %settings.output.display(),
        "updated index"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Parse a command line as `main` would, without the program name.
    fn args(line: &[&str]) -> Args {
        Args::try_parse_from(["webtronix", "urls.txt"].iter().chain(line))
            .expect("valid command line")
    }

    #[test]
    fn config_parses_every_key() {
        let config = parse_config(
            r#"
            output = "out.db"
            concurrency = 8
            timeout = 5
            format = "html"
            render = "never"
            chrome = "/usr/bin/chromium"
            log = "debug"
            verbose = true
            quiet = false
            "#,
        )
        .unwrap();
        assert_eq!(
            config,
            Config {
                output: Some("out.db".into()),
                concurrency: Some(8),
                timeout: Some(5),
                format: Some(Format::Html),
                render: Some(Render::Never),
                chrome: Some("/usr/bin/chromium".into()),
                log: Some("debug".into()),
                verbose: Some(true),
                quiet: Some(false),
            }
        );
        assert_eq!(parse_config("").unwrap(), Config::default());
    }

    #[test]
    fn config_rejects_unknown_keys_and_bad_values() {
        assert!(parse_config("concurency = 4").is_err());
        assert!(parse_config(r#"render = "sometimes""#).is_err());
        assert!(parse_config("timeout = -1").is_err());
    }

    #[test]
    fn command_line_beats_config_beats_defaults() {
        let config = Config {
            concurrency: Some(8),
            timeout: Some(5),
            format: Some(Format::Html),
            ..Config::default()
        };
        let settings =
            Settings::resolve(&args(&["-c", "4", "--render", "never"]), config)
                .unwrap();
        assert_eq!(
            settings,
            Settings {
                output: "index.db".into(),
                options: Options {
                    concurrency: 4,
                    timeout: Duration::from_secs(5),
                    format: Format::Html,
                    render: Render::Never,
                    ..Options::default()
                },
                log: "error,chromiumoxide=error".into(),
                quiet: false,
            }
        );
    }

    #[test]
    fn log_filter_comes_from_flags_then_file_then_verbosity() {
        let log = |line: &[&str], config: Config| {
            Settings::resolve(&args(line), config).unwrap().log
        };
        assert_eq!(log(&[], Config::default()), "error,chromiumoxide=error");
        assert_eq!(log(&["-v"], Config::default()), "info,chromiumoxide=error");
        let verbose = Config {
            verbose: Some(true),
            ..Config::default()
        };
        assert_eq!(log(&[], verbose), "info,chromiumoxide=error");
        // A flag overrides the file's verbosity even though they conflict.
        let verbose = Config {
            verbose: Some(true),
            ..Config::default()
        };
        assert_eq!(log(&["-q"], verbose), "off,chromiumoxide=error");
        let filtered = Config {
            log: Some("warn".into()),
            ..Config::default()
        };
        assert_eq!(log(&["-v"], filtered), "warn,chromiumoxide=error");
        let explicit = Config {
            log: Some("warn".into()),
            ..Config::default()
        };
        assert_eq!(
            log(&["--log", "webtronix=debug,chromiumoxide=warn"], explicit),
            "webtronix=debug,chromiumoxide=warn"
        );
        let both = Config {
            verbose: Some(true),
            quiet: Some(true),
            ..Config::default()
        };
        assert!(Settings::resolve(&args(&[]), both).is_err());
    }

    #[tokio::test]
    async fn missing_config_file_is_fine_only_when_implicit() {
        let dir = std::env::temp_dir()
            .join(format!("webtronix-config-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let missing = dir.join("missing.toml");
        assert_eq!(
            load_config(&missing, false).await.unwrap(),
            Config::default()
        );
        assert!(load_config(&missing, true).await.is_err());
        let present = dir.join(CONFIG_FILE);
        std::fs::write(&present, "timeout = 7").unwrap();
        assert_eq!(
            load_config(&present, false).await.unwrap().timeout,
            Some(7)
        );
        std::fs::write(&present, "timeout = [").unwrap();
        assert!(load_config(&present, false).await.is_err());
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
