//! Fetch a list of URLs concurrently, extract the visible text of each page
//! and store the result in a SQLite index on disk.
//!
//! The index is a single `pages` table keyed by URL holding the title, the
//! extracted text and the HTTP validators used for conditional requests.
//!
//! Bandwidth is kept down by deduplicating the input, accepting compressed
//! responses, refusing non-HTML bodies before they are read, and sending
//! conditional requests (`If-None-Match` / `If-Modified-Since`) based on the
//! existing index so unchanged pages are not downloaded again.

use std::{
    collections::HashMap,
    io::{self, Write},
    path::{Path, PathBuf},
    time::Duration,
};

use clap::Parser;
use eyre::{Context, Result, ensure};
use futures::{StreamExt, stream};
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::{Client, StatusCode, header};
use rusqlite::{Connection, params};
use scraper::{Html, Selector};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

/// Extract the text of a list of web pages into a SQLite index.
#[derive(Parser)]
#[command(version, about)]
struct Args {
    /// File containing one URL per line. Blank lines and `#` comments are ignored.
    urls: PathBuf,
    /// SQLite database to write. An existing index is reused to make
    /// conditional requests so unchanged pages are not re-downloaded.
    #[arg(short, long, default_value = "index.db")]
    output: PathBuf,
    /// Maximum number of in-flight requests.
    #[arg(short, long, default_value_t = 16)]
    concurrency: usize,
    /// Per-request timeout in seconds.
    #[arg(short, long, default_value_t = 30)]
    timeout: u64,
    /// Log each request. `RUST_LOG` overrides the level.
    #[arg(short, long)]
    verbose: bool,
    /// Suppress the progress bar and all logging.
    #[arg(short, long, conflicts_with = "verbose")]
    quiet: bool,
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

/// HTTP validators remembered for a URL, used for conditional requests.
#[derive(Clone, Default)]
struct Validators {
    etag: Option<String>,
    last_modified: Option<String>,
}

/// A freshly fetched page; one row of the `pages` table.
struct Page {
    url: String,
    title: Option<String>,
    text: String,
    validators: Validators,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let urls = read_urls(&args.urls).await?;

    let bar = if args.quiet {
        ProgressBar::hidden()
    } else {
        ProgressBar::new(urls.len() as u64)
    };
    bar.set_style(ProgressStyle::with_template(
        "{wide_bar} {pos}/{len} [{elapsed_precise}, eta {eta}]",
    )?);
    let log_level = if args.verbose { "info" } else { "off" };
    let log_writer = bar.clone();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| log_level.into()),
        )
        .with_writer(move || BarWriter(log_writer.clone()))
        .init();

    let mut db = open_index(&args.output)?;
    let previous = load_validators(&db)?;
    let client = Client::builder()
        .user_agent(concat!(
            env!("CARGO_PKG_NAME"),
            "/",
            env!("CARGO_PKG_VERSION")
        ))
        .timeout(Duration::from_secs(args.timeout))
        .gzip(true)
        .brotli(true)
        .build()?;

    let mut results = stream::iter(urls.iter().cloned())
        .map(|url| {
            let client = &client;
            let cached = previous.get(&url);
            async move {
                match fetch(client, &url, cached).await {
                    Ok(page) => page,
                    Err(e) if cached.is_some() => {
                        warn!(url, "keeping previous entry: {e:#}");
                        None
                    }
                    Err(e) => {
                        warn!(url, "skipping: {e:#}");
                        None
                    }
                }
            }
        })
        .buffer_unordered(args.concurrency);

    // Each page is written as soon as it lands; unchanged and failed pages
    // leave their existing row alone.
    let mut written = 0;
    while let Some(page) = results.next().await {
        bar.inc(1);
        let Some(page) = page else { continue };
        db = tokio::task::spawn_blocking(move || {
            upsert(&db, &page).map(|()| db)
        })
        .await??;
        written += 1;
    }
    let pruned = prune(&db, &urls)?;
    bar.finish_and_clear();
    info!(written, pruned, path = %args.output.display(), "updated index");
    Ok(())
}

/// Fetch and extract one page, sending a conditional request when validators
/// are available. Returns `None` on `304 Not Modified`.
async fn fetch(
    client: &Client,
    url: &str,
    cached: Option<&Validators>,
) -> Result<Option<Page>> {
    let mut req = client.get(url).header(header::ACCEPT, "text/html");
    if let Some(etag) = cached.and_then(|c| c.etag.as_ref()) {
        req = req.header(header::IF_NONE_MATCH, etag);
    }
    if let Some(lm) = cached.and_then(|c| c.last_modified.as_ref()) {
        req = req.header(header::IF_MODIFIED_SINCE, lm);
    }

    let resp = req.send().await?;
    if resp.status() == StatusCode::NOT_MODIFIED && cached.is_some() {
        info!(url, "not modified");
        return Ok(None);
    }
    let resp = resp.error_for_status()?;

    let header = |name| {
        resp.headers()
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned)
    };
    let content_type = header(header::CONTENT_TYPE);
    ensure!(
        content_type.as_deref().is_none_or(|ct| ct.contains("html")),
        "not HTML: {content_type:?}"
    );
    let validators = Validators {
        etag: header(header::ETAG),
        last_modified: header(header::LAST_MODIFIED),
    };

    let body = resp.text().await?;
    let (title, text) =
        tokio::task::spawn_blocking(move || extract(&body)).await?;
    info!(url, bytes = text.len(), "indexed");
    Ok(Some(Page {
        url: url.to_owned(),
        title,
        text,
        validators,
    }))
}

/// Pull the title and the visible text out of an HTML document, skipping
/// script, style and other non-content elements.
fn extract(html: &str) -> (Option<String>, String) {
    let doc = Html::parse_document(html);
    let title_selector = Selector::parse("title").expect("valid selector");
    let title = doc
        .select(&title_selector)
        .next()
        .map(|t| t.text().collect::<String>().trim().to_owned())
        .filter(|t| !t.is_empty());

    let text = doc
        .tree
        .nodes()
        .filter_map(|node| {
            let text = node.value().as_text()?;
            let parent = node.parent()?.value().as_element()?.name();
            Some((text, parent))
        })
        .filter(|(_, parent)| {
            !matches!(
                *parent,
                "script" | "style" | "noscript" | "template" | "title"
            )
        })
        .flat_map(|(text, _)| text.split_whitespace())
        .collect::<Vec<_>>()
        .join(" ");

    (title, text)
}

/// Read, trim and deduplicate the URL list.
async fn read_urls(path: &Path) -> Result<Vec<String>> {
    let contents = tokio::fs::read_to_string(path)
        .await
        .wrap_err_with(|| format!("reading {}", path.display()))?;
    let mut urls: Vec<String> = contents
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(str::to_owned)
        .collect();
    urls.sort();
    urls.dedup();
    Ok(urls)
}

/// Open (creating if needed) the index database.
fn open_index(path: &Path) -> Result<Connection> {
    let db = Connection::open(path)
        .wrap_err_with(|| format!("opening {}", path.display()))?;
    db.execute_batch(
        "PRAGMA journal_mode = WAL;
         PRAGMA synchronous = NORMAL;
         CREATE TABLE IF NOT EXISTS pages (
             url           TEXT PRIMARY KEY,
             title         TEXT,
             text          TEXT NOT NULL,
             etag          TEXT,
             last_modified TEXT
         );",
    )?;
    Ok(db)
}

/// Validators for every URL already in the index. The text column is never
/// read here, so this stays cheap however large the index grows.
fn load_validators(db: &Connection) -> Result<HashMap<String, Validators>> {
    let mut stmt = db.prepare("SELECT url, etag, last_modified FROM pages")?;
    let rows = stmt.query_map([], |row| {
        let validators = Validators {
            etag: row.get(1)?,
            last_modified: row.get(2)?,
        };
        Ok((row.get(0)?, validators))
    })?;
    Ok(rows.collect::<Result<_, _>>()?)
}

/// Insert or replace a page.
fn upsert(db: &Connection, page: &Page) -> Result<()> {
    db.execute(
        "INSERT INTO pages (url, title, text, etag, last_modified)
         VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT (url) DO UPDATE SET
             title = excluded.title, text = excluded.text,
             etag = excluded.etag, last_modified = excluded.last_modified",
        params![
            page.url,
            page.title,
            page.text,
            page.validators.etag,
            page.validators.last_modified
        ],
    )?;
    Ok(())
}

/// Drop rows for URLs no longer in the input list. Returns the number removed.
fn prune(db: &Connection, urls: &[String]) -> Result<usize> {
    let wanted = serde_json::to_string(urls)?;
    Ok(db.execute(
        "DELETE FROM pages WHERE url NOT IN (SELECT value FROM json_each(?1))",
        [wanted],
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_skips_non_content() {
        let html = r#"<html><head><title> Hi </title><style>p{}</style></head>
            <body><p>Hello,   <b>world</b></p><script>var x = 1;</script></body></html>"#;
        let (title, text) = extract(html);
        assert_eq!(title.as_deref(), Some("Hi"));
        assert_eq!(text, "Hello, world");
    }

    #[tokio::test]
    async fn read_urls_dedupes_and_ignores_comments() {
        let dir = std::env::temp_dir()
            .join(format!("webtronix-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("urls.txt");
        std::fs::write(
            &path,
            "# comment\nhttps://b\n\n https://a \nhttps://b\n",
        )
        .unwrap();
        assert_eq!(read_urls(&path).await.unwrap(), ["https://a", "https://b"]);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn index_round_trips_and_prunes() {
        let db = open_index(Path::new(":memory:")).unwrap();
        for url in ["https://a", "https://b"] {
            let page = Page {
                url: url.to_owned(),
                title: Some("t".into()),
                text: "hello ".repeat(100),
                validators: Validators {
                    etag: Some("\"x\"".into()),
                    last_modified: None,
                },
            };
            upsert(&db, &page).unwrap();
        }
        let validators = load_validators(&db).unwrap();
        assert_eq!(validators["https://a"].etag.as_deref(), Some("\"x\""));

        let text: String = db
            .query_row(
                "SELECT text FROM pages WHERE url = 'https://a'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(text, "hello ".repeat(100));

        assert_eq!(prune(&db, &["https://a".to_owned()]).unwrap(), 1);
        assert_eq!(load_validators(&db).unwrap().len(), 1);
    }
}
