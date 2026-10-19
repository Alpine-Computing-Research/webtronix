//! Fetch a list of URLs concurrently, extract the text of each HTML page or
//! PDF document and store the result in a SQLite index on disk.
//!
//! The index is a single `pages` table keyed by URL holding the MIME type,
//! the title, the extracted text and the HTTP validators used for conditional
//! requests.
//!
//! Bandwidth is kept down by deduplicating the input, accepting compressed
//! responses, refusing unsupported bodies before they are read, and sending
//! conditional requests (`If-None-Match` / `If-Modified-Since`) based on the
//! existing index so unchanged pages are not downloaded again.

use std::{
    any::Any,
    collections::HashMap,
    io::{self, Write},
    panic::{self, AssertUnwindSafe},
    path::{Path, PathBuf},
    time::Duration,
};

use clap::Parser;
use eyre::{Context, Result, bail, eyre};
use futures::{StreamExt, stream};
use indicatif::{ProgressBar, ProgressStyle};
use pdf_extract::{Document, Object, PlainTextOutput, decode_text_string};
use reqwest::{Client, StatusCode, header};
use rusqlite::{Connection, params};
use scraper::{Html, Selector};
use tokio::sync::mpsc;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

/// Extract the text of a list of web pages and PDFs into a SQLite index.
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
    /// Media type of the fetched body without parameters, e.g. `text/html`.
    mime: String,
    title: Option<String>,
    text: String,
    validators: Validators,
}

/// The body formats text can be extracted from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Html,
    Pdf,
}

impl Kind {
    /// Classify a media type (without parameters), or `None` if unsupported.
    fn from_mime(mime: &str) -> Option<Self> {
        if mime.ends_with("html") || mime.ends_with("xhtml+xml") {
            Some(Self::Html)
        } else if mime == "application/pdf" || mime == "application/x-pdf" {
            Some(Self::Pdf)
        } else {
            None
        }
    }

    /// Guess the format from the body when the server did not say.
    fn sniff(body: &[u8]) -> Self {
        if body.starts_with(b"%PDF-") {
            Self::Pdf
        } else {
            Self::Html
        }
    }

    fn mime(self) -> &'static str {
        match self {
            Self::Html => "text/html",
            Self::Pdf => "application/pdf",
        }
    }
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
    let log_level = match (args.verbose, args.quiet) {
        (true, _) => "info",
        (_, true) => "off",
        _ => "error",
    };
    let log_writer = bar.clone();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| log_level.into()),
        )
        .with_writer(move || BarWriter(log_writer.clone()))
        .init();
    // Panics (the PDF parser has a few on malformed input) are reported
    // through the logger so they do not tear the bar, and the one that
    // raised them is caught in `fetch` so only that URL is skipped.
    panic::set_hook(Box::new(|info| {
        let location = info
            .location()
            .map(|l| format!("{}:{}", l.file(), l.line()))
            .unwrap_or_default();
        error!(location, "panic: {}", panic_message(info.payload()));
    }));

    let db = open_index(&args.output)?;
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

    // Pages are written by a dedicated blocking task so a slow write never
    // stalls the in-flight fetches. Unchanged and failed pages leave their
    // existing row alone. The channel is bounded so a slow disk applies
    // back-pressure instead of buffering every page in memory.
    let (tx, mut rx) = mpsc::channel::<Page>(args.concurrency);
    let writer = tokio::task::spawn_blocking(move || {
        let mut written = 0;
        while let Some(page) = rx.blocking_recv() {
            upsert(&db, &page)?;
            written += 1;
        }
        Ok::<_, eyre::Report>((db, written))
    });
    while let Some(page) = results.next().await {
        bar.inc(1);
        let Some(page) = page else { continue };
        if tx.send(page).await.is_err() {
            // The writer only stops early if a write failed; its error is
            // reported below once the fetches are wound down.
            break;
        }
    }
    drop(tx);
    let (db, written) = writer.await??;
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
    let mut req = client
        .get(url)
        .header(header::ACCEPT, "text/html, application/pdf");
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
    // Media type without parameters, lower-cased. Servers that send nothing
    // useful (missing or `application/octet-stream`) get sniffed instead.
    let declared = header(header::CONTENT_TYPE)
        .map(|ct| ct.split(';').next().unwrap_or("").trim().to_lowercase())
        .filter(|ct| !ct.is_empty() && ct != "application/octet-stream");
    let kind = match &declared {
        Some(mime) => match Kind::from_mime(mime) {
            Some(kind) => Some(kind),
            // Refuse before downloading the body.
            None => bail!("unsupported type: {mime}"),
        },
        None => None,
    };
    let validators = Validators {
        etag: header(header::ETAG),
        last_modified: header(header::LAST_MODIFIED),
    };

    // Declared HTML goes through reqwest so the charset parameter is honoured.
    let body = match kind {
        Some(Kind::Html) => resp.text().await?.into_bytes(),
        _ => resp.bytes().await?.to_vec(),
    };
    let kind = kind.unwrap_or_else(|| Kind::sniff(&body));
    let mime = declared.unwrap_or_else(|| kind.mime().to_owned());
    let (title, text) = tokio::task::spawn_blocking(move || {
        panic::catch_unwind(AssertUnwindSafe(|| extract(kind, &body)))
            .unwrap_or_else(|payload| {
                Err(eyre!("extractor panicked: {}", panic_message(&*payload)))
            })
    })
    .await??;
    info!(url, mime, bytes = text.len(), "indexed");
    Ok(Some(Page {
        url: url.to_owned(),
        mime,
        title,
        text,
        validators,
    }))
}

/// The message carried by a panic payload, if it is the usual string.
fn panic_message(payload: &(dyn Any + Send)) -> &str {
    if let Some(s) = payload.downcast_ref::<&str>() {
        s
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s
    } else {
        "non-string panic payload"
    }
}

/// Pull the title and text out of a body of the given kind.
fn extract(kind: Kind, body: &[u8]) -> Result<(Option<String>, String)> {
    match kind {
        Kind::Html => Ok(extract_html(&String::from_utf8_lossy(body))),
        Kind::Pdf => extract_pdf(body),
    }
}

/// Pull the title (from the document info dictionary) and the text of every
/// page out of a PDF. Encrypted documents are opened with the empty password,
/// which is what most "protected" PDFs on the web use.
fn extract_pdf(bytes: &[u8]) -> Result<(Option<String>, String)> {
    let mut doc = Document::load_mem(bytes).map_err(|e| eyre!("{e}"))?;
    if doc.is_encrypted() {
        doc.decrypt("").map_err(|e| eyre!("decrypting: {e}"))?;
    }
    let title = doc
        .trailer
        .get(b"Info")
        .ok()
        .and_then(|info| doc.dereference(info).ok())
        .and_then(|(_, info)| info.as_dict().ok())
        .and_then(|info| info.get(b"Title").ok())
        .and_then(|title| match title {
            Object::String(..) => decode_text_string(title).ok(),
            _ => None,
        })
        .map(|t| t.trim().to_owned())
        .filter(|t| !t.is_empty());

    let mut raw = String::new();
    pdf_extract::output_doc(&doc, &mut PlainTextOutput::new(&mut raw))
        .map_err(|e| eyre!("{e}"))?;
    let text = raw.split_whitespace().collect::<Vec<_>>().join(" ");
    Ok((title, text))
}

/// Pull the title and the visible text out of an HTML document, skipping
/// script, style and other non-content elements.
fn extract_html(html: &str) -> (Option<String>, String) {
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
             mime          TEXT NOT NULL DEFAULT 'text/html',
             title         TEXT,
             text          TEXT NOT NULL,
             etag          TEXT,
             last_modified TEXT
         );",
    )?;
    // Indexes written before the `mime` column existed hold only HTML.
    let has_mime: i64 = db.query_row(
        "SELECT COUNT(*) FROM pragma_table_info('pages') WHERE name = 'mime'",
        [],
        |row| row.get(0),
    )?;
    if has_mime == 0 {
        db.execute_batch(
            "ALTER TABLE pages
             ADD COLUMN mime TEXT NOT NULL DEFAULT 'text/html'",
        )?;
    }
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
        "INSERT INTO pages (url, mime, title, text, etag, last_modified)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)
         ON CONFLICT (url) DO UPDATE SET
             mime = excluded.mime, title = excluded.title,
             text = excluded.text, etag = excluded.etag,
             last_modified = excluded.last_modified",
        params![
            page.url,
            page.mime,
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
        let (title, text) = extract_html(html);
        assert_eq!(title.as_deref(), Some("Hi"));
        assert_eq!(text, "Hello, world");
    }

    /// Build a one-page PDF with a correct cross-reference table.
    fn tiny_pdf(title: &str, body: &str) -> Vec<u8> {
        let content = format!("BT /F1 12 Tf 10 100 Td ({body}) Tj ET");
        let objects = [
            "<< /Type /Catalog /Pages 2 0 R >>".to_owned(),
            "<< /Type /Pages /Kids [3 0 R] /Count 1 >>".to_owned(),
            "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 200 200] \
             /Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>"
                .to_owned(),
            format!(
                "<< /Length {} >>\nstream\n{content}\nendstream",
                content.len()
            ),
            "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>".to_owned(),
            format!("<< /Title ({title}) >>"),
        ];
        let mut out = String::from("%PDF-1.4\n");
        let mut offsets = Vec::new();
        for (i, obj) in objects.iter().enumerate() {
            offsets.push(out.len());
            out.push_str(&format!("{} 0 obj\n{obj}\nendobj\n", i + 1));
        }
        let xref = out.len();
        out.push_str(&format!("xref\n0 {}\n", objects.len() + 1));
        out.push_str("0000000000 65535 f \n");
        for offset in offsets {
            out.push_str(&format!("{offset:010} 00000 n \n"));
        }
        out.push_str(&format!(
            "trailer\n<< /Size {} /Root 1 0 R /Info 6 0 R >>\n\
             startxref\n{xref}\n%%EOF\n",
            objects.len() + 1
        ));
        out.into_bytes()
    }

    #[test]
    fn extract_reads_pdf_title_and_text() {
        let pdf = tiny_pdf("A Paper", "Hello PDF");
        assert_eq!(Kind::sniff(&pdf), Kind::Pdf);
        let (title, text) = extract(Kind::Pdf, &pdf).unwrap();
        assert_eq!(title.as_deref(), Some("A Paper"));
        assert_eq!(text, "Hello PDF");
    }

    #[test]
    fn panic_message_reads_common_payloads() {
        let caught = panic::catch_unwind(|| panic!("boom {}", 1)).unwrap_err();
        assert_eq!(panic_message(&*caught), "boom 1");
        let caught = panic::catch_unwind(|| panic!("static")).unwrap_err();
        assert_eq!(panic_message(&*caught), "static");
        assert_eq!(
            panic_message(&Box::new(7u8) as &(dyn Any + Send)),
            "non-string panic payload"
        );
    }

    #[test]
    fn kind_classifies_media_types() {
        assert_eq!(Kind::from_mime("text/html"), Some(Kind::Html));
        assert_eq!(Kind::from_mime("application/xhtml+xml"), Some(Kind::Html));
        assert_eq!(Kind::from_mime("application/pdf"), Some(Kind::Pdf));
        assert_eq!(Kind::from_mime("image/png"), None);
        assert_eq!(Kind::sniff(b"<!doctype html>"), Kind::Html);
    }

    #[test]
    fn open_index_adds_mime_column_to_old_index() {
        let dir = std::env::temp_dir()
            .join(format!("webtronix-migrate-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("old.db");
        {
            let old = Connection::open(&path).unwrap();
            old.execute_batch(
                "CREATE TABLE pages (
                     url TEXT PRIMARY KEY, title TEXT, text TEXT NOT NULL,
                     etag TEXT, last_modified TEXT);
                 INSERT INTO pages (url, text) VALUES ('https://a', 'x');",
            )
            .unwrap();
        }
        let db = open_index(&path).unwrap();
        let mime: String = db
            .query_row(
                "SELECT mime FROM pages WHERE url = 'https://a'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(mime, "text/html");
        drop(db);
        std::fs::remove_dir_all(dir).unwrap();
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
                mime: "application/pdf".into(),
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

        let (mime, text): (String, String) = db
            .query_row(
                "SELECT mime, text FROM pages WHERE url = 'https://a'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(mime, "application/pdf");
        assert_eq!(text, "hello ".repeat(100));

        assert_eq!(prune(&db, &["https://a".to_owned()]).unwrap(), 1);
        assert_eq!(load_validators(&db).unwrap().len(), 1);
    }
}
