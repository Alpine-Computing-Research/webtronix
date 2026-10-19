//! Fetch a list of URLs concurrently, extract the text of each HTML page or
//! PDF document and store the result in a SQLite index on disk.
//!
//! HTML goes through [trafilatura], which keeps the main body of a page and
//! drops navigation, adverts, footers and other boilerplate. `--format html`
//! turns this off and stores the HTML itself. PDFs are converted to text
//! either way.
//!
//! The index is a single `pages` table keyed by URL holding the MIME type,
//! the title, the extracted text and the HTTP validators used for conditional
//! requests.
//!
//! [trafilatura]: https://crates.io/crates/trafilatura
//!
//! Pages that are only a script shell when served (single-page apps such as
//! Substack, where the server sends an empty root element and JavaScript
//! fills it in) are loaded in a headless Chrome so the text can be read from
//! the rendered DOM instead. `--render` controls when that happens; by
//! default only pages that look like such a shell are rendered.
//!
//! The two flags are independent, so each combination of extracting or not
//! and running JavaScript or not is available: `--format text --render
//! always` is text from the rendered DOM, `--format html --render never` is
//! the HTML exactly as served, and so on.
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

use chromiumoxide::browser::{Browser, BrowserConfig};
use clap::{Parser, ValueEnum};
use eyre::{Context, Result, bail, eyre};
use futures::{StreamExt, stream};
use indicatif::{ProgressBar, ProgressStyle};
use pdf_extract::{Document, Object, PlainTextOutput, decode_text_string};
use reqwest::{Client, StatusCode, header};
use rusqlite::{Connection, params};
use scraper::{Html, Selector};
use tokio::sync::{OnceCell, Semaphore, mpsc};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;
use trafilatura::Options;

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
    /// Per-request timeout in seconds. Also bounds each browser render.
    #[arg(short, long, default_value_t = 30)]
    timeout: u64,
    /// What to store for each HTML page: its extracted text, or the HTML
    /// itself (as served, or as rendered when `--render` applies). PDFs are
    /// converted to text either way.
    #[arg(short, long, value_enum, default_value_t = Format::Text)]
    format: Format,
    /// When to load an HTML page in a headless browser so its JavaScript
    /// runs before the page is stored or its text extracted. `auto` renders
    /// only pages whose served HTML looks like a script shell. Needs Chrome
    /// or Chromium; set `CHROME` to the binary if it is not found on its own.
    #[arg(long, value_enum, default_value_t = Render::Auto)]
    render: Render,
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

/// What is stored as the text of an HTML page. PDFs are unaffected.
/// Independent of [`Render`], which decides whether JavaScript runs first.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum Format {
    /// Its extracted text.
    Text,
    /// The HTML itself, verbatim.
    Html,
}

/// When HTML pages are rendered in a browser before they are stored or their
/// text is extracted.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum Render {
    /// Only pages whose served HTML looks like a script shell.
    Auto,
    /// Every HTML page.
    Always,
    /// No page; HTML is indexed as served.
    Never,
}

/// Tabs open at once. Each is a Chrome renderer process, far heavier than
/// an in-flight request, so this is not tied to `--concurrency`.
const MAX_TABS: usize = 4;

/// How long the DOM must stay unchanged after `load` before it is read.
const SETTLE: Duration = Duration::from_millis(250);

/// Most settle intervals waited for one page; pages with animations or
/// tickers never go quiet.
const MAX_SETTLE_ROUNDS: u32 = 20;

/// Consecutive unchanged settle intervals before the DOM counts as settled.
/// One is not enough: a page sits still for a quarter second with its
/// sign-up modal painted while the request for its real content is in
/// flight, more so when the crawl itself is loading the network.
const QUIET_ROUNDS: u32 = 3;

/// Fewest words of visible text a served page can have before it is taken
/// for a script shell.
const MIN_WORDS: usize = 200;

/// A headless browser shared by every fetch. Launching Chrome costs a
/// second or so and most runs never need it, so it starts on the first page
/// that does. A failed launch is reported once and remembered, so later
/// pages fall back to their served HTML without more noise.
struct Renderer {
    mode: Render,
    /// Bound on one page load, including the wait for its scripts to settle.
    timeout: Duration,
    /// `None` once a launch has failed.
    browser: OnceCell<Option<Browser>>,
    tabs: Semaphore,
}

impl Renderer {
    fn new(mode: Render, timeout: Duration) -> Self {
        Self {
            mode,
            timeout,
            browser: OnceCell::new(),
            tabs: Semaphore::new(MAX_TABS),
        }
    }

    /// The browser, launched on first use.
    async fn browser(&self) -> Option<&Browser> {
        self.browser
            .get_or_init(|| async {
                match launch_browser(self.timeout).await {
                    Ok(browser) => Some(browser),
                    Err(e) => {
                        warn!("no browser, indexing pages as served: {e:#}");
                        None
                    }
                }
            })
            .await
            .as_ref()
    }

    /// Load `url` in a fresh tab, wait for its scripts to settle and return
    /// the resulting DOM as HTML. `None` when no browser is available.
    async fn render(&self, url: &str) -> Result<Option<String>> {
        let Some(browser) = self.browser().await else {
            return Ok(None);
        };
        let _slot = self.tabs.acquire().await?;
        let tab = browser.new_page("about:blank").await?;
        let html =
            tokio::time::timeout(self.timeout, load_and_settle(&tab, url))
                .await;
        // Closed whatever happened, so a timed-out load does not keep its
        // tab (and renderer process) alive for the rest of the run.
        if let Err(e) = tab.close().await {
            warn!(url, "closing tab: {e}");
        }
        let html =
            html.map_err(|_| eyre!("timed out after {:?}", self.timeout))??;
        Ok(Some(html))
    }

    /// Shut the browser down, if one was launched.
    async fn close(self) {
        if let Some(Some(mut browser)) = self.browser.into_inner() {
            if let Err(e) = browser.close().await {
                warn!("closing browser: {e}");
            }
            // Reaps the child; it is killed on drop anyway if close failed.
            if let Err(e) = browser.wait().await {
                warn!("waiting for browser exit: {e}");
            }
        }
    }
}

/// Start a headless browser and spawn the task that drives its connection.
async fn launch_browser(timeout: Duration) -> Result<Browser> {
    let config = BrowserConfig::builder()
        .request_timeout(timeout)
        .build()
        .map_err(|e| eyre!(e))?;
    let (browser, mut handler) = Browser::launch(config).await?;
    // The handler multiplexes every tab over one websocket and ends with
    // the browser, so nothing needs to join it.
    tokio::spawn(async move {
        while let Some(event) = handler.next().await {
            if event.is_err() {
                break;
            }
        }
    });
    Ok(browser)
}

/// Serialises the document once it has fully loaded, as `Page::content`
/// does, and evaluates to `null` before then. Navigation resolves when the
/// new document is committed, not loaded, so a shell whose deferred scripts
/// are still downloading would otherwise look settled.
const SETTLED_CONTENT: &str = "document.readyState === 'complete'
    ? (document.doctype
        ? new XMLSerializer().serializeToString(document.doctype)
        : '') + document.documentElement.outerHTML
    : null";

/// Navigate a tab to `url`, then wait until the document has loaded and
/// its DOM has stopped changing (for at most [`MAX_SETTLE_ROUNDS`]) so
/// content that scripts fill in after `load`, such as a fetched post list,
/// is included. See [`settled`] for what counts as stopped.
async fn load_and_settle(
    tab: &chromiumoxide::Page,
    url: &str,
) -> Result<String> {
    tab.goto(url).await?;
    let mut last: Option<String> = None;
    let mut quiet = 0;
    for _ in 0..MAX_SETTLE_ROUNDS {
        tokio::time::sleep(SETTLE).await;
        let next: Option<String> =
            tab.evaluate(SETTLED_CONTENT).await?.into_value()?;
        quiet = if next.is_some() && next == last {
            quiet + 1
        } else {
            0
        };
        if let Some(html) = &next
            && settled(quiet, html)
        {
            return Ok(next.expect("checked above"));
        }
        last = next;
    }
    last.ok_or_else(|| eyre!("page never finished loading"))
}

/// Whether a rendered document that has been unchanged for `quiet`
/// consecutive intervals is done loading. It must have been quiet for
/// [`QUIET_ROUNDS`], and while it still reads like a shell (fewer than
/// [`MIN_WORDS`] words, the same test that sent it to the browser) it is
/// given the rest of the settle budget, since a page that stays that empty
/// is most likely still waiting on a request for its content.
fn settled(quiet: u32, html: &str) -> bool {
    quiet >= QUIET_ROUNDS
        && visible_text(html).split_whitespace().count() >= MIN_WORDS
}

/// Whether served HTML looks like a shell that a browser fills in with
/// JavaScript: it carries a `<noscript>` notice asking for JavaScript, or it
/// loads scripts yet shows fewer than [`MIN_WORDS`] words. A short static
/// page trips this too; rendering it only costs time, as the text is the
/// same either way.
fn needs_render(html: &str) -> bool {
    let doc = Html::parse_document(html);
    let noscript = Selector::parse("noscript").expect("valid selector");
    let asks_for_js = doc.select(&noscript).any(|n| {
        n.text()
            .collect::<String>()
            .to_lowercase()
            .contains("javascript")
    });
    if asks_for_js {
        return true;
    }
    let script = Selector::parse("script").expect("valid selector");
    doc.select(&script).next().is_some()
        && visible_text_of(&doc).split_whitespace().count() < MIN_WORDS
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
    // chromiumoxide warns about every protocol message newer than its
    // generated types, which a current Chrome sends constantly; harmless.
    let log_filter = format!("{log_level},chromiumoxide=error");
    let log_writer = bar.clone();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| log_filter.into()),
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

    let format = args.format;
    let renderer =
        Renderer::new(args.render, Duration::from_secs(args.timeout));
    let mut results = stream::iter(urls.iter().cloned())
        .map(|url| {
            let client = &client;
            let renderer = &renderer;
            let cached = previous.get(&url);
            async move {
                match fetch(client, renderer, &url, cached, format).await {
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
    // The exhausted stream still borrows the renderer until it is dropped.
    drop(results);
    renderer.close().await;
    bar.finish_and_clear();
    info!(written, pruned, path = %args.output.display(), "updated index");
    Ok(())
}

/// Fetch and extract one page, sending a conditional request when validators
/// are available. Returns `None` on `304 Not Modified`. With
/// [`Format::Html`] the text of an HTML page is the page itself.
///
/// The conditional request and the validators always concern the served
/// HTML; a page the renderer rebuilds in a browser still counts as unchanged
/// when the server says so.
async fn fetch(
    client: &Client,
    renderer: &Renderer,
    url: &str,
    cached: Option<&Validators>,
    format: Format,
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
    let (body, rendered) = match kind {
        Kind::Html => render_if_needed(renderer, url, body).await?,
        Kind::Pdf => (body, false),
    };
    let owned_url = url.to_owned();
    let (title, text) = tokio::task::spawn_blocking(move || {
        panic::catch_unwind(AssertUnwindSafe(|| {
            extract(kind, &body, &owned_url, format)
        }))
        .unwrap_or_else(|payload| {
            Err(eyre!("extractor panicked: {}", panic_message(&*payload)))
        })
    })
    .await??;
    info!(url, mime, rendered, bytes = text.len(), "indexed");
    Ok(Some(Page {
        url: url.to_owned(),
        mime,
        title,
        text,
        validators,
    }))
}

/// Swap a served HTML body for its browser-rendered DOM when the renderer's
/// mode calls for it, reporting whether that happened. A render that fails
/// is logged and the page is indexed as served, since a shell with a title
/// still beats no row at all.
async fn render_if_needed(
    renderer: &Renderer,
    url: &str,
    body: Vec<u8>,
) -> Result<(Vec<u8>, bool)> {
    let (wanted, body) = match renderer.mode {
        Render::Always => (true, body),
        Render::Never => (false, body),
        Render::Auto => {
            tokio::task::spawn_blocking(move || {
                let wanted = needs_render(&String::from_utf8_lossy(&body));
                (wanted, body)
            })
            .await?
        }
    };
    if !wanted {
        return Ok((body, false));
    }
    match renderer.render(url).await {
        Ok(Some(html)) => Ok((html.into_bytes(), true)),
        Ok(None) => Ok((body, false)),
        Err(e) => {
            warn!(url, "rendering failed, indexing as served: {e:#}");
            Ok((body, false))
        }
    }
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

/// Pull the title and text out of a body of the given kind. `url` is the
/// page's address, used to resolve links and metadata while extracting HTML.
/// With [`Format::Html`] an HTML body is stored as is rather than extracted.
fn extract(
    kind: Kind,
    body: &[u8],
    url: &str,
    format: Format,
) -> Result<(Option<String>, String)> {
    match kind {
        Kind::Html if format == Format::Html => {
            let html = String::from_utf8_lossy(body);
            Ok((html_title(&html), html.into_owned()))
        }
        Kind::Html => Ok(extract_html(&String::from_utf8_lossy(body), url)),
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

/// Pull the title and the main text out of an HTML document with
/// trafilatura, which drops navigation, adverts, footers and the like.
///
/// Pages trafilatura gives up on (for example ones with no paragraph of
/// prose at all) fall back to [`visible_text`] so they are still indexed.
fn extract_html(html: &str, url: &str) -> (Option<String>, String) {
    let mut opts = Options::default().with_fallback(true);
    opts.original_url = url.parse().ok();
    opts.exclude_comments = true;
    // A browser with scripts on never shows `<noscript>`, and its contents
    // (typically a "turn on JavaScript" banner with inline CSS) are raw
    // text that the fallback would otherwise recover as prose.
    opts.prune_selector = Some("noscript".to_owned());
    match trafilatura::extract(html, &opts) {
        Ok(result) => {
            // The `<title>` element is preferred; trafilatura's metadata
            // title (OpenGraph, JSON-LD) only fills in when it is missing.
            let title = html_title(html).or_else(|| {
                Some(result.metadata.title.trim().to_owned())
                    .filter(|t| !t.is_empty())
            });
            (title, result.content_text.trim().to_owned())
        }
        Err(e) => {
            warn!(url, "no main content found, indexing visible text: {e}");
            (html_title(html), visible_text(html))
        }
    }
}

/// The contents of the `<title>` element, if there is a non-empty one.
fn html_title(html: &str) -> Option<String> {
    let doc = Html::parse_document(html);
    let selector = Selector::parse("title").expect("valid selector");
    doc.select(&selector)
        .next()
        .map(|t| t.text().collect::<String>().trim().to_owned())
        .filter(|t| !t.is_empty())
}

/// All visible text of an HTML document joined by single spaces, skipping
/// script, style and other non-content elements.
fn visible_text(html: &str) -> String {
    visible_text_of(&Html::parse_document(html))
}

/// [`visible_text`] of an already parsed document.
fn visible_text_of(doc: &Html) -> String {
    doc.tree
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
        .join(" ")
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

    const URL: &str = "https://example.com/post";

    #[test]
    fn visible_text_skips_non_content() {
        let html = r#"<html><head><title> Hi </title><style>p{}</style></head>
            <body><p>Hello,   <b>world</b></p><script>var x = 1;</script></body></html>"#;
        assert_eq!(html_title(html).as_deref(), Some("Hi"));
        assert_eq!(visible_text(html), "Hello, world");
    }

    #[test]
    fn settled_waits_for_quiet_and_for_content() {
        let shell = "<html><body><div>Subscribe</div></body></html>";
        let words = ["word"; MIN_WORDS].join(" ");
        let full = format!("<html><body><p>{words}</p></body></html>");
        assert!(
            !settled(QUIET_ROUNDS - 1, &full),
            "one quiet round is not enough"
        );
        assert!(settled(QUIET_ROUNDS, &full));
        assert!(!settled(QUIET_ROUNDS, shell), "a quiet shell keeps waiting");
        assert!(!settled(MAX_SETTLE_ROUNDS, shell));
    }

    #[test]
    fn needs_render_spots_script_shells() {
        let shell = r#"<html><head><title>App</title>
            <script src="/static/app.js" defer></script></head>
            <body><div id="root"></div></body></html>"#;
        assert!(needs_render(shell));

        let prose = "words of real content ".repeat(100);
        let article = format!(
            "<html><body><script>track()</script>\
             <article><p>{prose}</p></article></body></html>"
        );
        assert!(!needs_render(&article));

        let plain = "<html><body><p>Short, and no scripts.</p></body></html>";
        assert!(!needs_render(plain));
    }

    #[test]
    fn needs_render_heeds_noscript_notice() {
        let prose = "words of real content ".repeat(100);
        let html = format!(
            "<html><body><noscript><p>Please enable JavaScript.</p>\
             </noscript><nav>{prose}</nav></body></html>"
        );
        assert!(needs_render(&html));
        let html = format!(
            "<html><body><noscript><img src=\"/pixel.gif\"></noscript>\
             <nav>{prose}</nav></body></html>"
        );
        assert!(!needs_render(&html));
    }

    #[test]
    fn extract_html_ignores_noscript() {
        let body = "This is the main article content. ".repeat(10);
        let html = format!(
            "<html><body><noscript><style>#b{{color:red}}</style>\
             <div id=\"b\">Please turn on JavaScript</div></noscript>\
             <article><p>{body}</p></article></body></html>"
        );
        let (_, text) = extract_html(&html, URL);
        assert!(text.contains("main article content"));
        assert!(!text.contains("JavaScript"), "noscript kept: {text}");
        assert!(!text.contains("color"), "noscript css kept: {text}");
    }

    #[test]
    fn extract_html_drops_boilerplate() {
        let body = "This is the main article content. ".repeat(10);
        let html = format!(
            "<html><head><title>Site - Post</title></head><body>\
             <nav><a href=\"/\">Home</a> <a href=\"/about\">About</a></nav>\
             <article><h1>Post</h1><p>{body}</p></article>\
             <footer>Copyright 2024 Site</footer></body></html>"
        );
        let (title, text) = extract_html(&html, URL);
        assert_eq!(title.as_deref(), Some("Site - Post"));
        assert!(text.contains("main article content"));
        assert!(!text.contains("Home"), "nav kept: {text}");
        assert!(!text.contains("Copyright"), "footer kept: {text}");
    }

    #[test]
    fn extract_html_falls_back_on_pages_without_prose() {
        let html = "<html><head><title>T</title></head><body>\
                    <ul><li>one</li><li>two</li></ul></body></html>";
        let (title, text) = extract_html(html, URL);
        assert_eq!(title.as_deref(), Some("T"));
        assert!(text.contains("one") && text.contains("two"), "{text}");
    }

    #[test]
    fn extract_keeps_html_verbatim_in_html_format() {
        let html = "<html><head><title>T</title></head>\
                    <body><nav>Menu</nav><p>Hi</p></body></html>";
        let (title, text) =
            extract(Kind::Html, html.as_bytes(), URL, Format::Html).unwrap();
        assert_eq!(title.as_deref(), Some("T"));
        assert_eq!(text, html);
        let (_, text) =
            extract(Kind::Html, html.as_bytes(), URL, Format::Text).unwrap();
        assert_ne!(text, html);
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
        // PDFs are converted to text whatever `--format` says.
        for format in [Format::Text, Format::Html] {
            let (title, text) = extract(Kind::Pdf, &pdf, URL, format).unwrap();
            assert_eq!(title.as_deref(), Some("A Paper"));
            assert_eq!(text, "Hello PDF");
        }
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
