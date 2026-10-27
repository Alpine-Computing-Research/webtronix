# AGENTS.md

Guidance for AI coding agents (and new contributors) working on `webtronix`.

## What this is

`webtronix` is a small Rust library with a command-line front end. It takes
a list of URLs, fetches them concurrently, extracts the text of each HTML
page or PDF, and upserts the result into a SQLite index (`index.db` by
default). Other programs call in through `webtronix::Index` (`open`,
`update`, `get`, `pages`) with a `webtronix::Options`; the binary is a thin
wrapper that only adds argument and config-file parsing, logging and a
progress bar. Pages that
are only a JavaScript shell when served (Substack home pages, for example)
are loaded in a headless Chrome and the rendered DOM is extracted instead.
Re-running it against an existing index sends conditional requests
(`If-None-Match` / `If-Modified-Since`) so unchanged pages are not downloaded
again, and prunes rows whose URL is no longer in the input list.

Every option can be set on the command line or in a TOML config file
(`--config`, or `webtronix.toml` in the working directory); the command line
wins over the file, the file over the default. Environment variables are
never read for configuration.

The library lives in `src/lib.rs` and the command line in `src/main.rs`.
Read the library's crate-level doc comment first; it is the authoritative
description of behaviour.

## Layout

| Path                         | Purpose                                                  |
| ---------------------------- | -------------------------------------------------------- |
| `src/lib.rs`                 | The library: `Index`, `Options`, fetch, extract, SQLite  |
| `src/main.rs`                | The binary: CLI args, config file, logging, progress bar |
| `Cargo.toml`                 | Rust 2024 edition; `cli` feature gates the binary's deps |
| `rustfmt.toml`               | `max_width = 80`                                         |
| `.github/workflows/main.yml` | CI (build, test, doc, lint, bench, audit)                |
| `*.tmp`, `*.db*`             | Local scratch and indexes; git-ignored, never commit     |

Key dependencies of the library: `reqwest` (rustls, gzip, brotli), `tokio` +
`futures` (concurrency), `trafilatura` + `scraper` (HTML text),
`pdf-extract` (PDF text), `chromiumoxide` (headless Chrome over the DevTools
protocol, for JavaScript-rendered pages), `rusqlite` (bundled SQLite),
`serde` (the `Format` and `Render` enums deserialize), `eyre` (errors),
`tracing` (logging). Only the binary needs `clap` (derive CLI), `toml`
(config file), `tracing-subscriber` and `indicatif` (progress bar); they are
optional, pulled in by the default `cli` feature.

## Build, test, lint

Everything CI runs, in the order you should run it before finishing a change:

```sh
cargo fmt -- --check                      # rustfmt, 80 columns
cargo clippy --all-targets -- -D warnings # warnings are errors
cargo clippy --no-default-features --all-targets -- -D warnings # library alone
cargo test
RUSTDOCFLAGS='-D warnings' cargo doc --no-deps
```

CI also runs `cargo build`, `cargo check`, `cargo bench` (there are currently
no benches) and `cargo audit`. A change is not done until fmt, both clippy
runs, test and doc all pass locally. Do not loosen lints or add `#[allow]` to get there;
fix the code instead.

Run the tool with:

```sh
cargo run -- urls.txt                 # writes ./index.db
cargo run -- urls.txt -o out.db -c 32 -t 60 -v
cargo run -- urls.txt --format html   # store HTML verbatim, PDFs still extracted
cargo run -- urls.txt --render never  # no browser: HTML as served
cargo run -- urls.txt --render always # run every HTML page's JavaScript in Chrome
cargo run -- urls.txt --format html --render always  # rendered DOM, no extraction
cargo run -- urls.txt --config prod.toml             # defaults from a file
```

`--log` (or `log` in the config file) takes `tracing` filter directives and
replaces the level chosen by `-v` / `-q`. Rendering needs a Chrome or
Chromium binary; `chromiumoxide` finds the usual ones and `--chrome` (or
`chrome` in the config file) points it elsewhere. Without one the tool warns
once and indexes pages as served.

## Architecture in one paragraph

`main` (binary) parses the command line, reads the config file and resolves
the two into `Settings` (command line, then file, then `Options::default`),
reads and dedupes the URL list, sets up logging and the progress bar, opens
the `Index` and calls `Index::update`. `Index::open` (library) creates the
`pages` table and migrates old indexes that lack the `mime` column.
`Index::update` loads the stored HTTP validators, then drives `fetch` over
every URL with `buffer_unordered(concurrency)`. Each `fetch` refuses unsupported content
types before reading the body, sniffs the body when the server is unhelpful,
hands HTML that looks like a script shell (`needs_render`) to the shared
`Renderer`, which lazily launches one headless Chrome and loads the page in a
tab until the DOM settles, and runs `extract` on a blocking thread inside
`catch_unwind` because the PDF parser can panic on malformed input. Fetched
pages go through a bounded `mpsc` channel to a single `spawn_blocking` writer
task, so a slow disk back-pressures fetches instead of buffering pages in
memory. The SQLite connection sits behind a `Mutex` in the `Index` so the
writer and the caller's reads can share it; each statement takes the lock
for its own duration only. After all fetches finish, `prune` drops rows for
URLs not in the input. In the binary, logging is routed through the progress
bar via `BarWriter` so log lines do not tear it, and the library's
`on_done` callback advances the bar.

## Conventions

- **Two files.** Everything that does the work is in `src/lib.rs`; only
  what is specific to running from a terminal (arguments, config file,
  logging set-up, progress bar) is in `src/main.rs`. Do not add modules as
  a matter of taste, and do not make the binary depend on anything the
  library does not expose publicly.
- **Public API.** The library's surface is `Index`, `Options`, `Summary`,
  `Page`, `Validators`, `Format`, `Render`, `Kind`, `extract` and
  `read_urls`. Keep it that small; new knobs go on `Options` (with a
  default) rather than as new entry points. The library never reads
  environment variables, installs a logger or a panic hook, or prints.
- **Doc comments on everything.** Every item, including private functions,
  structs and enum variants, has a `///` comment saying what it does and any
  non-obvious contract. `cargo doc` runs with `-D warnings`, so broken
  intra-doc links fail CI.
- **Explain the why in comments.** Existing comments explain decisions
  (why HTML goes through `resp.text()`, why the channel is bounded, why
  panics are caught). Match that; do not add comments that restate the code.
- **Errors.** Use `eyre::Result`, `bail!`, `eyre!` and `.wrap_err_with(..)`
  with a short lowercase context such as `"reading {path}"`. A failure on one
  URL is logged with `warn!` and skipped; it must never abort the whole run.
- **Logging.** `tracing` macros with structured fields (`info!(url, mime,
  "indexed")`). Never `println!` or `eprintln!`; stdout and stderr are owned
  by the progress bar.
- **Panics are a bug** except where explicitly contained (`extract` under
  `catch_unwind`). Do not `unwrap` on fallible input; `expect` is acceptable
  only for compile-time constants such as a static CSS selector.
- **Async vs blocking.** Network I/O is async. CPU-heavy or blocking work
  (parsing, SQLite) runs under `tokio::task::spawn_blocking`. Do not hold the
  SQLite `Connection` across an `.await`.
- **SQLite.** The `pages` schema is in `open_index`. Any schema change must
  include a migration for existing indexes (see the `mime` column pattern and
  its test) because users re-run the tool against old databases.
- **Formatting.** `cargo fmt` with the 80-column limit in `rustfmt.toml`.
  Imports are grouped `std`, then external crates, as the file does now.
- **Configuration.** Options come from the command line, then the config
  file, then `Options::default()`, resolved in `Settings::resolve`. Never
  read environment variables for configuration. A new option is added in
  four places: `Options` in the library (with its default), `Args` (as an
  `Option`, with the default stated in the help text), `Config` (same key
  name as the long flag) and `Settings::resolve`, plus a line in the
  README's example config.
- **No new dependencies without reason.** Prefer what is already in
  `Cargo.toml`. If one is needed, keep features minimal (see how `reqwest`
  and `tokio` are declared) and keep `rustls`; do not pull in OpenSSL.

## Tests

Unit tests live in `mod tests` at the bottom of each file (the pipeline's in
`src/lib.rs`, the config and argument handling's in `src/main.rs`) and run
with `cargo test`, as does the example in the library's crate doc. They are hermetic: no network access, SQLite via `:memory:` or
a temp directory named after the process id, and a hand-built one-page PDF
(`tiny_pdf`) for the PDF path. Follow that pattern:

- A behaviour change gets a test next to the existing ones.
- Never add a test that hits a real URL, and never launch a browser in a
  test; `needs_render` is tested on inline HTML, the render path only by
  hand. `Index::update` is exercised with an empty URL list, which builds
  the client and prunes without a single request.
- Clean up any temp directory you create.
- Test names describe the behaviour (`extract_html_drops_boilerplate`), not
  the function.

## Git

- Commit subjects are short, lowercase, imperative or noun-phrase, no
  trailing period (`add misc sqlite locks`, `pdf support`).
- `Cargo.lock` is committed (binary crate). Do not hand-edit it. Bump
  dependencies with `cargo update` deliberately and in their own commit,
  not as a side effect of another change.
- Never commit `*.db`, `*.tmp`, or the URL lists used for local runs.
- The default branch is `master`.

## Things to be careful about

- `--format` affects only HTML; PDFs are always converted to text. Keep
  that invariant and its test.
- `--format` and `--render` are independent axes (extract or not, run
  JavaScript or not) and every combination is valid. Do not couple them.
- Unsupported `Content-Type`s are rejected *before* the body is read. Do not
  move that check later; it is a bandwidth guard.
- Missing or `application/octet-stream` content types are sniffed from the
  body (`%PDF-` prefix or assume HTML). Only those two cases are sniffed.
- `load_validators` deliberately never reads the `text` column so start-up
  stays cheap on large indexes. Keep it that way.
- The `304 Not Modified` path returns `Ok(None)` and leaves the existing row
  untouched; so does any fetch error for a URL that already has a row.
- Rendering never fails a URL: a missing browser is warned about once, a
  failed or timed-out render is logged and the page is indexed as served.
  The validators always come from the served response, never the render.
- `needs_render` is a heuristic (a `<noscript>` notice asking for
  JavaScript, or scripts plus fewer than `MIN_WORDS` of visible text). A
  false positive only costs a render; tune it rather than special-casing
  sites.
- `<noscript>` is pruned before extraction because a scripted browser never
  shows it and trafilatura's fallback would otherwise index its raw text.
- `MAX_TABS` caps open tabs independently of `--concurrency`; each tab is a
  Chrome renderer process.
