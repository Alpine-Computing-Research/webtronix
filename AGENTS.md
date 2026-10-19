# AGENTS.md

Guidance for AI coding agents (and new contributors) working on `webtronix`.

## What this is

`webtronix` is a small Rust command-line tool. It takes a file of URLs,
fetches them concurrently, extracts the text of each HTML page or PDF, and
upserts the result into a SQLite index (`index.db` by default). Pages that
are only a JavaScript shell when served (Substack home pages, for example)
are loaded in a headless Chrome and the rendered DOM is extracted instead.
Re-running it against an existing index sends conditional requests
(`If-None-Match` / `If-Modified-Since`) so unchanged pages are not downloaded
again, and prunes rows whose URL is no longer in the input list.

The whole program lives in `src/main.rs`. Read its crate-level doc comment
first; it is the authoritative description of behaviour.

## Layout

| Path                         | Purpose                                                  |
| ---------------------------- | -------------------------------------------------------- |
| `src/main.rs`                | Entire program: CLI args, fetch, extract, SQLite, tests  |
| `Cargo.toml`                 | Rust 2024 edition; dependencies listed below             |
| `rustfmt.toml`               | `max_width = 80`                                         |
| `.github/workflows/main.yml` | CI (build, test, doc, lint, bench, audit)                |
| `*.tmp`, `*.db*`             | Local scratch and indexes; git-ignored, never commit     |

Key dependencies: `clap` (derive CLI), `reqwest` (rustls, gzip, brotli),
`tokio` + `futures` (concurrency), `trafilatura` + `scraper` (HTML text),
`pdf-extract` (PDF text), `chromiumoxide` (headless Chrome over the DevTools
protocol, for JavaScript-rendered pages), `rusqlite` (bundled SQLite),
`eyre` (errors), `tracing` (logging), `indicatif` (progress bar).

## Build, test, lint

Everything CI runs, in the order you should run it before finishing a change:

```sh
cargo fmt -- --check                      # rustfmt, 80 columns
cargo clippy --all-targets -- -D warnings # warnings are errors
cargo test
RUSTDOCFLAGS='-D warnings' cargo doc --no-deps
```

CI also runs `cargo build`, `cargo check`, `cargo bench` (there are currently
no benches) and `cargo audit`. A change is not done until fmt, clippy, test
and doc all pass locally. Do not loosen lints or add `#[allow]` to get there;
fix the code instead.

Run the tool with:

```sh
cargo run -- urls.txt                 # writes ./index.db
cargo run -- urls.txt -o out.db -c 32 -t 60 -v
cargo run -- urls.txt --format html   # store HTML verbatim, PDFs still extracted
cargo run -- urls.txt --render never  # no browser: HTML as served
cargo run -- urls.txt --render always # run every HTML page's JavaScript in Chrome
cargo run -- urls.txt --format html --render always  # rendered DOM, no extraction
```

`RUST_LOG` overrides the log level chosen by `-v` / `-q`. Rendering needs a
Chrome or Chromium binary; `chromiumoxide` finds the usual ones and the
`CHROME` environment variable points it elsewhere. Without one the tool
warns once and indexes pages as served.

## Architecture in one paragraph

`main` reads and dedupes the URL list, opens the index (creating the `pages`
table and migrating old indexes that lack the `mime` column), loads the
stored HTTP validators, then drives `fetch` over every URL with
`buffer_unordered(concurrency)`. Each `fetch` refuses unsupported content
types before reading the body, sniffs the body when the server is unhelpful,
hands HTML that looks like a script shell (`needs_render`) to the shared
`Renderer`, which lazily launches one headless Chrome and loads the page in a
tab until the DOM settles, and runs `extract` on a blocking thread inside
`catch_unwind` because the PDF parser can panic on malformed input. Fetched
pages go through a bounded
`mpsc` channel to a single `spawn_blocking` writer task that owns the SQLite
connection, so a slow disk back-pressures fetches instead of buffering pages
in memory. Logging is routed through the progress bar via `BarWriter` so log
lines do not tear it. After all fetches finish, `prune` drops rows for URLs
not in the input.

## Conventions

- **Single file.** Keep everything in `src/main.rs` unless a change genuinely
  needs a second module. Do not split it up as a matter of taste.
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
- **No new dependencies without reason.** Prefer what is already in
  `Cargo.toml`. If one is needed, keep features minimal (see how `reqwest`
  and `tokio` are declared) and keep `rustls`; do not pull in OpenSSL.

## Tests

Unit tests live in `mod tests` at the bottom of `src/main.rs` and run with
`cargo test`. They are hermetic: no network access, SQLite via `:memory:` or
a temp directory named after the process id, and a hand-built one-page PDF
(`tiny_pdf`) for the PDF path. Follow that pattern:

- A behaviour change gets a test next to the existing ones.
- Never add a test that hits a real URL, and never launch a browser in a
  test; `needs_render` is tested on inline HTML, the render path only by
  hand.
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
