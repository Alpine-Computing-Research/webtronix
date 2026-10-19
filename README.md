# Webtronix #

Webtronix retrieves web pages and caches them locally on-disk using SQLite. It is intended for use by upstream ML applications, namely RAG and keyword classification.

## Usage ##

```
$ webtronix -h
Fetch a list of URLs concurrently and extract their text into a SQLite index

Usage: webtronix [OPTIONS] <URLS>

Arguments:
  <URLS>  File containing one URL per line. Blank lines and `#` comments are ignored

Options:
  -o, --output <OUTPUT>            SQLite database to write. An existing index is reused to make conditional requests so unchanged pages are not re-downloaded [default: index.db]
  -c, --concurrency <CONCURRENCY>  Maximum number of in-flight requests [default: 16]
  -t, --timeout <TIMEOUT>          Per-request timeout in seconds. Also bounds each browser render [default: 30]
  -f, --format <FORMAT>            What to store for each HTML page: its extracted text, or the HTML itself (as served, or as rendered when `--render` applies). PDFs are converted to text either way [default: text] [possible values: text, html]
      --render <RENDER>            When to load an HTML page in a headless browser so its JavaScript runs before the page is stored or its text extracted. `auto` renders only pages whose served HTML looks like a script shell. Needs Chrome or Chromium; set `CHROME` to the binary if it is not found on its own [default: auto] [possible values: auto, always, never]
  -v, --verbose                    Log each request. `RUST_LOG` overrides the level
  -q, --quiet                      Suppress the progress bar and all logging
  -h, --help                       Print help (see more with '--help')
  -V, --version                    Print version
```

### Output modes ###

`--format` decides whether each HTML page is reduced to its text or stored as
HTML, and `--render` decides whether its JavaScript is run in a headless
Chrome first. The two are independent:

|                | JavaScript run (hydrated)        | No JavaScript (dehydrated)      |
| -------------- | -------------------------------- | ------------------------------- |
| Extracted text | `--render always`                | `--render never`                |
| Raw HTML       | `--format html --render always`  | `--format html --render never`  |

The default (`--format text --render auto`) extracts text and renders only
pages whose served HTML looks like a JavaScript shell. `--format html
--render auto` likewise stores the served HTML except for such shells, which
get the rendered DOM. PDFs are converted to text in every combination.
