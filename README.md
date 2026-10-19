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
  -t, --timeout <TIMEOUT>          Per-request timeout in seconds [default: 30]
  -v, --verbose                    Log each request. `RUST_LOG` overrides the level
  -q, --quiet                      Suppress the progress bar and all logging
      --raw-html                   Store each HTML page exactly as fetched instead of its extracted text. PDFs are still converted to text
  -h, --help                       Print help
  -V, --version                    Print version
```

