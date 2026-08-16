# Quantgress

![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Python](https://img.shields.io/badge/python-3.10%2B-blue.svg)
![DuckDB](https://img.shields.io/badge/database-DuckDB-fff000.svg)
![FastAPI](https://img.shields.io/badge/api-FastAPI-009688.svg)
![Data](https://img.shields.io/badge/data-public%20domain-brightgreen.svg)

**A self-hosted, open-source alternative to Quiver Quantitative.** Congressional stock trading disclosures plus 15 adjacent public-data feeds (lobbying, insider trades, 13F holdings, short volume, patents, campaign donations, executive pay, net worth, and more), scraped from primary government sources, normalized into a single [DuckDB](https://duckdb.org/) file, and served over a read-only REST API.

Every dataset here is public U.S. government or public-domain disclosure data: SEC, Senate/House ethics offices, FEC, FINRA, USAspending, USPTO, Wikimedia. Nothing is scraped from a paid or access-controlled source.

```
py q.py "SELECT tkr, count(*) AS n, sum(amount_low) AS min_dollars
         FROM trades WHERE tkr IS NOT NULL
         GROUP BY tkr ORDER BY n DESC LIMIT 5"
```

---

## Table of Contents

- [Why Quantgress](#why-quantgress)
- [Datasets](#datasets)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
- [Quick Start](#quick-start)
- [The API](#the-api)
- [Data Model](#data-model)
- [Scheduling](#scheduling)
- [Legal & Compliance](#legal--compliance)
- [Known Limitations](#known-limitations)
- [Roadmap](#roadmap)
- [License](#license)

---

## Why Quantgress

Products like Quiver Quantitative and CapitolTrades package congressional trading disclosures (and related alternative datasets) into a paid API. All of the underlying data is public. Quantgress is the build-it-yourself version: own scrapers against primary sources, one local database, and a thin API layer over it, auditable end to end. Every recovered or inferred value, like a ticker guessed from free text, is kept separate from what was actually scraped, so it stays reversible.

The project was scoped originally against congressional trades (Senate + House Periodic Transaction Reports under the STOCK Act) and grew, phase by phase, into a near-complete rebuild of Quiver's public dataset catalog.

## Datasets

18 build phases, each independently runnable. ✅ = built and verified against live data.

| # | Dataset | Source | Auth | Cadence | Module |
|---|---|---|---|---|---|
| 1 | Senate stock trades (PTRs) | [efdsearch.senate.gov](https://efdsearch.senate.gov/) | session only | daily | `scrape_senate.py` |
| 2 | House stock trades (PTRs) | [disclosures-clerk.house.gov](https://disclosures-clerk.house.gov/) | none | daily | `scrape_house.py` |
| 3 | Ticker resolution (congress trades) | embedded in filing text | — | — | `entities.py` |
| 4 | Daily incremental run | — | — | daily (cron) | `daily.py` |
| 5 | Read-only REST API | — | — | — | `api.py` |
| 6 | Corporate lobbying | [LDA.gov](https://lda.gov/api/) | free key or anon | as needed | `scrape_lobbying.py` |
| 7 | Government contracts | [USAspending v2](https://api.usaspending.gov/) | none | rolling 7-day | `scrape_contracts.py` |
| 8 | Entity resolution engine | SEC `company_tickers.json` | contact-email UA | — | `entities.py` |
| 9 | Insider trades (Form 4) | SEC bulk data sets + EDGAR daily index | none (UA header) | daily gap-fill | `scrape_insiders.py` |
| 10 | 13F institutional holdings | [SEC Form 13F data sets](https://www.sec.gov/data-research/sec-markets-data/form-13f-data-sets) | none (UA header) | quarterly | `scrape_13f.py` |
| 11 | Off-exchange short volume | [FINRA daily files](https://www.finra.org/finra-data/browse-catalog/short-sale-volume-data) | none | daily | `scrape_short_volume.py` |
| 12 | Patents | [USPTO Open Data Portal](https://data.uspto.gov/) | API key (ID.me) | as needed | `scrape_patents.py` |
| 13 | Corporate/PAC donations | [OpenFEC](https://api.open.fec.gov/) | free API key | as needed | `scrape_donors.py` |
| 14 | Wikipedia pageviews | [Wikimedia REST API](https://doc.wikimedia.org/generated-data-platform/aqs/analytics-api/reference/page-views.html) | none | as needed | `scrape_pageviews.py` |
| 15 | Politician net worth (floor estimate) | derived — `trades` × live prices | none | on demand | `networth.py` |
| 16 | Executive compensation (Pay vs. Performance) | [SEC XBRL Frames API](https://www.sec.gov/edgar/sec-api-documentation) | none (UA header) | as needed | `scrape_execcomp.py` |
| 17 | Donald Trump 278-T trades | [ProPublica DocumentCloud mirror](https://projects.propublica.org/trump-team-financial-disclosures/) | none | manual | `scrape_trump.py` |
| 18 | Senate Annual Financial Disclosure (accurate net worth) | efdsearch.senate.gov | session only | annual | `scrape_senate_annual.py` |

Every scraper is independently runnable, resumable (safe to `Ctrl-C` and re-run), and ships with a `--selftest` flag that validates its parsing logic offline against captured sample data — no network call required.

## Architecture

```
Primary sources (SEC, Senate/House, FEC, FINRA, USPTO, Wikimedia, USAspending)
        │
        ▼
  scrape_*.py  ──── one script per dataset, own table, own resume logic
        │
        ▼
  entities.py  ──── cross-dataset ticker/company resolution
        │             (writes *_guess / *_guess_how, never touches scraped values)
        ▼
  congress_trades.duckdb  ──── single-file embedded OLAP database
        │
        ├── daily.py   ──── scheduled incremental driver (Task Scheduler / cron)
        │
        └── api.py     ──── FastAPI read-only layer, one route per dataset
                              │
                              ▼
                        REST clients / dashboards
```

**Design choices that shape the codebase:**

- **DuckDB, not Postgres.** Single-file, zero-server, embedded OLAP engine. Right fit for a personal research dataset that's queried far more than it's written. 
- **One declarative route table, not one function per dataset.** `api.py`'s `RELATIONS` dict maps a route name to `(relation, filter columns, default order)`; a single generic `/{dataset}` handler serves all of them. Adding a dataset means adding a dict entry, not a new function. Same pattern in `entities.py`'s `SOURCES` list.
- **Guesses are always reversible.** Any value `entities.py` infers (e.g. a ticker recovered from free text) is written to a separate `*_guess` / `*_guess_how` column. The original scraped value is never overwritten, and every inferred class of guess can be audited or reversed with one `UPDATE`.
- **Fresh DB connection per API request.** A long-lived read-only DuckDB connection won't see rows written by another process after it connects, so `api.py` opens and closes a connection per request rather than pooling one at startup. This keeps every response consistent with whatever `daily.py` last committed.

## Project Structure

```
Quantgress/
├── schema.py                 # table DDL + the trades view, shared by both chambers
├── scrape_senate.py          # Phase 1 — Senate PTR scraper
├── scrape_house.py           # Phase 2 — House PTR scraper (PDF parsing)
├── entities.py                # Phase 3/8 — cross-dataset ticker/company resolution
├── daily.py                   # Phase 4 — scheduled incremental driver
├── api.py                     # Phase 5 — read-only FastAPI layer
├── scrape_lobbying.py         # Phase 6 — LDA.gov lobbying filings
├── scrape_contracts.py        # Phase 7 — USAspending contract awards
├── scrape_insiders.py         # Phase 9 — SEC Form 4 insider transactions
├── scrape_13f.py              # Phase 10 — SEC 13F institutional holdings
├── scrape_short_volume.py     # Phase 11 — FINRA off-exchange short volume
├── scrape_patents.py          # Phase 12 — USPTO granted patents
├── scrape_donors.py           # Phase 13 — OpenFEC corporate/PAC donations
├── scrape_pageviews.py        # Phase 14 — Wikimedia pageviews
├── networth.py                # Phase 15/18 — derived net worth (floor + annual)
├── scrape_execcomp.py         # Phase 16 — SEC Pay vs. Performance (XBRL)
├── scrape_trump.py            # Phase 17 — Trump OGE 278-T trades
├── scrape_senate_annual.py    # Phase 18 — Senate Annual Financial Disclosure
├── q.py                       # query helper (sidesteps PowerShell quoting issues)
├── congress_trades.duckdb     # the database (gitignored in a production checkout)
├── requirements.txt
└── .env                       # USPTO_API_KEY / FEC_API_KEY (gitignored)
```

## Getting Started

### Prerequisites

- Python 3.10+ (Windows: use `py`, not `python`; `python` resolves to the Microsoft Store stub)
- No database server — DuckDB ships as a library dependency

### Installation

```bash
git clone <this-repo>
cd Quantgress
py -m pip install -r requirements.txt
```

### API keys (optional, only needed for Phases 12 & 13)

Every other dataset needs no key — either a fully open endpoint or a descriptive `User-Agent`. Patents (USPTO) and corporate donations (OpenFEC) need a free key each. Create a `.env` file in the project root:

```
USPTO_API_KEY=<from data.uspto.gov, requires ID.me verification>
FEC_API_KEY=<from api.data.gov/signup, instant>
```

Both scrapers still run `--selftest` with no key or network access at all.

## Quick Start

```bash
# 1. Validate every parser offline (instant, no network)
py scrape_senate.py --selftest
py scrape_house.py --selftest

# 2. Bounded live runs to confirm access before a full backfill
py scrape_senate.py --limit 20
py scrape_house.py --year 2026 --limit 25

# 3. Resolve tickers embedded in the free-text asset names
py entities.py --dry     # preview, writes nothing
py entities.py           # write recovered tickers

# 4. Query
py q.py                                  # per-senator summary
py q.py --types                          # asset-type breakdown
py q.py "SELECT * FROM trades LIMIT 10"  # arbitrary SQL

# 5. Serve it
py -m uvicorn api:app --reload
curl http://127.0.0.1:8000/trades?tkr=NVDA&limit=5
```

A full historical backfill of just Senate + House trades is ~1.3 hours against 2,411 filings at a self-imposed 2-second rate limit. Every scraper accepts `--limit N` for a bounded test run. Run that before an unbounded one.

The full command reference (every flag, every gotcha, every verified query) lives in [`08 Reference/Quantgress - Command Reference.md`](../Second-Brain/08%20Reference/Quantgress%20-%20Command%20Reference.md) in the accompanying project wiki.

## The API

`api.py` is a read-only FastAPI layer over the database: one generic route per dataset, driven by a declarative `RELATIONS` table, plus three hand-built routes for the original congressional-trades scope.

```bash
py api.py --selftest                             # offline route checks, no server
py -m uvicorn api:app --reload                    # dev server, http://127.0.0.1:8000
py -m uvicorn api:app --host 0.0.0.0 --port 8000   # LAN-visible, trusted networks only
```

| Endpoint | Description |
|---|---|
| `GET /` | Dataset names and row counts |
| `GET /trades` | The unified congress-trades view. Filters: `tkr`, `last_name`, `chamber` |
| `GET /politician/{name}` | Per-politician summary + trade listing (substring match on last name) |
| `GET /ticker/{symbol}` | All activity for one ticker (exact, case-insensitive) |
| `GET /lobbying` `/contracts` `/insiders` `/13f-positions` `/13f-changes` `/13f-top-holders` `/short-volume` `/patents` `/donors` `/pageviews` `/exec-comp` `/trump-trades` `/senate-assets` `/senate-liabilities` | Generic filtered listing per dataset. See `RELATIONS` in `api.py` for exact filter columns |

Every listing route accepts `?limit=` (default 100, max 1000) and `?offset=`. Filter columns use one of three match modes chosen per column: exact (codes/IDs/years), case-insensitive exact (tickers), or case-insensitive substring (names). Not a uniform strategy: exact-matching a name column or substring-matching a ticker column both produce wrong results. Interactive docs are auto-generated by FastAPI at `/docs`.

**Not built:** authentication, rate limiting, and CORS. This is designed to run on `127.0.0.1` for personal/local use — add all three before ever binding to `0.0.0.0` outside a trusted network. See [Legal & Compliance](#legal--compliance) before considering a public deployment.

## Data Model

Query the **`trades`** view, not the raw `senate_trades` / `house_trades` tables. It unions both chambers and resolves three bugs every raw query would otherwise hit:

| Column | What it fixes |
|---|---|
| `chamber` | `'S'` / `'H'`, union of both raw tables |
| `tkr` | `coalesce(ticker, ticker_guess)`; includes tickers recovered from free text |
| `txn_date` / `filed_date` | real `DATE` columns, not `MM/DD/YYYY` text that sorts lexically wrong |
| `tkr_recovered` | flags rows whose ticker came from inference, not the filing itself |

**Two dates matter differently:** `txn_date` is when the trade happened; `filed_date` is when the public could first have known about it. Any signal analysis should key off `filed_date`. Building on `txn_date` bakes in look-ahead bias.

**`amount_low` / `amount_high` are a disclosure bracket, not a price.** The STOCK Act only requires a dollar range, never an exact figure. `sum(amount_low)` is a floor, never a total.

Every non-congress dataset (lobbying, contracts, insider trades, 13F, short volume, patents, donors, pageviews, exec comp, Trump trades, Senate annual disclosures) gets its own standalone table — none of them share `trades`' chamber/ticker shape, so they're not unioned into it.

## Scheduling

`daily.py` is the incremental driver: Senate (full history, but only new filings actually write), House (current + prior calendar year, to catch year-boundary filings), then `entities.py` to resolve tickers on whatever was just added. Every underlying scraper already resumes from what's stored, so a normal day only touches new filings. `daily.py` adds no new scraping logic of its own, only call order.

```bash
py daily.py --selftest    # offline check, instant
py daily.py                # run it once
```

Scheduled with the OS's native scheduler, not a long-running Python process:

```
schtasks /create /tn "Quantgress Daily" /sc daily /st 09:00 ^
  /tr "cmd /c cd /d C:\path\to\Quantgress && py daily.py >> daily.log 2>&1" /f
```

STOCK Act disclosures have a 30–45 day filing window, so a daily cadence is comfortably fast enough; a missed day is invisible and self-heals on the next run.

**Beyond `daily.py`:** only Phases 1-3 run on the schedule above. The other 12 phases (lobbying, contracts, insider trades, 13F, short volume, patents, donors, pageviews, exec comp, Trump trades, Senate annual disclosures) are manual-only today (`py scrape_*.py`) — a real gap once any of that data is served publicly, since a paid tier can't silently go stale. `deploy/cron.d/` has the Linux deployment schedule for the Oracle Cloud box: one `/etc/cron.d` file per cadence group (daily/weekly/quarterly/annual, grouped by each source's actual upstream update frequency, not run nightly regardless) — see [`deploy/cron.d/README.md`](deploy/cron.d/README.md) for the install steps and the full phase-to-cadence mapping. Trump trades (Phase 17) is the one deliberate exception, left manual since ProPublica's mirror has no fixed publication schedule to key a cron line off of.

## Legal & Compliance

Every dataset scraped here is public government or public-domain disclosure data, legal to collect and use for personal/research purposes. **Redistributing it commercially is a separate question with real constraints, surfaced while scoping a paid public API for this project:**

- **Corporate/PAC donations (`corporate_donations`)** — 52 U.S.C. § 30111(a)(4) bars commercial use or solicitation of individual FEC contributor data. `api.py` only ever serves `corporate_donations_agg`, a pre-aggregated view with `contributor_name` and `sub_id` stripped out entirely — no individual donor is identifiable through the API.
- **Off-exchange short volume (`short_volume`)** — FINRA's site-wide Terms of Use bar bulk scraping and commercial redistribution; its own API terms bar building a competing product with the data. Currently unresolved whether the daily short-volume catalog's "public dissemination" framing carries a looser license. **Treat this dataset as personal/research use only until confirmed otherwise directly with FINRA.**
- Every other dataset (SEC EDGAR, USAspending, USPTO, LDA.gov, Senate/House disclosures, Wikimedia) is unrestricted public-domain U.S. government data.

None of the above blocks personal use, local research, or the API as shipped (unauthenticated, `127.0.0.1`-only). It matters only if this is ever exposed publicly or monetized.

## Known Limitations

- **House OCR is not built.** Scanned (image-only) PTR filings are queued (`house_filings.status = 'scanned'`) rather than parsed — roughly an eighth of recent filings, more in older years. `py scrape_house.py --ocr-queue` lists what's waiting.
- **Net worth figures are floor estimates, not real holdings.** Disclosures only ever give a dollar bracket, never a share count — `networth.py`'s default mode sums bracket floors, marked to live prices. `networth.py --annual` (Phase 18, Senate only) is materially more accurate: real Dec-31 asset/liability snapshots from Annual Financial Disclosure Reports, not inferred from trade brackets.
- **Entity resolution coverage is genuinely low for four datasets** (lobbying, contracts, patents, donors) — 2–7% of distinct names match a public ticker on the first pass, by design: most lobbying clients, contract recipients, patent assignees, and donors are private companies, government bodies, or associations with no ticker to have.
- **No `con.close()` in every scraper** (in progress). A completed run's data can sit WAL-only until the connection is cleanly closed. Don't delete a `.duckdb.wal` file without checking whether it holds unflushed data first.
- **Trump 278-T data is OCR'd from scanned forms**, not born-digital text. Query `trump_trades_clean`, not the raw `trump_trades` table, unless specifically auditing the parse. The clean view filters out rows with detectable OCR corruption.

## Roadmap

Not currently planned: WSB/Reddit sentiment, Twitter/X data (API now paid), corporate jet flight tracking (FAA coverage shrinking), House Annual Financial Disclosure (PDF-based, same difficulty class as House PTRs). Full phase-by-phase implementation notes, live-run numbers, and every gotcha found along the way live in the project wiki.

## License

MIT — see [LICENSE](LICENSE).
