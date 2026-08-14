# Quantgress

Congress trades first, then the rest of the [[Quiver Quant API - Dataset Catalog]], ordered cheapest-first.

**DB:** `congress_trades.duckdb` (DuckDB). Shape is shared in `schema.py` — `senate_trades` and `house_trades` are both written through it so neither scraper can silently redefine the `trades` view. Every scraper here is resumable: re-running skips rows already stored.

**Ticker safety rule (Phase 3, never relaxed):** every adapter writes to its own `<col>_guess` / `<col>_guess_how` columns, never touching a real scraped value. A NULL real ticker is a fact about the source; a guess is a derived column. The two must stay separable.

## Status

- Phase 0 — bootstrap from existing dumps. **Skipped deliberately** — went straight to the real scraper, since owning the scraper was the actual point.
- Phase 1 — Senate scraper. ✅ DONE
- Phase 2 — House scraper. ✅ DONE (OCR path deliberately not built)
- Phase 3 — ticker resolution. ✅ DONE (name normalization still open)
- Phase 4 — daily cron. ✅ DONE
- Phase 5 — FastAPI layer. ✅ DONE
- Phase 6 — corporate lobbying. ✅ DONE
- Phase 7 — government contracts. ✅ DONE
- Phase 8 — entity resolution refactor. ✅ DONE
- Phase 9 — insider trades (Form 4). ✅ DONE
- Phase 10 — 13F holdings, changes, top shareholders. *pending*
- Phase 11 — off-exchange short volume. *pending*
- Phase 12 — patents. *pending*
- Phase 13 — corporate donors. *pending*
- Phase 14 — Wikipedia pageviews. ✅ DONE (built out of order, before 8–13)

Ordering note: phases are ordered cheapest-first, with **one exception — Phase 8 is a refactor that blocks everything after it, so it is not skippable for convenience.** Phase 14 breaks the order deliberately (see below).

---

## Phase 1 — Senate scraper — ✅ DONE

**Module:** `scrape_senate.py`

Senate PTRs from [efdsearch.senate.gov](https://efdsearch.senate.gov) render as HTML tables, so **no OCR needed** — built first for exactly that reason.

```
py scrape_senate.py            # scrape everything into congress_trades.duckdb
py scrape_senate.py --limit 20 # stop after 20 filings (full run takes hours)
py scrape_senate.py --selftest # run parser checks, no network
```

Re-running skips filings already in the DB, so an interrupted run resumes.

**Gotchas:** the site gates on a session/CSRF flow. Full run takes hours.

## Phase 2 — House scraper — ✅ DONE

**Module:** `scrape_house.py`

Annual disclosure ZIP index at disclosures-clerk.house.gov → rows with `FilingType 'P'` are PTRs → per-filing PDF at `ptr-pdfs/{year}/{DocID}.pdf` → `pdfplumber` text extraction.

```
py scrape_house.py --selftest          # offline parser checks
py scrape_house.py --year 2026 --limit 20
py scrape_house.py --year 2026         # one year
py scrape_house.py                     # 2012 -> present
py scrape_house.py --ocr-queue         # filings that need OCR
```

**OCR path deliberately not built:** most PTRs are born-digital, so the text layer is enough. Roughly an eighth are scanned paper (worse in older years); those extract to nothing and are recorded with `status='scanned'` instead of blocking the run. `--ocr-queue` lists them. Scanned filings are queued, not blocked on.

**Gotchas:**
- Pre-2018 PDFs render small caps as lowercase glyphs — a ticker arrives as `"RoP"` or `"aaPl"`. `entities.py` handles this (see Phase 8).
- Re-running skips filings already attempted (by `doc_id`), so an interrupted run resumes.

## Phase 3 — ticker resolution — ✅ DONE

Coverage went 535 → 875 of 890 rows (60% → 98%).

**The planned approach was wrong:** the first version did fuzzy name matching and mapped "ABB Ltd." to the wrong security (ABLZF instead of ABBNY). It was deleted. The rule that survived is in `entities.py` — normalization + **exact match only**, no fuzzy matching, and an ambiguous normalized name (two real tickers collapsing to one string) is dropped rather than guessed at.

Name normalization to SEC company names is still open.

## Phase 4 — daily cron — ✅ DONE

**Module:** `daily.py`

```
py daily.py            # senate (all years) + house (this year, last year) + tickers
py daily.py --selftest # offline check of the year window, no network
```

Each step already does its own incremental work (senate skips links already in `senate_trades`, house skips `doc_ids` already in `house_filings`), so this file is just call order, not new logic.

**Gotchas:**
- House only needs current + previous year: the annual ZIP is keyed by filing year, and a late-Dec filing can land in next year's index in January, so the prior year stays live a few weeks past New Year's. Older years never gain filings after publication.
- STOCK Act gives filers 30–45 days to disclose, so **once-a-day is plenty**.
- Runs under Windows Task Scheduler; stdout/stderr redirect to `daily.log` at the scheduler level (`cmd /c ... >> daily.log 2>&1`).

## Phase 5 — FastAPI layer — ✅ DONE

Endpoints: `/trades`, `/politician/{name}`, `/ticker/{symbol}`, all over a **read-only DuckDB connection**.

Note: no FastAPI module exists in the repo yet (no `app.py`/`main.py`, no `fastapi`/`uvicorn` in `requirements.txt`). The API surface is specified here; the implementation file is still to be committed. `q.py` is the current query surface — it queries the `trades` view, not the raw `senate_trades`/`house_trades` tables (the view has recovered tickers coalesced in, real DATE columns, and both chambers).

## Phase 6 — corporate lobbying — ✅ DONE

**Module:** `scrape_lobbying.py`

Senate LDA filings via the [LDA.gov API](https://lda.gov/api/). Plain JSON REST — no session gate, no PDFs — the opposite of Phase 2. Each filing already carries registrant, client and lobbying-activity data nested in one response, so no per-filing detail call.

```
py scrape_lobbying.py --selftest                 # offline checks, no network
py scrape_lobbying.py --year 2026 --limit 20     # bounded run
py scrape_lobbying.py --year 2026                # one year (~55-110k filings)
py scrape_lobbying.py                            # current year only
```

Re-running skips `filing_uuids` already stored.

> **Correction (2026-08-14):** the original spec said this dataset "joins to `trades` on ticker." **Confirmed wrong by building it** — the LDA API has no ticker field anywhere; `client_name` is free text (`"CITY OF SOMERTON (AZ)"`, `"NEXXUS CONSULTING, LLC"`). Phase 6 needs the same Phase 8 entity resolution as every later phase; it does not get a free pass.

**Gotchas:**
- Default scope is current year only: the API hard-caps `page_size` at 25 regardless of what's requested (measured: asked for 250, got 25), and a single year already runs 55k–110k filings — a 2012–present backfill would be tens of thousands of pages.

## Phase 7 — government contracts — ✅ DONE

**Module:** `scrape_contracts.py`

[USAspending v2](https://api.usaspending.gov/) — **no API key at all**. Largest dollar figures in the catalog; same shape of work as Phase 6, built immediately after while the pattern was fresh. One POST endpoint returns fully flattened award rows, so no per-award detail call.

```
py scrape_contracts.py --selftest                    # offline checks, no network
py scrape_contracts.py --limit 20                     # bounded run, last 7 days
py scrape_contracts.py --days 30                      # last 30 days of contract actions
py scrape_contracts.py --start 2026-01-01 --end 2026-01-31 --limit 500
py scrape_contracts.py                                # last 7 days, unbounded
```

Re-running skips `generated_internal_ids` already stored.

**Gotchas:**
- Default window is 7 days, not lobbying's current-year: a partial 2026 (Jan–Aug) already has 2.6M contract awards — orders of magnitude larger than any prior dataset.

## Phase 8 — entity resolution refactor — ✅ DONE

**Module:** `entities.py`

Generalized `resolve_tickers.py` into one engine with a per-source adapter list, now that Phases 6–7 proved the problem recurs with a different key per dataset: `asset_name` for congress trades, `client_name` for lobbying, recipient name for contracts — and it doesn't stop there (Phase 9+).

```
py entities.py            # resolve every registered source, write guesses
py entities.py --dry      # preview, write nothing
py entities.py --selftest # offline checks, no network
```

**Two resolution strategies, ranked by trust exactly like Phase 3 taught:**
1. **`extract`** — the ticker is already embedded in the text (congress trades' `asset_name` has it in parens). Just read it out; this is `resolve_tickers.py`'s original logic moved here unchanged. Handles two layouts plus the Phase 2 small-caps case: trailing parens `"Roper Technologies, Inc. - Common Stock (ROP)"`, leading prefix `"ACN - Accenture plc Class A Ordinary Shares (Ireland)"`, and lowercase-glyph pre-2018 House PDFs.
2. **`sec_name`** — normalize a free-text company name and look it up against SEC's own public company list, **exact match only after normalization**. No fuzzy matching (that's what mapped ABB to the wrong security in Phase 3's deleted first version). An ambiguous normalized name is dropped rather than guessed at.

**Safety property (kept from Phase 3):** every adapter writes its own `<col>_guess` / `<col>_guess_how` pair, never touching a real scraped value. Types that genuinely have no symbol (bonds, munis, real property, private LLCs) keep a NULL ticker — that is correct, not a parse failure.

## Phase 9 — insider trades (Form 4) — ✅ DONE

**Module:** `scrape_insiders.py`

SEC quarterly data sets for the 2006→ backfill, EDGAR daily index for live. Two access paths for one dataset:

- **`bulk`** — quarterly Insider Transactions Data Sets (2006 → latest posted quarter), one zip of tab-delimited tables per quarter.
- **`live`** — EDGAR daily index + per-filing ownership XML, for the days since the last quarterly zip was posted (the bulk data set always lags by up to a quarter).

Sequenced ahead of the name-matched datasets because CIK → ticker is a lookup table, not an inference — it exercises the new `entities.py` on easy mode first.

> **Correction (build time):** the "CIK → ticker is a lookup table" framing undersold it. The ticker is already sitting right next to the CIK in both the bulk `SUBMISSION` table and the live XML (`ISSUERTRADINGSYMBOL` / `issuerTradingSymbol` on the issuer, not the reporting owner) — so there's no lookup to do, and **Phase 9 needs no `entities.py` resolution at all**.

**Scope cut in v1:** only Table I (non-derivative) transactions — the actual reported buy/sell of the underlying stock. Table II (derivatives: options, RSUs, swaps) and the two holdings-only tables are a different, more complex signal and are left out.

## Phase 10 — 13F holdings, changes, top shareholders — *pending*

One SEC pipeline, three Quiver datasets: raw holdings, the quarter-over-quarter diff, and the same data pivoted by issuer. Best datasets-per-unit-effort in the whole catalog.

**Gotchas:** quarterly refresh — does **not** belong on the Phase 4 daily cron.

## Phase 11 — off-exchange short volume — *pending*

FINRA daily files, fixed-layout, posted by 6pm ET on the trade date. Genuinely daily — the first dataset here that rewards the cron (unlike Phase 10).

## Phase 12 — patents — *pending*

USPTO Open Data Portal. First dataset with no clean key at all: assignee name → ticker. The real test of Phase 8.

## Phase 13 — corporate donors — *pending*

OpenFEC. Committee/donor name matching, same difficulty class as Phase 12, and it closes the political-money loop opened in Phase 6 — donations in, lobbying out, trades alongside.

## Phase 14 — Wikipedia pageviews — ✅ DONE (built out of order)

**Module:** `scrape_pageviews.py`

Wikimedia API, no key. Cheap to build but sequenced last in the original plan because it is an **attention signal rather than a disclosure record** — a different kind of data from everything above it, and the one most likely to be noise. Compare against [[Silly Alternative-Data Trading Signal Ideas]]'s Google Trends findings before trusting it.

Different in shape from every other scraper: there is no feed to walk. Phases 6/7 hand back every filing/award that exists; this API only answers "how many views did *this* article get" for an article you already know the title of. There is no ticker→Wikipedia-title mapping yet (that's Phase 8's job), so the script takes article titles explicitly.

```
py scrape_pageviews.py --selftest                    # offline checks, no network
py scrape_pageviews.py --article "Tesla, Inc."        # last 30 days, one article
py scrape_pageviews.py --article "Tesla, Inc." --article "Apple Inc." --days 90
py scrape_pageviews.py --article "Tesla, Inc." --start 2026-01-01 --end 2026-01-31
```

Re-running skips `(article, date)` pairs already stored.
