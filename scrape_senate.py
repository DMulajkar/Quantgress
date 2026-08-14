"""Scrape Senate periodic transaction reports (PTRs) from efdsearch.senate.gov into DuckDB.

Phase 1 of the Congress Trades project: Senate only. Senate PTRs render as HTML
tables, so no OCR is needed -- House PTRs are scanned PDFs and are a later phase.

Usage:
    py scrape_senate.py            # scrape everything into congress_trades.duckdb
    py scrape_senate.py --limit 20 # stop after 20 filings (full run takes hours)
    py scrape_senate.py --selftest # run parser checks, no network

Re-running skips filings already in the DB, so an interrupted run resumes.
"""

import io
import re
import sys
import time

import duckdb
import pandas as pd
import requests
from bs4 import BeautifulSoup

ROOT = "https://efdsearch.senate.gov"
LANDING = f"{ROOT}/search/home/"
SEARCH = f"{ROOT}/search/"
REPORTS = f"{ROOT}/search/report/data/"

START_DATE = "01/01/2012 00:00:00"
BATCH = 100
RATE_LIMIT_SECS = 2  # be polite to a .gov host
DB_PATH = "congress_trades.duckdb"

# ponytail: three constants beat a config.yaml here; add one if this grows knobs


def _session():
    """Return a session that has accepted the eFD prohibition agreement."""
    s = requests.Session()
    s.headers["User-Agent"] = "congress-trades/0.1 (personal research)"
    r = s.get(LANDING)
    token = BeautifulSoup(r.text, "html.parser").find(
        attrs={"name": "csrfmiddlewaretoken"}
    )["value"]
    s.post(
        LANDING,
        data={"csrfmiddlewaretoken": token, "prohibition_agreement": "1"},
        headers={"Referer": LANDING},
    )
    s.csrf = s.cookies.get("csrftoken") or s.cookies["csrf"]
    return s


def list_ptrs(s):
    """Yield (first, last, office, link, filed) for every PTR since START_DATE."""
    offset = 0
    while True:
        time.sleep(RATE_LIMIT_SECS)
        rows = s.post(
            REPORTS,
            data={
                "start": str(offset),
                "length": str(BATCH),
                "report_types": "[11]",  # 11 = periodic transaction report
                "filer_types": "[]",
                "submitted_start_date": START_DATE,
                "submitted_end_date": "",
                "candidate_state": "",
                "senator_state": "",
                "office_id": "",
                "first_name": "",
                "last_name": "",
                "csrfmiddlewaretoken": s.csrf,
            },
            headers={"Referer": SEARCH},
        ).json()["data"]
        if not rows:
            return
        for first, last, office, report_html, filed in rows:
            href = BeautifulSoup(report_html, "html.parser").a["href"]
            yield first.strip(), last.strip(), office.strip(), href, filed
        offset += BATCH


def parse_amount(text):
    """'$1,001 - $15,000' -> (1001, 15000). Open-ended high returns None."""
    nums = [int(n.replace(",", "")) for n in re.findall(r"[\d,]+", text or "")]
    if not nums:
        return None, None
    return nums[0], (nums[1] if len(nums) > 1 else None)


def parse_ptr(html):
    """Return transaction rows from a PTR page. Empty list if it has no table."""
    tables = pd.read_html(io.StringIO(html))
    if not tables:
        return []
    df = tables[0]
    df.columns = [str(c).strip().lower() for c in df.columns]

    def col(*names):
        for n in names:
            if n in df.columns:
                return df[n]
        return pd.Series([None] * len(df))

    out = pd.DataFrame(
        {
            "tx_date": col("transaction date"),
            "owner": col("owner"),
            "ticker": col("ticker"),
            "asset_name": col("asset name"),
            "asset_type": col("asset type"),
            "tx_type": col("type"),
            "amount_raw": col("amount"),
        }
    )
    out[["amount_low", "amount_high"]] = [
        parse_amount(a) for a in out["amount_raw"]
    ]
    # eFD writes "--" for a missing ticker; normalize that and NaN to None so
    # DuckDB stores real NULLs instead of the string "--" or a float nan
    text = ["tx_date", "owner", "ticker", "asset_name", "asset_type",
            "tx_type", "amount_raw"]
    out[text] = out[text].replace("--", None).astype(object)
    out[text] = out[text].where(pd.notna(out[text]), None)
    return out.to_dict("records")


def ensure_view(con):
    """The `trades` view: what you actually want to query.

    Bakes in the two things every query otherwise repeats -- coalescing the
    recovered ticker, and parsing MM/DD/YYYY strings into real DATEs so
    ORDER BY sorts chronologically instead of lexically.
    """
    con.execute(
        r"""CREATE OR REPLACE VIEW trades AS SELECT
            last_name,
            -- Exchange rows pack both legs into one cell ("--  AMCR" = gave up
            -- an untickered holding, received AMCR). Take the trailing symbol,
            -- i.e. what they hold afterwards. Plain tickers pass through.
            coalesce(
                nullif(nullif(regexp_extract(trim(ticker), '([A-Z.\-]+)$', 1),
                              '--'), ''),
                ticker_guess) AS tkr,
            asset_name, tx_type, amount_low, amount_high,
            try_strptime(tx_date, '%m/%d/%Y')::DATE AS txn_date,
            try_strptime(filed, '%m/%d/%Y')::DATE AS filed_date,
            date_diff('day', try_strptime(tx_date, '%m/%d/%Y')::DATE,
                             try_strptime(filed, '%m/%d/%Y')::DATE) AS lag_days,
            -- secondary columns: filtering, auditing, provenance
            asset_type, owner,
            ticker IS NULL AND ticker_guess IS NOT NULL AS tkr_recovered,
            first_name, office, link
        FROM senate_trades"""
    )


def main(limit=None):
    con = duckdb.connect(DB_PATH)
    con.execute(
        """CREATE TABLE IF NOT EXISTS senate_trades (
            first_name VARCHAR, last_name VARCHAR, office VARCHAR,
            filed VARCHAR, link VARCHAR, tx_date VARCHAR, owner VARCHAR,
            ticker VARCHAR, asset_name VARCHAR, asset_type VARCHAR,
            tx_type VARCHAR, amount_raw VARCHAR,
            amount_low BIGINT, amount_high BIGINT,
            ticker_guess VARCHAR, ticker_guess_how VARCHAR)"""
    )
    ensure_view(con)
    done = {r[0] for r in con.execute("SELECT DISTINCT link FROM senate_trades").fetchall()}

    s = _session()
    skipped_paper = 0
    scraped = 0
    for first, last, office, link, filed in list_ptrs(s):
        if limit is not None and scraped >= limit:
            break
        if link in done:
            continue
        if "/search/view/paper/" in link:
            skipped_paper += 1  # scanned filing; needs OCR, out of scope for phase 1
            continue
        time.sleep(RATE_LIMIT_SECS)
        r = s.get(ROOT + link)
        if r.url == LANDING:  # session expired mid-run
            s = _session()
            r = s.get(ROOT + link)
        rows = parse_ptr(r.text)
        if not rows:
            continue
        df = pd.DataFrame(rows).assign(
            first_name=first, last_name=last, office=office, filed=filed, link=link
        )
        con.execute(
            "INSERT INTO senate_trades (first_name, last_name, office, filed,"
            " link, tx_date, owner, ticker, asset_name, asset_type, tx_type,"
            " amount_raw, amount_low, amount_high)"
            " SELECT first_name, last_name, office, filed, link, tx_date, owner,"
            " ticker, asset_name, asset_type, tx_type, amount_raw, amount_low,"
            " amount_high FROM df"
        )
        scraped += 1
        print(f"{last}, {first} — {len(rows)} txns")

    total = con.execute("SELECT count(*) FROM senate_trades").fetchone()[0]
    print(f"\n{total} transactions in {DB_PATH}; skipped {skipped_paper} scanned filings")


def selftest():
    assert parse_amount("$1,001 - $15,000") == (1001, 15000)
    assert parse_amount("$50,000,001 -") == (50000001, None)
    assert parse_amount("") == (None, None)
    assert parse_amount(None) == (None, None)

    html = """<table><thead><tr><th>#</th><th>Transaction Date</th>
      <th>Owner</th><th>Ticker</th><th>Asset Name</th><th>Asset Type</th>
      <th>Type</th><th>Amount</th></tr></thead><tbody>
      <tr><td>1</td><td>02/18/2026</td><td>Spouse</td><td>MSFT</td>
        <td>Microsoft Corp</td><td>Stock</td><td>Purchase</td>
        <td>$1,001 - $15,000</td></tr>
      <tr><td>2</td><td>02/19/2026</td><td>Self</td><td>--</td>
        <td>Some Muni Bond</td><td>Corporate Bond</td><td>Sale</td>
        <td>$15,001 - $50,000</td></tr></tbody></table>"""
    rows = parse_ptr(html)
    assert len(rows) == 2, rows
    assert rows[0]["ticker"] == "MSFT"
    assert rows[0]["amount_low"] == 1001 and rows[0]["amount_high"] == 15000
    assert rows[1]["ticker"] is None, "'--' should normalize to None"
    assert rows[1]["tx_type"] == "Sale"
    print("selftest ok")


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        n = sys.argv.index("--limit") + 1 if "--limit" in sys.argv else 0
        main(int(sys.argv[n]) if n else None)
