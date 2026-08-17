"""Scrape SEC Form 13F institutional holdings into DuckDB.

Phase 10 of Quantgress: "one SEC pipeline, three Quiver datasets" -- raw
holdings, the quarter-over-quarter diff, and the same data pivoted by issuer
(top shareholders). Same bulk-quarterly-zip shape as Phase 9
(scrape_insiders.py) -- SEC's Form 13F structured data sets, one zip of
tab-delimited tables per quarter -- so this module scrapes one flat table
(`f13_holdings`) and gets the other two datasets for free as SQL views
(`f13_changes`, `f13_top_holders`) computed over it, rather than as separate
scraped tables.

Usage:
    py scrape_13f.py --selftest              # offline checks, no network
    py scrape_13f.py --quarter 2026q1 --limit 50   # bounded backfill
    py scrape_13f.py --quarter 2026q1        # one quarter, full
    py scrape_13f.py                         # latest posted quarter

Re-running skips (accession_number, infotable_sk) already stored, so an
interrupted run resumes -- same pattern as Phase 9.

# ponytail: bulk-only in v1, no "live" EDGAR-daily mode like Phase 9 has.
# 13F is inherently quarterly (45-day deadline after quarter-end) and, per
# the build spec, deliberately does NOT belong on the Phase 4 daily cron --
# there's no "gap since the last bulk zip" to fill on a tight cadence the way
# Phase 9's Form 4 stream needs. Re-run the bulk path once a new quarter's
# zip is posted; that is the entire refresh story this phase needs.

# ponytail: CUSIP, not ticker, is the join key on the wire -- 13F's INFOTABLE
# has no ticker/symbol field at all, unlike Phase 9 where the ticker sat
# right next to the CIK. issuer_name is free text ("APPLE INC"), so this
# reuses entities.py's existing sec_name adapter unchanged (same strategy as
# Phase 6/7's client_name/recipient_name) -- registered as a new SOURCES
# entry there, no new resolution logic needed.

# ponytail: VALUE arrives in the bulk data set in THOUSANDS of dollars (SEC's
# own convention, confirmed against the record layout) -- stored here already
# multiplied out to value_usd so nothing downstream has to remember the
# footgun every time it touches this table.
"""

import csv
import datetime
import io
import re
import sys
import time
import zipfile

import duckdb
import requests

from schema import DB_PATH

INDEX_URL = "https://www.sec.gov/data-research/sec-markets-data/form-13f-data-sets"
# Path prefix isn't stable across quarters (same discovery live taught Phase
# 9) -- read the real href off the index page instead of guessing the URL.
# ponytail: SEC also renamed the *files* partway through, not just the path --
# quarters through 2023q4 are "2026q1_form13f.zip", 2024q1-onward are
# "01mar2026-31may2026_form13f.zip" (window starting the quarter-end month,
# named that way because the 45-day filing deadline runs past quarter-end).
# Both patterns map onto the same YYYYqN key so --quarter 2026q1 keeps working.
ZIP_RE = re.compile(
    r'href="(/files/[^"]+/(?:(\d{4})q([1-4])|01(mar|jun|sep|dec)(\d{4})-\d{2}\w{3}\d{4})_form13f\.zip)"'
)
_MONTH_Q = {"mar": "1", "jun": "2", "sep": "3", "dec": "4"}

TABLE = "f13_holdings"
COLUMNS = ["accession_number", "infotable_sk", "period_of_report", "filed_date",
           "manager_cik", "manager_name", "issuer_name", "cusip", "share_class",
           "value_usd", "shares", "share_type", "put_call", "investment_discretion",
           "voting_auth_sole", "voting_auth_shared", "voting_auth_none"]


def ensure_table(con):
    con.execute(f"""CREATE TABLE IF NOT EXISTS {TABLE} (
        accession_number VARCHAR, infotable_sk INTEGER,
        period_of_report VARCHAR, filed_date VARCHAR,
        manager_cik VARCHAR, manager_name VARCHAR,
        issuer_name VARCHAR, cusip VARCHAR, share_class VARCHAR,
        value_usd DOUBLE, shares DOUBLE, share_type VARCHAR,
        put_call VARCHAR, investment_discretion VARCHAR,
        voting_auth_sole DOUBLE, voting_auth_shared DOUBLE, voting_auth_none DOUBLE,
        PRIMARY KEY (accession_number, infotable_sk))""")


def ensure_views(con):
    """f13_changes and f13_top_holders -- the other two Quiver datasets,
    computed over f13_holdings rather than scraped separately.

    Both read issuer_ticker_guess, which entities.py adds via `ALTER TABLE
    ... ADD COLUMN IF NOT EXISTS` the same way it does for lobbying_filings
    and gov_contracts -- but the views need that column to exist at CREATE
    VIEW time even before entities.py has ever run, so this does the same
    idempotent ALTER itself first.
    """
    con.execute(f"ALTER TABLE {TABLE} ADD COLUMN IF NOT EXISTS issuer_ticker_guess VARCHAR")
    con.execute(f"ALTER TABLE {TABLE} ADD COLUMN IF NOT EXISTS issuer_ticker_guess_how VARCHAR")

    # One canonical row per (manager, cusip, quarter): sum same-cusip splits
    # within one filing (a manager can report sole/shared lots as separate
    # INFOTABLE rows), then pick the most-recently-filed accession_number per
    # quarter so a 13F-HR/A amendment supersedes the original it corrects --
    # same "as filed" -> "as of" collapse the raw table itself intentionally
    # does NOT do (each accession_number/infotable_sk stays its own row there).
    con.execute(f"""
        CREATE OR REPLACE VIEW f13_positions AS
        WITH summed AS (
            SELECT accession_number, manager_cik, manager_name, period_of_report, filed_date,
                   cusip, any_value(issuer_name) AS issuer_name,
                   any_value(issuer_ticker_guess) AS issuer_ticker_guess,
                   sum(value_usd) AS value_usd, sum(shares) AS shares
            FROM {TABLE}
            GROUP BY accession_number, manager_cik, manager_name, period_of_report, filed_date, cusip
        ), ranked AS (
            SELECT *, row_number() OVER (
                PARTITION BY manager_cik, cusip, period_of_report
                ORDER BY filed_date DESC, accession_number DESC
            ) AS rn
            FROM summed
        )
        SELECT manager_cik, manager_name, period_of_report, cusip, issuer_name,
               issuer_ticker_guess, value_usd, shares
        FROM ranked WHERE rn = 1
    """)

    # Quarter-over-quarter diff per (manager, cusip). "new" means no earlier
    # quarter had a row for this pair. NOTE: a fully exited position (a
    # manager drops a stock to zero) is NOT flagged here -- 13F never reports
    # a zero-share row, the position just stops appearing in the next
    # filing, so spotting an exit needs anti-joining against every manager's
    # full history each quarter. Left as a known gap, same as Phase 2's OCR
    # path and Phase 3's name normalization -- documented, not silently missing.
    con.execute("""
        CREATE OR REPLACE VIEW f13_changes AS
        SELECT manager_cik, manager_name, cusip, issuer_name, issuer_ticker_guess,
               period_of_report,
               lag(period_of_report) OVER w AS prior_period,
               value_usd, lag(value_usd) OVER w AS prior_value_usd,
               value_usd - lag(value_usd) OVER w AS value_change_usd,
               shares, lag(shares) OVER w AS prior_shares,
               shares - lag(shares) OVER w AS share_change,
               CASE WHEN lag(shares) OVER w IS NULL THEN 'new'
                    WHEN shares > lag(shares) OVER w THEN 'increased'
                    WHEN shares < lag(shares) OVER w THEN 'decreased'
                    ELSE 'unchanged' END AS change_type
        FROM f13_positions
        WINDOW w AS (PARTITION BY manager_cik, cusip ORDER BY period_of_report)
    """)

    # Same positions, pivoted by issuer: who holds the most of each stock,
    # per quarter.
    con.execute("""
        CREATE OR REPLACE VIEW f13_top_holders AS
        SELECT cusip, issuer_name, issuer_ticker_guess, period_of_report,
               manager_cik, manager_name, value_usd, shares,
               row_number() OVER (
                   PARTITION BY cusip, period_of_report
                   ORDER BY value_usd DESC NULLS LAST
               ) AS rank
        FROM f13_positions
    """)


def _num(x):
    return None if x in (None, "") else float(x)


def _iso_date(s):
    """'30-JUN-2026' -> '2026-06-30'. Same DD-MON-YYYY layout Phase 9 parses."""
    if not s:
        return None
    try:
        return datetime.datetime.strptime(s, "%d-%b-%Y").date().isoformat()
    except ValueError:
        return None


# ---------------------------------------------------------------- bulk mode

def _read_tsv(zf, name):
    with zf.open(name) as f:
        return list(csv.DictReader(io.TextIOWrapper(f, encoding="utf-8", errors="replace"),
                                    delimiter="\t"))


def read_quarter_zip(content):
    """Yields exactly one (sub_rows, cover_rows, info_row_iter) triple.

    SUBMISSION.tsv/COVERPAGE.tsv: one row per filing, small, still fully
    materialized via _read_tsv. INFOTABLE.tsv: one row per holding, millions
    of rows for a big quarter -- stays a lazy csv.DictReader instead of a
    list (that list() was OOM-killing this script on the 956Mi-RAM Oracle
    box, confirmed live 2026-08-17 at anon-rss ~747MB). Caller must drive
    info_row_iter to exhaustion before the zip closes -- it stays open only
    while this generator is suspended at yield:
        for sub, cover, info in read_quarter_zip(content):
            for row in parse_bulk_quarter(sub, cover, info):
                ...
    """
    with zipfile.ZipFile(io.BytesIO(content)) as zf:
        sub = _read_tsv(zf, "SUBMISSION.tsv")
        cover = _read_tsv(zf, "COVERPAGE.tsv")
        with zf.open("INFOTABLE.tsv") as f:
            info = csv.DictReader(
                io.TextIOWrapper(f, encoding="utf-8", errors="replace"), delimiter="\t")
            yield sub, cover, info


def parse_bulk_quarter(sub_rows, cover_rows, info_rows):
    """Join SUBMISSION + COVERPAGE + INFOTABLE on ACCESSION_NUMBER.

    13F-NT (notice only -- manager reports no holdings, another manager files
    on its behalf) and 13F-COMBO cover pages are skipped by construction:
    only SUBMISSIONTYPE starting '13F-HR' (the original or an amendment)
    ever has INFOTABLE rows worth keeping, same "does this row have an
    actual position" filter as Phase 9's Form 3/5 skip.
    """
    sub_by_acc = {s["ACCESSION_NUMBER"]: s for s in sub_rows
                  if s["SUBMISSIONTYPE"].startswith("13F-HR")}
    cover_by_acc = {c["ACCESSION_NUMBER"]: c for c in cover_rows}

    for i in info_rows:
        acc = i["ACCESSION_NUMBER"]
        sub = sub_by_acc.get(acc)
        if not sub:
            continue
        cover = cover_by_acc.get(acc, {})
        value = _num(i.get("VALUE"))
        yield {
            "accession_number": acc, "infotable_sk": int(i["INFOTABLE_SK"]),
            "period_of_report": _iso_date(sub["PERIODOFREPORT"]),
            "filed_date": _iso_date(sub["FILING_DATE"]),
            "manager_cik": sub["CIK"], "manager_name": cover.get("FILINGMANAGER_NAME"),
            "issuer_name": i["NAMEOFISSUER"], "cusip": i["CUSIP"],
            "share_class": i.get("TITLEOFCLASS") or None,
            "value_usd": value * 1000 if value is not None else None,  # SEC reports VALUE in thousands
            "shares": _num(i.get("SSHPRNAMT")),
            "share_type": i.get("SSHPRNAMTTYPE") or None,
            "put_call": i.get("PUTCALL") or None,
            "investment_discretion": i.get("INVESTMENTDISCRETION") or None,
            "voting_auth_sole": _num(i.get("VOTING_AUTH_SOLE")),
            "voting_auth_shared": _num(i.get("VOTING_AUTH_SHARED")),
            "voting_auth_none": _num(i.get("VOTING_AUTH_NONE")),
        }


def list_periods(s):
    """{'2026q2': full_url, ...} scraped off the index page, newest last."""
    r = _get(s, INDEX_URL)
    if r is None:
        return {}
    out = {}
    for path, y, q, mon, y2 in ZIP_RE.findall(r.text):
        key = f"{y}q{q}" if y else f"{y2}q{_MONTH_Q[mon]}"
        out[key] = "https://www.sec.gov" + path
    return out


# --------------------------------------------------------------- networking

def _session():
    s = requests.Session()
    s.headers["User-Agent"] = "quantgress/0.1 (mmulajkar@gmail.com)"  # sec.gov 403s without a contact email
    return s


def _get(s, url, tries=4):
    for attempt in range(tries):
        try:
            r = s.get(url, timeout=30)
        except requests.exceptions.RequestException:
            if attempt == tries - 1:
                raise
            time.sleep(5 * (attempt + 1))
            continue
        if r.status_code == 200:
            return r
        if r.status_code == 404:
            return None
        if attempt == tries - 1:
            r.raise_for_status()
        time.sleep(5 * (attempt + 1))


# --------------------------------------------------------------------- main

def main_bulk(quarter=None, limit=None):
    con = duckdb.connect(DB_PATH)
    ensure_table(con)
    done = {(r[0], r[1]) for r in con.execute(
        f"SELECT accession_number, infotable_sk FROM {TABLE}").fetchall()}

    s = _session()
    periods = list_periods(s)
    if not periods:
        sys.exit("could not read the 13F structured data set index page")
    quarter = quarter or max(periods)
    url = periods.get(quarter)
    if not url:
        sys.exit(f"unknown quarter {quarter!r}; have {min(periods)}..{max(periods)}")

    print(f"{quarter}: downloading {url}")
    r = _get(s, url)
    content = r.content
    del r  # drop the Response (incl. its internal buffers) before the
           # streaming parse starts -- free insurance, not the actual fix

    added = skipped = 0
    insert_sql = f"INSERT INTO {TABLE} ({','.join(COLUMNS)}) VALUES ({','.join('?' * len(COLUMNS))})"
    for sub, cover, info in read_quarter_zip(content):
        print(f"  {len(sub)} submissions, {len(cover)} cover pages")
        info_count = 0

        def _tap(it):
            nonlocal info_count
            for row in it:
                info_count += 1
                yield row

        for row in parse_bulk_quarter(sub, cover, _tap(info)):
            if limit is not None and added >= limit:
                break
            key = (row["accession_number"], row["infotable_sk"])
            if key in done:
                skipped += 1
                continue
            con.execute(insert_sql, [row[c] for c in COLUMNS])
            done.add(key)
            added += 1
            if added % 5000 == 0:
                print(f"  ...{added} added so far")

    ensure_views(con)
    total = con.execute(f"SELECT count(*) FROM {TABLE}").fetchone()[0]
    print(f"\n{total} 13F holdings rows in {DB_PATH}; this run added {added},"
          f" skipped {skipped} already-stored, out of {info_count} info-table rows read")


SAMPLE_SUB = [
    {"ACCESSION_NUMBER": "A1", "FILING_DATE": "15-AUG-2026", "SUBMISSIONTYPE": "13F-HR",
     "CIK": "0001067983", "PERIODOFREPORT": "30-JUN-2026"},
    # notice-only filing -- manager reports no holdings itself -- must be dropped
    {"ACCESSION_NUMBER": "A2", "FILING_DATE": "16-AUG-2026", "SUBMISSIONTYPE": "13F-NT",
     "CIK": "0009999999", "PERIODOFREPORT": "30-JUN-2026"},
]
SAMPLE_COVER = [{"ACCESSION_NUMBER": "A1", "FILINGMANAGER_NAME": "Berkshire Hathaway Inc"}]
SAMPLE_INFO = [
    {"ACCESSION_NUMBER": "A1", "INFOTABLE_SK": "1", "NAMEOFISSUER": "APPLE INC",
     "TITLEOFCLASS": "COM", "CUSIP": "037833100", "VALUE": "150000",
     "SSHPRNAMT": "1000000", "SSHPRNAMTTYPE": "SH", "PUTCALL": "",
     "INVESTMENTDISCRETION": "SOLE", "VOTING_AUTH_SOLE": "1000000",
     "VOTING_AUTH_SHARED": "0", "VOTING_AUTH_NONE": "0"},
    # belongs to the dropped 13F-NT submission -- must not appear in the output
    {"ACCESSION_NUMBER": "A2", "INFOTABLE_SK": "1", "NAMEOFISSUER": "SHOULD NOT APPEAR",
     "TITLEOFCLASS": "COM", "CUSIP": "000000000", "VALUE": "1",
     "SSHPRNAMT": "1", "SSHPRNAMTTYPE": "SH", "PUTCALL": "",
     "INVESTMENTDISCRETION": "SOLE", "VOTING_AUTH_SOLE": "1",
     "VOTING_AUTH_SHARED": "0", "VOTING_AUTH_NONE": "0"},
]


def selftest():
    # --- bulk join ---
    rows = list(parse_bulk_quarter(SAMPLE_SUB, SAMPLE_COVER, SAMPLE_INFO))
    assert len(rows) == 1, "13F-NT (notice-only, no holdings) must be dropped"
    r = rows[0]
    assert r["accession_number"] == "A1" and r["infotable_sk"] == 1
    assert r["period_of_report"] == "2026-06-30" and r["filed_date"] == "2026-08-15"
    assert r["manager_cik"] == "0001067983" and r["manager_name"] == "Berkshire Hathaway Inc"
    assert r["issuer_name"] == "APPLE INC" and r["cusip"] == "037833100"
    assert r["value_usd"] == 150_000_000.0, "VALUE is reported in thousands -- must be scaled to real dollars"
    assert r["shares"] == 1_000_000.0
    assert r["put_call"] is None, "blank PUTCALL must be None, not ''"

    assert _iso_date("30-JUN-2026") == "2026-06-30"
    assert _iso_date("") is None and _iso_date(None) is None

    # --- read_quarter_zip: round-trip a real zip, and guard against ever
    # regressing INFOTABLE.tsv back to a fully-materialized list (that list()
    # was the OOM -- see read_quarter_zip's docstring) ---
    def _write_tsv(zf, name, rows):
        buf = io.StringIO()
        w = csv.DictWriter(buf, fieldnames=rows[0].keys(), delimiter="\t")
        w.writeheader()
        w.writerows(rows)
        zf.writestr(name, buf.getvalue())

    zip_buf = io.BytesIO()
    with zipfile.ZipFile(zip_buf, "w") as zf:
        _write_tsv(zf, "SUBMISSION.tsv", SAMPLE_SUB)
        _write_tsv(zf, "COVERPAGE.tsv", SAMPLE_COVER)
        _write_tsv(zf, "INFOTABLE.tsv", SAMPLE_INFO)

    zrows = []
    quarters_seen = 0
    for zsub, zcover, zinfo in read_quarter_zip(zip_buf.getvalue()):
        quarters_seen += 1
        assert not isinstance(zinfo, list), "INFOTABLE.tsv must stay a lazy iterator, not a materialized list"
        zrows = list(parse_bulk_quarter(zsub, zcover, zinfo))  # must consume zinfo before this loop advances
    assert quarters_seen == 1
    assert len(zrows) == 1 and zrows[0]["issuer_name"] == "APPLE INC", zrows

    # --- list_periods URL discovery, both path-prefix variants ---
    sample_html = (
        'a href="/files/datastandardsinnovation/data/form-13f-structured-data-sets/2026q2_form13f.zip"'
        'a href="/files/structureddata/data/form-13f-structured-data-sets/2025q4_form13f.zip"'
    )

    class _FakeIndexResp:
        text = sample_html
        status_code = 200

    class _FakeIndexSession:
        def get(self, *a, **k):
            return _FakeIndexResp()

    periods = list_periods(_FakeIndexSession())
    assert periods["2026q2"].endswith(
        "/datastandardsinnovation/data/form-13f-structured-data-sets/2026q2_form13f.zip")
    assert periods["2025q4"].endswith(
        "/structureddata/data/form-13f-structured-data-sets/2025q4_form13f.zip")
    assert max(periods) == "2026q2", "lexical sort of 'YYYYqN' must still pick the newest quarter"

    # --- views: quarter-over-quarter diff + top holders, over synthetic data ---
    con = duckdb.connect(":memory:")
    ensure_table(con)
    insert_sql = f"INSERT INTO {TABLE} ({','.join(COLUMNS)}) VALUES ({','.join('?' * len(COLUMNS))})"
    synthetic = [
        # Berkshire's AAPL position grows Q1 -> Q2
        dict(accession_number="B-Q1", infotable_sk=1, period_of_report="2026-03-31", filed_date="2026-05-10",
             manager_cik="BRK", manager_name="Berkshire Hathaway Inc", issuer_name="APPLE INC",
             cusip="037833100", share_class="COM", value_usd=100_000_000.0, shares=900_000.0,
             share_type="SH", put_call=None, investment_discretion="SOLE",
             voting_auth_sole=900_000.0, voting_auth_shared=0.0, voting_auth_none=0.0),
        dict(accession_number="B-Q2", infotable_sk=1, period_of_report="2026-06-30", filed_date="2026-08-15",
             manager_cik="BRK", manager_name="Berkshire Hathaway Inc", issuer_name="APPLE INC",
             cusip="037833100", share_class="COM", value_usd=150_000_000.0, shares=1_000_000.0,
             share_type="SH", put_call=None, investment_discretion="SOLE",
             voting_auth_sole=1_000_000.0, voting_auth_shared=0.0, voting_auth_none=0.0),
        # a second manager opening a brand-new AAPL position in Q2 -- no prior row
        dict(accession_number="V-Q2", infotable_sk=1, period_of_report="2026-06-30", filed_date="2026-08-12",
             manager_cik="VANG", manager_name="Vanguard Group Inc", issuer_name="APPLE INC",
             cusip="037833100", share_class="COM", value_usd=50_000_000.0, shares=300_000.0,
             share_type="SH", put_call=None, investment_discretion="SOLE",
             voting_auth_sole=300_000.0, voting_auth_shared=0.0, voting_auth_none=0.0),
    ]
    for row in synthetic:
        con.execute(insert_sql, [row[c] for c in COLUMNS])
    ensure_views(con)

    changes = con.execute(
        "SELECT manager_cik, change_type, share_change, value_change_usd FROM f13_changes "
        "WHERE cusip='037833100' AND period_of_report='2026-06-30' ORDER BY manager_cik"
    ).fetchall()
    assert changes == [
        ("BRK", "increased", 100_000.0, 50_000_000.0),
        ("VANG", "new", None, None),
    ], changes

    top = con.execute(
        "SELECT manager_cik, rank FROM f13_top_holders "
        "WHERE cusip='037833100' AND period_of_report='2026-06-30' ORDER BY rank"
    ).fetchall()
    assert top == [("BRK", 1), ("VANG", 2)], top

    # re-running ensure_views (idempotent ALTER + CREATE OR REPLACE) must not error
    ensure_views(con)

    # _get retries a connection-level failure, and does not retry a 404
    class _FlakyThenOK:
        calls = 0

        def get(self, *a, **k):
            _FlakyThenOK.calls += 1
            if _FlakyThenOK.calls < 3:
                raise requests.exceptions.ConnectionError("simulated DNS blip")
            return type("R", (), {"status_code": 200})()

    real_sleep, time.sleep = time.sleep, lambda _: None
    try:
        resp = _get(_FlakyThenOK(), "http://example.invalid", tries=4)
    finally:
        time.sleep = real_sleep
    assert resp.status_code == 200 and _FlakyThenOK.calls == 3

    print("selftest ok")


def arg(flag, default=None):
    return sys.argv[sys.argv.index(flag) + 1] if flag in sys.argv else default


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        n = arg("--limit")
        main_bulk(arg("--quarter"), int(n) if n else None)
