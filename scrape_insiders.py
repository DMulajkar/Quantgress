"""Scrape SEC Form 4 insider transactions into DuckDB.

Phase 9 of Quantgress: two access paths for one dataset, per the build spec.
  - "bulk"  -- quarterly Insider Transactions Data Sets (2006->latest posted
    quarter), one zip of tab-delimited tables per quarter.
  - "live"  -- EDGAR daily index + per-filing ownership XML, for the days
    since the last quarterly zip was posted (the bulk data set always lags
    by up to a quarter).

Ticker comes straight from the source (ISSUERTRADINGSYMBOL /
issuerTradingSymbol on the issuer, not the reporting owner) -- unlike every
phase since 6, this needs no entities.py resolution at all. The build spec's
"CIK -> ticker is a lookup table" framing undersold it: the ticker is already
sitting right next to the CIK in both the bulk SUBMISSION table and the live
XML, so there's no lookup to do either.

Only Table I (non-derivative) transactions -- the actual reported buy/sell of
the underlying stock. Table II (derivative: options, RSUs, swaps) and the two
holdings-only tables are a different, more complex signal and are left out of
v1.
# ponytail: skip Table II. Table I is the "did an insider buy or sell the
# stock" signal every other phase's `trades` shape already matches; adding
# derivatives means a second row shape (strike price, exercise/expiration
# dates) for a signal nothing here consumes yet.

Usage:
    py scrape_insiders.py --selftest                # offline checks, no network
    py scrape_insiders.py --quarter 2026q2 --limit 50   # bounded backfill
    py scrape_insiders.py --quarter 2026q2           # one quarter, full
    py scrape_insiders.py                            # latest posted quarter
    py scrape_insiders.py --live --days 7            # EDGAR daily index, last N days
    py scrape_insiders.py --live --days 7 --limit 50 # bounded

Re-running skips (accession_number, trans_seq) already stored, so both an
interrupted bulk run and a repeated live run resume/dedupe cleanly. A Form
4/A amendment gets its own accession_number and is stored as its own row --
same "as filed" posture as the raw congress trade tables, no attempt to
collapse amendments into the original.
"""

import csv
import datetime
import io
import re
import sys
import time
import xml.etree.ElementTree as ET
import zipfile
from collections import Counter, defaultdict

import duckdb
import requests

from schema import DB_PATH

INDEX_URL = "https://www.sec.gov/data-research/sec-markets-data/insider-transactions-data-sets"
# Path prefix isn't stable across quarters -- older zips sit under
# .../structureddata/..., the newest under .../datastandardsinnovation/...,
# confirmed live. Read the real href off the index page instead of building
# the URL from a guessed pattern.
ZIP_RE = re.compile(r'href="(/files/[^"]+/(\d{4})q([1-4])_form345\.zip)"')
XML_BLOCK_RE = re.compile(r"<XML>\s*(<\?xml.*?</ownershipDocument>)\s*</XML>",
                           re.DOTALL | re.IGNORECASE)

RATE_LIMIT_SECS = 0.15  # SEC's own guidance caps automated access at 10 req/sec
# ponytail: 7, same reasoning as Phase 7 -- filling the whole bulk-to-live gap
# after a fresh quarter boundary can take more than one run; --days widens it.
DAYS_DEFAULT = 7

TABLE = "insider_trades"
COLUMNS = ["accession_number", "trans_seq", "filed_date", "trans_date",
           "issuer_cik", "issuer_name", "ticker",
           "owner_cik", "owner_name", "owner_relationship",
           "security_title", "trans_code", "acquired_disposed",
           "shares", "price_per_share", "shares_owned_following"]


def ensure_table(con):
    con.execute(f"""CREATE TABLE IF NOT EXISTS {TABLE} (
        accession_number VARCHAR, trans_seq INTEGER,
        filed_date VARCHAR, trans_date VARCHAR,
        issuer_cik VARCHAR, issuer_name VARCHAR, ticker VARCHAR,
        owner_cik VARCHAR, owner_name VARCHAR, owner_relationship VARCHAR,
        security_title VARCHAR, trans_code VARCHAR, acquired_disposed VARCHAR,
        shares DOUBLE, price_per_share DOUBLE, shares_owned_following DOUBLE,
        PRIMARY KEY (accession_number, trans_seq))""")


def _num(x):
    return None if x in (None, "") else float(x)


def _iso_date(s):
    """'31-OCT-2025' -> '2025-10-31'."""
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
    with zipfile.ZipFile(io.BytesIO(content)) as zf:
        return (_read_tsv(zf, "SUBMISSION.tsv"), _read_tsv(zf, "NONDERIV_TRANS.tsv"),
                _read_tsv(zf, "REPORTINGOWNER.tsv"))


def parse_bulk_quarter(sub_rows, trans_rows, owner_rows):
    """Join SUBMISSION + NONDERIV_TRANS + REPORTINGOWNER on ACCESSION_NUMBER.

    Form 4/4A only -- Form 3 (initial position, no transaction) and Form 5
    (annual catch-up) are skipped, same "is this an actual trade" filter as
    the docstring's Table I decision.
    """
    sub_by_acc = {s["ACCESSION_NUMBER"]: s for s in sub_rows
                  if s["DOCUMENT_TYPE"] in ("4", "4/A")}
    owners_by_acc = defaultdict(list)
    for o in owner_rows:
        owners_by_acc[o["ACCESSION_NUMBER"]].append(o)

    seq = Counter()
    for t in trans_rows:
        acc = t["ACCESSION_NUMBER"]
        sub = sub_by_acc.get(acc)
        if not sub:
            continue
        owners = owners_by_acc.get(acc, [])
        n = seq[acc]
        seq[acc] += 1
        yield {
            "accession_number": acc, "trans_seq": n,
            "filed_date": _iso_date(sub["FILING_DATE"]),
            "trans_date": _iso_date(t["TRANS_DATE"]),
            "issuer_cik": sub["ISSUERCIK"], "issuer_name": sub["ISSUERNAME"],
            "ticker": sub["ISSUERTRADINGSYMBOL"] or None,
            "owner_cik": "; ".join(o["RPTOWNERCIK"] for o in owners) or None,
            "owner_name": "; ".join(o["RPTOWNERNAME"] for o in owners) or None,
            "owner_relationship": "; ".join(o["RPTOWNER_RELATIONSHIP"] for o in owners
                                             if o["RPTOWNER_RELATIONSHIP"]) or None,
            "security_title": t["SECURITY_TITLE"],
            "trans_code": t["TRANS_CODE"] or None,
            "acquired_disposed": t["TRANS_ACQUIRED_DISP_CD"] or None,
            "shares": _num(t["TRANS_SHARES"]),
            "price_per_share": _num(t["TRANS_PRICEPERSHARE"]),
            "shares_owned_following": _num(t["SHRS_OWND_FOLWNG_TRANS"]),
        }


def list_quarters(s):
    """{'2026q2': full_url, ...} scraped off the index page, newest last."""
    r = _get(s, INDEX_URL)
    if r is None:
        return {}
    return {f"{y}q{q}": "https://www.sec.gov" + path for path, y, q in ZIP_RE.findall(r.text)}


# ---------------------------------------------------------------- live mode

def list_daily_form4(s, date):
    """Yield full-index paths ('edgar/data/CIK/ACCESSION.txt') for every
    Form 4 / 4-A filed on `date`. A 404 means no index was published that
    day (weekend/holiday) -- a legitimate answer, not a failure, same as
    scrape_pageviews.py's 404 handling."""
    url = (f"https://www.sec.gov/Archives/edgar/daily-index/{date.year}/"
           f"QTR{(date.month - 1) // 3 + 1}/form.{date:%Y%m%d}.idx")
    r = _get(s, url, not_found=(403, 404))
    if r is None:
        return
    for line in r.text.splitlines():
        parts = line.split()
        if len(parts) >= 2 and parts[0] in ("4", "4/A") and parts[-1].endswith(".txt"):
            yield parts[-1]


def accession_from_path(path):
    return path.rsplit("/", 1)[-1].removesuffix(".txt")


def extract_xml(txt):
    """Pull the primary ownershipDocument XML out of EDGAR's SGML-wrapped
    full submission text. Non-greedy match anchored on the closing tag finds
    the first (always the primary, per-EDGAR-convention SEQUENCE 1) XML
    document, even if the filing bundles other XML/exhibits after it."""
    m = XML_BLOCK_RE.search(txt)
    return m.group(1) if m else None


def _tag_value(el, path):
    """Most ownership XML leaves wrap their text in a nested <value>; read
    that. transactionCode is the one exception (see parse_form4_xml)."""
    child = el.find(path)
    if child is None:
        return None
    v = child.find("value")
    return v.text.strip() if v is not None and v.text else None


def parse_form4_xml(xml_text, filed_date):
    """One filing's ownershipDocument XML -> list of Table I transaction
    rows (trans_seq 0, 1, 2... in document order)."""
    root = ET.fromstring(xml_text)
    issuer = root.find("issuer")
    issuer_cik = issuer_name = ticker = None
    if issuer is not None:
        issuer_cik = (issuer.findtext("issuerCik") or "").strip() or None
        issuer_name = (issuer.findtext("issuerName") or "").strip() or None
        ticker = (issuer.findtext("issuerTradingSymbol") or "").strip() or None

    names, ciks, rels = [], [], []
    for o in root.findall("reportingOwner"):
        oid = o.find("reportingOwnerId")
        if oid is not None:
            names.append((oid.findtext("rptOwnerName") or "").strip())
            ciks.append((oid.findtext("rptOwnerCik") or "").strip())
        rel = o.find("reportingOwnerRelationship")
        roles = []
        if rel is not None:
            if rel.findtext("isDirector") == "1":
                roles.append("DIRECTOR")
            if rel.findtext("isOfficer") == "1":
                roles.append("OFFICER")
            if rel.findtext("isTenPercentOwner") == "1":
                roles.append("TENPERCENTOWNER")
            if rel.findtext("isOther") == "1":
                roles.append("OTHER")
        if roles:
            rels.append(",".join(roles))
    owner_name = "; ".join(n for n in names if n) or None
    owner_cik = "; ".join(c for c in ciks if c) or None
    owner_relationship = "; ".join(rels) or None

    rows = []
    table = root.find("nonDerivativeTable")
    if table is None:
        return rows
    for i, tx in enumerate(table.findall("nonDerivativeTransaction")):
        code_el = tx.find("transactionCoding/transactionCode")
        rows.append({
            "trans_seq": i, "filed_date": filed_date,
            "trans_date": _tag_value(tx, "transactionDate"),
            "issuer_cik": issuer_cik, "issuer_name": issuer_name, "ticker": ticker,
            "owner_cik": owner_cik, "owner_name": owner_name,
            "owner_relationship": owner_relationship,
            "security_title": _tag_value(tx, "securityTitle"),
            "trans_code": code_el.text.strip() if code_el is not None and code_el.text else None,
            "acquired_disposed": _tag_value(tx, "transactionAmounts/transactionAcquiredDisposedCode"),
            "shares": _num(_tag_value(tx, "transactionAmounts/transactionShares")),
            "price_per_share": _num(_tag_value(tx, "transactionAmounts/transactionPricePerShare")),
            "shares_owned_following": _num(
                _tag_value(tx, "postTransactionAmounts/sharesOwnedFollowingTransaction")),
        })
    return rows


# --------------------------------------------------------------- networking

def _session():
    s = requests.Session()
    s.headers["User-Agent"] = "quantgress/0.1 (mmulajkar@gmail.com)"  # sec.gov 403s without a contact email
    return s


def _get(s, url, tries=4, not_found=(404,)):
    """GET with retry -- same shape as every other phase's _get. A 404 is a
    legitimate answer here too (no daily index on a non-trading day), so it
    short-circuits without burning a retry, same as scrape_pageviews.py.

    `not_found` widens that set for one call site: today's not-yet-published
    daily index 403s rather than 404ing (confirmed live -- S3-style "access
    denied" for a key that doesn't exist yet, not a real auth failure)."""
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
        if r.status_code in not_found:
            return None
        if attempt == tries - 1:
            r.raise_for_status()
        time.sleep(5 * (attempt + 1))


def fetch_filing(s, path):
    r = _get(s, "https://www.sec.gov/Archives/" + path)
    return r.text if r else None


# --------------------------------------------------------------------- main

def _insert_rows(con, done, rows):
    """Insert whichever of `rows` aren't already in `done`. Returns the rows
    actually added -- not just a count -- since a rerun can skip some rows in
    a filing and add others, so "added" isn't reliably a prefix of `rows`."""
    added, skipped = [], 0
    insert_sql = f"INSERT INTO {TABLE} ({','.join(COLUMNS)}) VALUES ({','.join('?' * len(COLUMNS))})"
    for row in rows:
        key = (row["accession_number"], row["trans_seq"])
        if key in done:
            skipped += 1
            continue
        con.execute(insert_sql, [row[c] for c in COLUMNS])
        done.add(key)
        added.append(row)
    return added, skipped


def main_bulk(quarter=None, limit=None):
    con = duckdb.connect(DB_PATH)
    ensure_table(con)
    done = {(r[0], r[1]) for r in con.execute(
        f"SELECT accession_number, trans_seq FROM {TABLE}").fetchall()}

    s = _session()
    quarters = list_quarters(s)
    if not quarters:
        sys.exit("could not read the quarterly data set index page")
    quarter = quarter or max(quarters)
    url = quarters.get(quarter)
    if not url:
        sys.exit(f"unknown quarter {quarter!r}; have {min(quarters)}..{max(quarters)}")

    print(f"{quarter}: downloading {url}")
    r = _get(s, url)
    sub, trans, owner = read_quarter_zip(r.content)
    print(f"  {len(sub)} submissions, {len(trans)} Table I transactions, {len(owner)} owner rows")

    added = skipped = 0
    insert_sql = f"INSERT INTO {TABLE} ({','.join(COLUMNS)}) VALUES ({','.join('?' * len(COLUMNS))})"
    for row in parse_bulk_quarter(sub, trans, owner):
        if limit is not None and added >= limit:
            break
        key = (row["accession_number"], row["trans_seq"])
        if key in done:
            skipped += 1
            continue
        con.execute(insert_sql, [row[c] for c in COLUMNS])
        done.add(key)
        added += 1
        if added % 2000 == 0:
            print(f"  ...{added} added so far")

    total = con.execute(f"SELECT count(*) FROM {TABLE}").fetchone()[0]
    print(f"\n{total} insider transactions in {DB_PATH}; this run added {added},"
          f" skipped {skipped} already-stored")


def main_live(days=DAYS_DEFAULT, limit=None):
    con = duckdb.connect(DB_PATH)
    ensure_table(con)
    done = {(r[0], r[1]) for r in con.execute(
        f"SELECT accession_number, trans_seq FROM {TABLE}").fetchall()}

    s = _session()
    added = skipped = 0
    today = datetime.date.today()
    for delta in range(days):
        if limit is not None and added >= limit:
            break
        date = today - datetime.timedelta(days=delta)
        filings = list(list_daily_form4(s, date))
        print(f"{date}: {len(filings)} Form 4 filings")
        for path in filings:
            if limit is not None and added >= limit:
                break
            time.sleep(RATE_LIMIT_SECS)
            txt = fetch_filing(s, path)
            if not txt:
                continue
            xml = extract_xml(txt)
            if not xml:
                continue
            acc = accession_from_path(path)
            rows = parse_form4_xml(xml, date.isoformat())
            for row in rows:
                row["accession_number"] = acc
            new_rows, sk = _insert_rows(con, done, rows)
            added += len(new_rows)
            skipped += sk
            for row in new_rows:
                print(f"  {row['ticker'] or row['issuer_name']:<8} {row['trans_code']} "
                      f"{row['acquired_disposed']} {row['shares']} @ {row['price_per_share']}"
                      f"  {row['owner_name']}")

    total = con.execute(f"SELECT count(*) FROM {TABLE}").fetchone()[0]
    print(f"\n{total} insider transactions in {DB_PATH}; this run added {added},"
          f" skipped {skipped} already-stored")


SAMPLE_FORM4_XML = """<?xml version="1.0"?>
<ownershipDocument>
    <issuer>
        <issuerCik>0000002488</issuerCik>
        <issuerName>ADVANCED MICRO DEVICES INC</issuerName>
        <issuerTradingSymbol>AMD</issuerTradingSymbol>
    </issuer>
    <reportingOwner>
        <reportingOwnerId>
            <rptOwnerCik>0001267376</rptOwnerCik>
            <rptOwnerName>Hahn Ava</rptOwnerName>
        </reportingOwnerId>
        <reportingOwnerRelationship>
            <isDirector>0</isDirector>
            <isOfficer>1</isOfficer>
            <isTenPercentOwner>0</isTenPercentOwner>
            <isOther>0</isOther>
        </reportingOwnerRelationship>
    </reportingOwner>
    <nonDerivativeTable>
        <nonDerivativeTransaction>
            <securityTitle><value>Common Stock</value></securityTitle>
            <transactionDate><value>2026-08-11</value></transactionDate>
            <transactionCoding>
                <transactionCode>S</transactionCode>
            </transactionCoding>
            <transactionAmounts>
                <transactionShares><value>143</value></transactionShares>
                <transactionPricePerShare><value>474.75</value></transactionPricePerShare>
                <transactionAcquiredDisposedCode><value>D</value></transactionAcquiredDisposedCode>
            </transactionAmounts>
            <postTransactionAmounts>
                <sharesOwnedFollowingTransaction><value>17644</value></sharesOwnedFollowingTransaction>
            </postTransactionAmounts>
        </nonDerivativeTransaction>
    </nonDerivativeTable>
    <derivativeTable></derivativeTable>
</ownershipDocument>
"""


def selftest():
    # --- bulk join ---
    sub_rows = [
        {"ACCESSION_NUMBER": "A1", "FILING_DATE": "31-OCT-2025", "DOCUMENT_TYPE": "4",
         "ISSUERCIK": "0000002488", "ISSUERNAME": "ADVANCED MICRO DEVICES INC",
         "ISSUERTRADINGSYMBOL": "AMD"},
        {"ACCESSION_NUMBER": "A2", "FILING_DATE": "01-NOV-2025", "DOCUMENT_TYPE": "3",  # Form 3 -- skipped
         "ISSUERCIK": "0000320193", "ISSUERNAME": "APPLE INC", "ISSUERTRADINGSYMBOL": "AAPL"},
    ]
    trans_rows = [
        {"ACCESSION_NUMBER": "A1", "SECURITY_TITLE": "Common Stock", "TRANS_DATE": "29-OCT-2025",
         "TRANS_CODE": "A", "TRANS_ACQUIRED_DISP_CD": "A", "TRANS_SHARES": "100",
         "TRANS_PRICEPERSHARE": "", "SHRS_OWND_FOLWNG_TRANS": "1100"},
        {"ACCESSION_NUMBER": "A1", "SECURITY_TITLE": "Common Stock", "TRANS_DATE": "31-OCT-2025",
         "TRANS_CODE": "S", "TRANS_ACQUIRED_DISP_CD": "D", "TRANS_SHARES": "50",
         "TRANS_PRICEPERSHARE": "180.5", "SHRS_OWND_FOLWNG_TRANS": "1050"},
        {"ACCESSION_NUMBER": "A2", "SECURITY_TITLE": "Common Stock", "TRANS_DATE": "01-NOV-2025",
         "TRANS_CODE": "", "TRANS_ACQUIRED_DISP_CD": "A", "TRANS_SHARES": "10",
         "TRANS_PRICEPERSHARE": "", "SHRS_OWND_FOLWNG_TRANS": "10"},  # under a Form 3 -- dropped
    ]
    owner_rows = [{"ACCESSION_NUMBER": "A1", "RPTOWNERCIK": "0001267376",
                   "RPTOWNERNAME": "Hahn Ava", "RPTOWNER_RELATIONSHIP": "OFFICER"}]
    rows = list(parse_bulk_quarter(sub_rows, trans_rows, owner_rows))
    assert len(rows) == 2, "Form 3 row must be dropped"
    assert [r["trans_seq"] for r in rows] == [0, 1]
    assert rows[0]["ticker"] == "AMD" and rows[0]["filed_date"] == "2025-10-31"
    assert rows[0]["trans_date"] == "2025-10-29" and rows[0]["shares"] == 100.0
    assert rows[0]["price_per_share"] is None, "blank price must be None, not 0.0 or ''"
    assert rows[0]["owner_name"] == "Hahn Ava" and rows[0]["owner_relationship"] == "OFFICER"
    assert rows[1]["trans_code"] == "S" and rows[1]["price_per_share"] == 180.5

    assert _iso_date("31-OCT-2025") == "2025-10-31"
    assert _iso_date("") is None and _iso_date(None) is None

    # --- list_quarters URL discovery, both path-prefix variants ---
    sample_html = (
        'a href="/files/datastandardsinnovation/data/insider-transactions-data-sets/2026q2_form345.zip"'
        'a href="/files/structureddata/data/insider-transactions-data-sets/2025q4_form345.zip"'
    )

    class _FakeIndexResp:
        text = sample_html
        status_code = 200

    class _FakeIndexSession:
        def get(self, *a, **k):
            return _FakeIndexResp()

    quarters = list_quarters(_FakeIndexSession())
    assert quarters["2026q2"].endswith("/datastandardsinnovation/data/insider-transactions-data-sets/2026q2_form345.zip")
    assert quarters["2025q4"].endswith("/structureddata/data/insider-transactions-data-sets/2025q4_form345.zip")
    assert max(quarters) == "2026q2", "lexical sort of 'YYYYqN' must still pick the newest quarter"

    # --- live XML parsing ---
    xml = extract_xml(f"<TYPE>4\n<XML>\n{SAMPLE_FORM4_XML}</XML>\n</TEXT>")
    assert xml is not None and xml.strip().startswith("<?xml")
    rows = parse_form4_xml(xml, "2026-08-13")
    assert len(rows) == 1
    r = rows[0]
    assert r["ticker"] == "AMD" and r["issuer_cik"] == "0000002488"
    assert r["owner_name"] == "Hahn Ava" and r["owner_relationship"] == "OFFICER"
    assert r["trans_code"] == "S" and r["acquired_disposed"] == "D"
    assert r["shares"] == 143.0 and r["price_per_share"] == 474.75
    assert r["shares_owned_following"] == 17644.0
    assert r["filed_date"] == "2026-08-13" and r["trans_date"] == "2026-08-11"

    # --- daily index line parsing ---
    idx_text = (
        "Form Type   Company Name                          CIK\n"
        "---------------------------------------------------------------\n"
        "4                Advanced Micro Devices Inc            2488     20260813    edgar/data/2488/0000002488-26-000141.txt\n"
        "4/A              Some Amender Inc                      9999     20260813    edgar/data/9999/0000009999-26-000001.txt\n"
        "3                Not A Transaction Inc                 1111     20260813    edgar/data/1111/0000001111-26-000002.txt\n"
    )

    class _FakeDailyResp:
        text = idx_text
        status_code = 200

    class _FakeDailySession:
        def get(self, *a, **k):
            return _FakeDailyResp()

    paths = list(list_daily_form4(_FakeDailySession(), datetime.date(2026, 8, 13)))
    assert paths == ["edgar/data/2488/0000002488-26-000141.txt",
                      "edgar/data/9999/0000009999-26-000001.txt"], paths
    assert accession_from_path(paths[0]) == "0000002488-26-000141"

    # a 404 daily index (weekend/holiday) must come back as no filings, not raise
    class _NotFound:
        def get(self, *a, **k):
            return type("R", (), {"status_code": 404})()

    real_sleep, time.sleep = time.sleep, lambda _: None
    try:
        assert list(list_daily_form4(_NotFound(), datetime.date(2026, 8, 15))) == []
    finally:
        time.sleep = real_sleep

    # today's not-yet-published index 403s instead of 404ing -- confirmed
    # live -- and list_daily_form4 must treat that as "no data" too
    def _raise(self):
        raise requests.exceptions.HTTPError("403")

    class _Forbidden:
        def get(self, *a, **k):
            return type("R", (), {"status_code": 403, "raise_for_status": _raise})()

    try:
        assert list(list_daily_form4(_Forbidden(), datetime.date(2026, 8, 14))) == []
    finally:
        time.sleep = real_sleep

    # but a plain _get call (not the daily-index path) must still treat a
    # 403 as a real error, not silently swallow it -- widening not_found is
    # opt-in per call site, not a global change to auth-failure behavior
    try:
        _get(_Forbidden(), "http://example.invalid", tries=1)
    except requests.exceptions.HTTPError:
        pass
    else:
        raise AssertionError("plain _get must not swallow a 403")
    finally:
        time.sleep = real_sleep

    # _get retries a connection-level failure, and does not retry a 404 --
    # same contract as scrape_pageviews.py
    class _FlakyThenOK:
        calls = 0

        def get(self, *a, **k):
            _FlakyThenOK.calls += 1
            if _FlakyThenOK.calls < 3:
                raise requests.exceptions.ConnectionError("simulated DNS blip")
            return type("R", (), {"status_code": 200})()

    try:
        r = _get(_FlakyThenOK(), "http://example.invalid", tries=4)
    finally:
        time.sleep = real_sleep
    assert r.status_code == 200 and _FlakyThenOK.calls == 3

    print("selftest ok")


def arg(flag, default=None):
    return sys.argv[sys.argv.index(flag) + 1] if flag in sys.argv else default


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        n = arg("--limit")
        lim = int(n) if n else None
        if "--live" in sys.argv:
            main_live(int(arg("--days", DAYS_DEFAULT)), lim)
        else:
            main_bulk(arg("--quarter"), lim)
