"""Scrape corporate PAC/committee donations from the OpenFEC API into DuckDB.

Phase 13 of Quantgress: closes the political-money loop opened in Phase 6
(scrape_lobbying.py) -- donations in, lobbying out, congress trades
alongside. Same difficulty class as Phase 12 (scrape_patents.py): the donor
is a free-text name (`contributor_name`) with no ticker field, so it's
another `entities.py` sec_name adapter, not a lookup table.

Endpoint: /v1/schedules/schedule_a/, filtered to `contributor_type=committee`
-- Schedule A itemized receipts where the contributor is a committee/PAC/
organization rather than a person. That's the "corporate donor" slice: a
company's PAC giving to a candidate or party committee, not the 67M+
individual line items FEC also carries. Committee/donor name matching, same
as the module docstring above.

# ponytail: the original build used `is_individual=false` for this filter,
# per the (wrong) assumption that it meant "contributor is not an individual."
# Confirmed live it does not filter by entity type at all -- see the
# ponytail note on list_donations() below for what it actually does and what
# the live-confirmed fix is.

Usage:
    py scrape_donors.py --selftest              # offline checks, no network
    py scrape_donors.py --limit 20                # bounded run, current cycle
    py scrape_donors.py --cycle 2024               # one two-year FEC cycle
    py scrape_donors.py --cycle 2024 --limit 500
    py scrape_donors.py                            # current cycle, unbounded

Re-running skips `sub_id`s already stored, so an interrupted run resumes --
same pattern as every other scraper here.

# ponytail: like Phase 12, this module was built with api.open.fec.gov
# unreachable from the dev environment (same network-egress block hit on
# finra.org/data.uspto.gov), so the request shape below is built from the
# public openFEC source (fecgov/openFEC on GitHub) rather than a live call.
# Unlike Phase 12's USPTO gate, though, an OpenFEC key is a free, instant
# api.data.gov signup -- no ID.me step -- so that part isn't a real gotcha,
# just an unverified one.
"""

import datetime
import os
import sys
import time

import duckdb
import requests

from schema import DB_PATH

API = "https://api.open.fec.gov/v1/schedules/schedule_a/"
RATE_LIMIT_SECS = 0.3  # no documented limit found; same courtesy default as Phase 7/9/12
PAGE_SIZE = 100  # documented max per_page for this endpoint

TABLE = "corporate_donations"
COLUMNS = ["sub_id", "contributor_name", "entity_type", "contribution_date",
           "contribution_amount", "committee_id", "committee_name", "cycle"]


def ensure_table(con):
    con.execute(f"""CREATE TABLE IF NOT EXISTS {TABLE} (
        sub_id VARCHAR PRIMARY KEY, contributor_name VARCHAR, entity_type VARCHAR,
        contribution_date VARCHAR, contribution_amount DOUBLE,
        committee_id VARCHAR, committee_name VARCHAR, cycle INTEGER)""")


def ensure_agg_view(con):
    """Paid-tier-safe shape: company/PAC totals only, never a raw row.

    52 U.S.C. Sec 30111(a)(4) bars commercial use of FEC-disclosed
    contributor info; Quiver Quantitative's own election-contributions page
    ships exactly this shape (ticker, total $, no contributor_name/sub_id)
    as the aggregate-only workaround -- see 03 Concepts/Quantgress API
    Monetization.md. api.py's "donors" route reads this view, not the raw
    table, so contributor_name and sub_id never leave the API.

    Self-ALTERs contributor_ticker_guess in first, same as scrape_13f.py's
    f13_positions -- entities.py may not have run yet when this is called.
    """
    con.execute(f"ALTER TABLE {TABLE} ADD COLUMN IF NOT EXISTS contributor_ticker_guess VARCHAR")
    con.execute(f"""
        CREATE OR REPLACE VIEW corporate_donations_agg AS
        SELECT contributor_ticker_guess, committee_name, cycle,
               count(*) AS num_contributions, sum(contribution_amount) AS total_amount
        FROM {TABLE}
        GROUP BY contributor_ticker_guess, committee_name, cycle
    """)


def current_cycle(today=None):
    """FEC two-year cycles are labeled by their ending (even) year -- 2025 and
    2026 are both the '2026 cycle'. Round an odd year up."""
    y = (today or datetime.date.today()).year
    return y if y % 2 == 0 else y + 1


def parse_record(item):
    committee = item.get("committee") or {}
    return {
        "sub_id": item.get("sub_id"),
        "contributor_name": item.get("contributor_name"),
        "entity_type": item.get("entity_type"),
        "contribution_date": item.get("contribution_receipt_date"),
        "contribution_amount": item.get("contribution_receipt_amount"),
        "committee_id": item.get("committee_id"),
        "committee_name": committee.get("name"),
        "cycle": item.get("two_year_transaction_period"),
    }


def _load_dotenv():
    # ponytail: same stdlib .env loader as scrape_patents.py -- copy, not a
    # shared helper, until a third phase wants one too.
    path = os.path.join(os.path.dirname(__file__), ".env")
    if not os.path.exists(path):
        return
    for line in open(path):
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip())


def _session():
    _load_dotenv()
    key = os.environ.get("FEC_API_KEY")
    if not key:
        sys.exit("FEC_API_KEY not set -- free instant signup at "
                  "https://api.data.gov/signup/, put it in .env as "
                  "FEC_API_KEY=<key>, or export it before running.")
    s = requests.Session()
    s.headers["User-Agent"] = "quantgress/0.1 (personal research)"
    return s, key


def _get(s, params, tries=4):
    """GET with retry -- mirrors scrape_contracts._post/scrape_patents._get:
    retries both a bad status code and a connection-level failure."""
    for attempt in range(tries):
        try:
            r = s.get(API, params=params, timeout=30)
        except requests.exceptions.RequestException:
            if attempt == tries - 1:
                raise
            time.sleep(5 * (attempt + 1))
            continue
        if r.status_code == 200:
            return r
        if attempt == tries - 1:
            r.raise_for_status()
        time.sleep(5 * (attempt + 1))


def list_donations(s, api_key, cycle):
    """Yield one Schedule A result dict at a time for corporate/committee
    donors in the given two-year cycle. Seek-based pagination -- the
    endpoint's own docs call out cursor pagination (not page numbers) for
    this resource, via last_index/last_contribution_receipt_date echoed back
    in the response's pagination.last_indexes.

    # ponytail: found live -- `is_individual=false` does NOT filter out
    # individual donors. Per FEC's own docs it's a de-dup/reporting flag (which
    # copy of a transaction counts toward the "total from individuals" figure,
    # since one contribution can appear multiple times across committees as an
    # earmark), not an entity-type filter. Confirmed with a live DEMO_KEY call:
    # is_individual=false still returned entity_type="IND" rows for real people
    # ("LINDHOLM, CHAD", "AREVALO, JONATHAN"). `contributor_type=committee` is
    # the actual filter -- confirmed live it returns only PAC/ORG/CCM/CAN rows,
    # zero plain IND.
    """
    last_index = last_date = None
    while True:
        time.sleep(RATE_LIMIT_SECS)
        params = {"api_key": api_key, "contributor_type": "committee",
                  "two_year_transaction_period": cycle,
                  "sort": "contribution_receipt_date", "per_page": PAGE_SIZE}
        if last_index is not None:
            params["last_index"] = last_index
            params["last_contribution_receipt_date"] = last_date
        data = _get(s, params).json()
        results = data.get("results", [])
        yield from results
        if len(results) < PAGE_SIZE:
            break
        idx = (data.get("pagination") or {}).get("last_indexes") or {}
        last_index, last_date = idx.get("last_index"), idx.get("last_contribution_receipt_date")
        if last_index is None:
            break


def main(cycle, limit=None):
    con = duckdb.connect(DB_PATH)
    ensure_table(con)
    ensure_agg_view(con)
    done = {r[0] for r in con.execute(f"SELECT sub_id FROM {TABLE}").fetchall()}

    s, api_key = _session()
    added = skipped = 0
    insert_sql = (f"INSERT INTO {TABLE} ({','.join(COLUMNS)}) "
                  f"VALUES ({','.join('?' * len(COLUMNS))})")
    print(f"cycle {cycle}:")
    for item in list_donations(s, api_key, cycle):
        if limit is not None and added >= limit:
            break
        row = parse_record(item)
        if not row["sub_id"] or row["sub_id"] in done:
            skipped += 1
            continue
        con.execute(insert_sql, [row[c] for c in COLUMNS])
        done.add(row["sub_id"])
        added += 1
        amt = f" ${row['contribution_amount']:,.0f}" if row["contribution_amount"] else ""
        print(f"  {row['contributor_name']} -> {row['committee_name']}{amt}")

    total = con.execute(f"SELECT count(*) FROM {TABLE}").fetchone()[0]
    print(f"\n{total} corporate donation rows in {DB_PATH}; this run added {added},"
          f" skipped {skipped} already-stored")


def selftest():
    assert current_cycle(datetime.date(2026, 1, 15)) == 2026
    assert current_cycle(datetime.date(2025, 1, 15)) == 2026
    assert current_cycle(datetime.date(2024, 12, 31)) == 2024

    sample = {
        "sub_id": "4041920221234567890", "contributor_name": "ACME WIDGET CORP PAC",
        "entity_type": "PAC", "contribution_receipt_date": "2026-03-14T00:00:00",
        "contribution_receipt_amount": 5000.0, "committee_id": "C00123456",
        "committee": {"name": "FRIENDS OF JANE SMITH"}, "two_year_transaction_period": 2026,
    }
    row = parse_record(sample)
    assert row["sub_id"] == "4041920221234567890"
    assert row["contributor_name"] == "ACME WIDGET CORP PAC" and row["entity_type"] == "PAC"
    assert row["committee_name"] == "FRIENDS OF JANE SMITH" and row["cycle"] == 2026
    assert row["contribution_amount"] == 5000.0

    # a record with no nested committee object (unusual, but seen elsewhere
    # in the API) must not crash, just leave committee_name None
    bare = dict(sample, committee=None)
    row2 = parse_record(bare)
    assert row2["committee_name"] is None

    # _get retries a connection-level failure, not just a bad status code
    class _FlakyThenOK:
        calls = 0

        def get(self, *a, **k):
            _FlakyThenOK.calls += 1
            if _FlakyThenOK.calls < 3:
                raise requests.exceptions.ConnectionError("simulated DNS blip")
            return type("R", (), {"status_code": 200})()

    real_sleep, time.sleep = time.sleep, lambda _: None
    try:
        r = _get(_FlakyThenOK(), {}, tries=4)
    finally:
        time.sleep = real_sleep
    assert r.status_code == 200 and _FlakyThenOK.calls == 3

    # list_donations follows the last_indexes cursor across pages and stops
    # once a page comes back short of PAGE_SIZE
    class _TwoPages:
        calls = 0

        def get(self, url, params, timeout):
            _TwoPages.calls += 1
            if "last_index" not in params:
                body = {"results": [dict(sample, sub_id=str(i)) for i in range(PAGE_SIZE)],
                        "pagination": {"last_indexes": {"last_index": "99",
                                                         "last_contribution_receipt_date": "2026-03-01"}}}
            else:
                assert params["last_index"] == "99"
                body = {"results": [dict(sample, sub_id="last")], "pagination": {}}
            return type("R", (), {"status_code": 200, "json": staticmethod(lambda: body)})()

    try:
        results = list(list_donations(_TwoPages(), "DEMO_KEY", 2026))
    finally:
        time.sleep = real_sleep
    assert len(results) == PAGE_SIZE + 1 and _TwoPages.calls == 2

    # corporate_donations_agg (the "donors" route's actual source, see
    # ensure_agg_view) never exposes contributor_name/sub_id, and its totals
    # match the raw rows it's grouping
    mem = duckdb.connect(":memory:")
    ensure_table(mem)
    ensure_agg_view(mem)
    ins = (f"INSERT INTO {TABLE} (sub_id, contributor_name, contribution_amount, "
           f"committee_name, cycle, contributor_ticker_guess) VALUES (?,?,?,?,?,?)")
    mem.execute(ins, ["1", "ACME WIDGET CORP PAC", 1000.0, "FRIENDS OF JANE SMITH", 2026, "ACME"])
    mem.execute(ins, ["2", "ACME WIDGET CORP PAC", 500.0, "FRIENDS OF JANE SMITH", 2026, "ACME"])
    ensure_agg_view(mem)  # re-running after inserts must still just replace, not duplicate
    cols = [d[0] for d in mem.execute("SELECT * FROM corporate_donations_agg").description]
    assert "contributor_name" not in cols and "sub_id" not in cols
    agg = mem.execute("SELECT total_amount, num_contributions FROM corporate_donations_agg "
                       "WHERE contributor_ticker_guess = 'ACME'").fetchone()
    assert agg == (1500.0, 2)

    print("selftest ok")


def arg(flag, default=None):
    return sys.argv[sys.argv.index(flag) + 1] if flag in sys.argv else default


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        n = arg("--limit")
        main(int(arg("--cycle", current_cycle())), int(n) if n else None)
