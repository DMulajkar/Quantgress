"""Scrape granted patents from the USPTO Open Data Portal (ODP) into DuckDB.

Phase 12 of Quantgress: the first dataset in this repo with no clean key at
all -- a patent's `assignee_name` is free text on the grant record, same as
lobbying's `client_name` and contracts' `recipient_name`, so this is what
Phase 8's `entities.py` (sec_name adapter) was generalized for.

Usage:
    py scrape_patents.py --selftest              # offline checks, no network
    py scrape_patents.py --limit 20                # bounded run, last 10 days
    py scrape_patents.py --days 30                 # last 30 calendar days
    py scrape_patents.py --start 2026-01-01 --end 2026-01-31 --limit 500
    py scrape_patents.py                           # last 10 days, unbounded

Re-running skips `application_number`s already stored, so an interrupted run
resumes -- same pattern as every other date-range scraper here.

# Confirmed live 2026-08-16 (full-scrape run on the Oracle server): the
# request shape and query syntax (`q=field:value`) were right, but deep
# pagination on a busy grant day 413s -- see the 404/413 handling in _get.

# ponytail: unlike every prior data.gov-adjacent source (LDA, USAspending,
# SEC), this one gates on a real API key, not just a User-Agent string --
# free MyUSPTO account + ID.me identity verification, then an `X-API-Key`
# header. Set USPTO_API_KEY in the environment before running; --selftest
# needs no key since it never touches the network.
"""

import datetime
import os
import sys
import time

import duckdb
import requests

from schema import DB_PATH

API = "https://api.uspto.gov/api/v1/patent/applications/search"
RATE_LIMIT_SECS = 0.3  # no documented limit found; same courtesy default as Phase 7/9
PAGE_SIZE = 100  # unconfirmed live -- ODP's own default page size is 25; 100 is a guess pending a real run
# ponytail: 10, not Phase 7's 7 -- patents are only granted on Tuesdays (USPTO's
# weekly grant day), so a 7-day window can straddle exactly one grant day with
# no slack if a run slips a day or two; 10 always covers at least one Tuesday.
DAYS_DEFAULT = 10

TABLE = "patents"
COLUMNS = ["application_number", "patent_number", "invention_title",
           "filing_date", "grant_date", "assignee_name", "assignee_source"]


def ensure_table(con):
    con.execute(f"""CREATE TABLE IF NOT EXISTS {TABLE} (
        application_number VARCHAR PRIMARY KEY, patent_number VARCHAR,
        invention_title VARCHAR, filing_date VARCHAR, grant_date VARCHAR,
        assignee_name VARCHAR, assignee_source VARCHAR)""")


def pick_assignee(item):
    """Prefer the recorded assignment's assignee (a real ownership transfer)
    over the first applicant's name (who may just be the inventor, not the
    entity that actually owns the patent) -- same trust ordering as Phase 8's
    extract-before-sec_name ranking: prefer the more direct signal."""
    for assignment in item.get("assignmentBag") or []:
        for assignee in assignment.get("assigneeBag") or []:
            name = assignee.get("assigneeNameText")
            if name:
                return name, "assignment"
    meta = item.get("applicationMetaData") or {}
    name = meta.get("firstApplicantName")
    if name:
        return name, "applicant_fallback"
    return None, None


def parse_application(item):
    meta = item.get("applicationMetaData") or {}
    assignee, how = pick_assignee(item)
    return {
        "application_number": item.get("applicationNumberText"),
        "patent_number": meta.get("patentNumber"),
        "invention_title": meta.get("inventionTitle"),
        "filing_date": meta.get("filingDate"),
        "grant_date": meta.get("grantDate"),
        "assignee_name": assignee,
        "assignee_source": how,
    }


def _load_dotenv():
    # ponytail: stdlib KEY=value parser, not python-dotenv -- one key in one
    # file doesn't earn a new dependency. Doesn't override a real env var.
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
    key = os.environ.get("USPTO_API_KEY")
    if not key:
        sys.exit("USPTO_API_KEY not set -- sign up for a free MyUSPTO account "
                  "(requires ID.me verification), put it in .env as "
                  "USPTO_API_KEY=<key>, or export it before running.")
    s = requests.Session()
    s.headers["X-API-Key"] = key
    s.headers["User-Agent"] = "quantgress/0.1 (personal research)"
    return s


def _get(s, params, tries=4):
    """GET with retry -- mirrors scrape_contracts._post: retries both a bad
    status code and a connection-level failure.

    # ponytail: found live, not from docs -- a zero-result query 404s here
    # ({"code":"404",...,"detailedMessage":"No matching records found..."}),
    # not an empty 200 like the docstring below originally assumed. Same
    # "404 is a legitimate answer, not a failure" shape as
    # scrape_pageviews._get -- returns None immediately, no retry burned.
    """
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
        if r.status_code in (404, 413):
            # 404: no matching records for this query (legit, see above).
            # 413: found live, not in USPTO's docs -- ODP's search index
            # apparently caps how deep one query can paginate; hit at
            # offset=1400 on a single (heavy) grant day. Same "stop, don't
            # fail" treatment as 404 -- missing the tail past the cap beats
            # crashing the whole multi-day run over one busy Tuesday.
            return None
        if attempt == tries - 1:
            r.raise_for_status()
        time.sleep(5 * (attempt + 1))


def list_applications(s, day):
    """Yield one application result dict at a time for patents granted on
    `day` (a date object). Most days are empty -- patents are only granted on
    Tuesdays -- a 404 ("no matching records") is normal on those, not an
    error."""
    offset = 0
    while True:
        time.sleep(RATE_LIMIT_SECS)
        params = {"q": f"applicationMetaData.grantDate:{day.isoformat()}",
                  "offset": offset, "limit": PAGE_SIZE}
        r = _get(s, params)
        if r is None:
            return
        data = r.json()
        results = data.get("patentFileWrapperDataBag", [])
        yield from results
        offset += len(results)
        if not results or offset >= data.get("count", offset):
            break


def daterange(start, end):
    d = start
    while d <= end:
        yield d
        d += datetime.timedelta(days=1)


def main(start, end, limit=None):
    con = duckdb.connect(DB_PATH)
    ensure_table(con)
    done = {r[0] for r in con.execute(f"SELECT application_number FROM {TABLE}").fetchall()}

    s = _session()
    added = skipped = 0
    insert_sql = (f"INSERT INTO {TABLE} ({','.join(COLUMNS)}) "
                  f"VALUES ({','.join('?' * len(COLUMNS))})")
    print(f"{start} to {end}:")
    for day in daterange(start, end):
        if limit is not None and added >= limit:
            break
        day_added = day_skipped = 0
        for item in list_applications(s, day):
            if limit is not None and added >= limit:
                break
            row = parse_application(item)
            if not row["application_number"] or row["application_number"] in done:
                day_skipped += 1
                continue
            con.execute(insert_sql, [row[c] for c in COLUMNS])
            done.add(row["application_number"])
            added += 1
            day_added += 1
        if day_added or day_skipped:
            print(f"  {day.isoformat()}: {day_added} new, {day_skipped} already stored")

    total = con.execute(f"SELECT count(*) FROM {TABLE}").fetchone()[0]
    print(f"\n{total} patents in {DB_PATH}; this run added {added},"
          f" skipped {skipped} already-stored")


def selftest():
    sample = {
        "applicationNumberText": "16123456",
        "applicationMetaData": {
            "patentNumber": "11123456", "inventionTitle": "WIDGET WITH FLANGE",
            "filingDate": "2022-03-01", "grantDate": "2026-08-11",
            "firstApplicantName": "Jane Q. Inventor",
        },
        "assignmentBag": [{"assigneeBag": [{"assigneeNameText": "ACME WIDGET CORP"}]}],
    }
    row = parse_application(sample)
    assert row["application_number"] == "16123456"
    assert row["patent_number"] == "11123456"
    assert row["invention_title"] == "WIDGET WITH FLANGE"
    assert row["grant_date"] == "2026-08-11"
    # a recorded assignment wins over the first applicant's own name
    assert row["assignee_name"] == "ACME WIDGET CORP" and row["assignee_source"] == "assignment"

    # no assignment recorded yet -- falls back to the first applicant
    no_assignment = dict(sample, assignmentBag=[])
    row2 = parse_application(no_assignment)
    assert row2["assignee_name"] == "Jane Q. Inventor" and row2["assignee_source"] == "applicant_fallback"

    # neither present -- genuinely nothing to resolve, not a parse failure
    bare = {"applicationNumberText": "16999999", "applicationMetaData": {}}
    row3 = parse_application(bare)
    assert row3["assignee_name"] is None and row3["assignee_source"] is None

    # an assignment record with an empty assigneeBag doesn't crash the walk
    empty_bag = dict(sample, assignmentBag=[{"assigneeBag": []}])
    row4 = parse_application(empty_bag)
    assert row4["assignee_name"] == "Jane Q. Inventor", "falls through to applicant, not None"

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

    # list_applications stops paginating once offset reaches the reported count
    class _TwoPages:
        calls = 0

        def get(self, url, params, timeout):
            _TwoPages.calls += 1
            if params["offset"] == 0:
                body = {"patentFileWrapperDataBag": [dict(sample, applicationNumberText="A")] * 2,
                        "count": 3}
            else:
                body = {"patentFileWrapperDataBag": [dict(sample, applicationNumberText="B")],
                        "count": 3}
            return type("R", (), {"status_code": 200, "json": staticmethod(lambda: body)})()

    try:
        results = list(list_applications(_TwoPages(), datetime.date(2026, 8, 11)))
    finally:
        time.sleep = real_sleep
    assert len(results) == 3 and _TwoPages.calls == 2

    # a 404 ("no matching records") is a legitimate answer, not a failure --
    # _get returns None immediately (no retry burned), list_applications
    # yields nothing and doesn't crash
    class _NoResults:
        calls = 0

        def get(self, *a, **k):
            _NoResults.calls += 1
            return type("R", (), {"status_code": 404})()

    results = list(list_applications(_NoResults(), datetime.date(2026, 1, 1)))
    assert results == [] and _NoResults.calls == 1

    # 413 ("query paginated too deep", found live 2026-08-16 at offset=1400)
    # gets the same no-retry stop as 404, not a crash
    class _TooDeep:
        calls = 0

        def get(self, *a, **k):
            _TooDeep.calls += 1
            return type("R", (), {"status_code": 413})()

    results = list(list_applications(_TooDeep(), datetime.date(2026, 8, 11)))
    assert results == [] and _TooDeep.calls == 1

    print("selftest ok")


def arg(flag, default=None):
    return sys.argv[sys.argv.index(flag) + 1] if flag in sys.argv else default


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        today = datetime.date.today()
        days = int(arg("--days", DAYS_DEFAULT))
        start = datetime.date.fromisoformat(arg("--start", (today - datetime.timedelta(days=days)).isoformat()))
        end = datetime.date.fromisoformat(arg("--end", today.isoformat()))
        n = arg("--limit")
        main(start, end, int(n) if n else None)
