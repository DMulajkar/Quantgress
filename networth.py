"""Phase 15 of Quantgress: politician net worth, derived from data already in
congress_trades.duckdb -- no new scrape, no new table, no scraper con.close()
gotcha to add (read-only, nothing written).

Quiver's own description: net worth per member, computed from their
disclosed portfolio marked to live stock prices, updated hourly; excludes
primary residence and non-equity liabilities, so it's a floor estimate, not
a real net-worth figure. This is a floor on a floor: amount_low is already
the bottom of a disclosed dollar bracket (see [[Quantgress]]), and this just
scales that by a price ratio -- STOCK Act disclosures never give a share
count, so there is no exact figure to compute here or at Quiver.

Position estimate: sum amount_low, +Purchase / -Sale (Full|Partial), per
(chamber, last_name, tkr). Exchange rows are corporate actions, not
discretionary trades -- excluded, same as every other buy/sell signal in
this project. A net-positive sum is treated as "still plausibly held";
net-zero-or-negative is floored at 0, not carried as a short position
there's no evidence for.

Mark-to-market: amount_low was disclosed as of the position's last
transaction date, not today. Scale it by (current close / close on that
date), via Yahoo Finance's public chart endpoint -- the same one `yfinance`
wraps, hit directly here since `requests` (already a dependency) is enough
for one JSON endpoint; no new dependency earns its keep for that. One
request per distinct ticker prices every politician who holds it, not one
request per position.

Party is not in any Quantgress table (the disclosure forms don't carry it) --
pulled fresh each run from the unitedstates/congress-legislators project's
public YAML (no key, no scrape, the standard free reference for this), and
joined on (chamber, normalized last_name). Current-members-only file, so a
politician who's left office since it was last updated shows "Unknown".

Usage:
    py networth.py --selftest         # offline checks, no network
    py networth.py --limit 20         # bounded run, price 20 tickers only
    py networth.py                    # full run, every net-positive ticker
    py networth.py --member Pelosi    # one politician's ticker breakdown

# ponytail: EOD/last-close prices, not intraday -- Quiver's "hourly" needs a
# streaming quote endpoint, a different (and more rate-limited) Yahoo path.
# Upgrade if a live dashboard ever needs same-day price moves.
"""

import datetime
import re
import sys
import time

import duckdb
import pandas as pd
import requests
import yaml

from schema import DB_PATH

CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?range=5y&interval=1d"
RATE_LIMIT_SECS = 0.3  # no documented Yahoo limit; courtesy delay, same family as Phase 11/14

PARTY_BASE = "https://raw.githubusercontent.com/unitedstates/congress-legislators/main/{}"
# trades.chamber -> the legislators file's term "type" for that chamber
CHAMBER_TERM_TYPE = {"S": "sen", "H": "rep"}

POSITIONS_SQL = """
SELECT chamber, last_name, tkr,
       sum(CASE WHEN tx_type = 'Purchase' THEN amount_low
                WHEN tx_type IN ('Sale (Full)', 'Sale (Partial)') THEN -amount_low
                ELSE 0 END) AS net_invested,
       max(txn_date) AS basis_date
FROM trades
WHERE tkr IS NOT NULL AND tx_type != 'Exchange'
GROUP BY chamber, last_name, tkr
HAVING net_invested > 0
"""


def _session():
    s = requests.Session()
    # Yahoo's edge rejects requests with no browser-like UA -- confirmed live,
    # the generic descriptive UA that's fine for SEC/LDA/USAspending instead
    # gets a plain-text "Edge: Too Many Requests" body, not JSON.
    s.headers["User-Agent"] = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36")
    return s


def parse_chart(data):
    """Yahoo's chart JSON -> (current_price, [(date, close), ...] sorted).

    A delisted/unknown symbol comes back as HTTP 200 with `result: null` and
    an `error` block -- unlike every other API this project has hit, this
    one does NOT use a 404 to mean "no data." Returns (None, []) for that.
    """
    result = (data.get("chart") or {}).get("result")
    if not result:
        return None, []
    r = result[0]
    current = r.get("meta", {}).get("regularMarketPrice")
    timestamps = r.get("timestamp") or []
    closes = r.get("indicators", {}).get("quote", [{}])[0].get("close") or []
    series = sorted((datetime.date.fromtimestamp(ts), c)
                     for ts, c in zip(timestamps, closes) if c is not None)
    return current, series


def nearest_price(series, target_date):
    """Close price on or before target_date. Falls back to the earliest
    available close if target_date predates the series, or to the latest
    if target_date is after it (observed live: a bad-data future txn_date,
    2026-12-26, sorts past every real close in the fetched series)."""
    if not series:
        return None
    on_or_before = [c for d, c in series if d <= target_date]
    return on_or_before[-1] if on_or_before else series[0][1]


def _get(s, url, tries=4):
    for attempt in range(tries):
        try:
            r = s.get(url, timeout=15)
            return r.json()
        except (requests.exceptions.RequestException, ValueError):
            if attempt == tries - 1:
                return {}
            time.sleep(3 * (attempt + 1))


def fetch_prices(s, symbol):
    time.sleep(RATE_LIMIT_SECS)
    return parse_chart(_get(s, CHART_URL.format(symbol=symbol)))


def _to_date(x):
    return x.date() if hasattr(x, "date") else x


def normalize_last_name(name):
    """"Moran,", "King, Jr.", "Hagerty, IV" -> "moran", "king", "hagerty" --
    strips the suffix/comma artifacts already logged in the House/Senate
    scrapers' raw last_name (see [[Quantgress]]) down to a bare surname, so
    it matches congress-legislators' clean name.last."""
    name = re.sub(r",?\s*(Jr\.?|Sr\.?|I{2,3}|IV|V)$", "", (name or "").strip(), flags=re.IGNORECASE)
    return name.rstrip(",").strip().lower()


def load_party_lookup(legislators):
    """List of congress-legislators records -> {(chamber, norm_last_name): party}.

    Keyed off each person's most recent term *per chamber type*, not just
    their single most recent term overall -- a rep-then-senator resolves
    correctly for both chambers instead of the House stint winning by date.
    """
    lookup = {}
    for person in legislators:
        last = normalize_last_name(person.get("name", {}).get("last"))
        if not last:
            continue
        latest_by_type = {}
        for term in person.get("terms", []):
            t = term.get("type")
            if t in ("sen", "rep") and term.get("party"):
                if t not in latest_by_type or term["start"] > latest_by_type[t]["start"]:
                    latest_by_type[t] = term
        for chamber, term_type in CHAMBER_TERM_TYPE.items():
            if term_type in latest_by_type:
                lookup[(chamber, last)] = latest_by_type[term_type]["party"]
    return lookup


def fetch_party_lookup(s):
    """current file first, then historical fills in whatever's missing --
    NOT the other way around, so a currently-serving member's fresher term
    data always wins over any past-chamber history the same person has.

    Confirmed live this matters: legislators-current.yaml (as of its
    2026-07-15 snapshot) has no entry at all for sitting Sen. Markwayne
    Mullin -- he's only in legislators-historical.yaml. The two files don't
    partition current-vs-retired congressmembers on any date the trades
    data can predict, so both get fetched every run rather than guessing
    which one a given last_name needs.

    Empty dict (every politician shows "Unknown") on any failure -- party is
    enrichment, not worth failing the whole net-worth run over.
    """
    lookup = {}
    for filename in ("legislators-current.yaml", "legislators-historical.yaml"):
        try:
            r = s.get(PARTY_BASE.format(filename), timeout=60)
            r.raise_for_status()
            for key, party in load_party_lookup(yaml.safe_load(r.text)).items():
                lookup.setdefault(key, party)
        except (requests.exceptions.RequestException, yaml.YAMLError):
            print(f"warning: could not fetch {filename} -- some politicians will show Unknown")
    return lookup


def main(limit=None, member=None):
    con = duckdb.connect(DB_PATH, read_only=True)
    positions = con.execute(POSITIONS_SQL).fetchdf()
    if member:
        positions = positions[positions["last_name"].str.contains(member, case=False)]
    if positions.empty:
        print("no net-positive positions found" + (f" for {member!r}" if member else ""))
        return

    symbols = sorted(positions["tkr"].unique())
    if limit is not None:
        symbols = symbols[:limit]
        positions = positions[positions["tkr"].isin(symbols)]

    s = _session()
    party_lookup = fetch_party_lookup(s)
    price_cache = {}
    priced = skipped = 0
    for sym in symbols:
        current, series = fetch_prices(s, sym)
        price_cache[sym] = (current, series)
        priced += current is not None
        skipped += current is None

    rows = []
    for _, pos in positions.iterrows():
        current, series = price_cache[pos["tkr"]]
        basis = nearest_price(series, _to_date(pos["basis_date"]))
        if current is not None and basis:
            value, was_priced = pos["net_invested"] * (current / basis), True
        else:
            value, was_priced = pos["net_invested"], False  # no price data -- unadjusted floor
        party = party_lookup.get((pos["chamber"], normalize_last_name(pos["last_name"])), "Unknown")
        rows.append({"chamber": pos["chamber"], "last_name": pos["last_name"], "party": party,
                      "tkr": pos["tkr"], "net_invested": pos["net_invested"],
                      "mtm_value": round(value), "priced": was_priced})

    df = pd.DataFrame(rows)
    pd.set_option("display.width", 200, "display.max_columns", 50)
    matched = (df["party"] != "Unknown").sum()
    print(f"{priced} tickers priced, {skipped} had no Yahoo data (delisted/unknown symbol)")
    print(f"{df['last_name'].nunique()} politicians, {matched}/{len(df)} positions matched to a known party\n")

    money = lambda x: f"${x:,.0f}"  # display-only -- keep the real numbers for the sums below

    if member:
        df = df.sort_values("mtm_value", ascending=False)
        total = df["mtm_value"].sum()
        printable = df.assign(net_invested=df["net_invested"].map(money),
                               mtm_value=df["mtm_value"].map(money))
        print(printable.to_string(index=False))
        print(f"\n{member} floor net worth (disclosed equities only): {money(total)}")
    else:
        summary = (df.groupby(["chamber", "last_name", "party"])["mtm_value"].sum()
                     .reset_index().rename(columns={"mtm_value": "net_worth_floor"})
                     .sort_values("net_worth_floor", ascending=False))
        by_party = summary.groupby("party")["net_worth_floor"].agg(["sum", "count"])
        by_party.columns = ["total_floor", "politicians"]
        printable = summary.assign(net_worth_floor=summary["net_worth_floor"].map(money))
        print(printable.to_string(index=False))
        by_party_printable = by_party.assign(total_floor=by_party["total_floor"].map(money))
        print(f"\n{by_party_printable.to_string()}")


def selftest():
    fake = {"chart": {"result": [{
        "meta": {"regularMarketPrice": 150.0},
        "timestamp": [1700000000, 1700086400, 1700172800],
        "indicators": {"quote": [{"close": [100.0, None, 110.0]}]},
    }], "error": None}}
    current, series = parse_chart(fake)
    assert current == 150.0
    assert len(series) == 2  # the None close is dropped

    # delisted/unknown symbol -- result is null, not a raise
    assert parse_chart({"chart": {"result": None, "error": {"code": "Not Found"}}}) == (None, [])

    series = [(datetime.date(2026, 1, 1), 10.0), (datetime.date(2026, 3, 1), 20.0)]
    assert nearest_price(series, datetime.date(2026, 2, 1)) == 10.0    # last close before target
    assert nearest_price(series, datetime.date(2025, 1, 1)) == 10.0    # predates series -> earliest
    assert nearest_price(series, datetime.date(2026, 12, 31)) == 20.0  # future txn_date -> latest
    assert nearest_price([], datetime.date(2026, 1, 1)) is None

    # _get retries a connection-level failure and a non-JSON body alike,
    # same "Edge: Too Many Requests" shape observed live without a real UA
    class _FlakyThenOK:
        calls = 0

        def get(self, *a, **k):
            _FlakyThenOK.calls += 1
            if _FlakyThenOK.calls < 3:
                return type("R", (), {"json": lambda self: (_ for _ in ()).throw(ValueError())})()
            return type("R", (), {"json": lambda self: {"ok": True}})()

    real_sleep, time.sleep = time.sleep, lambda _: None
    try:
        assert _get(_FlakyThenOK(), "http://example.invalid", tries=4) == {"ok": True}
        assert _FlakyThenOK.calls == 3
    finally:
        time.sleep = real_sleep

    # the four artifact shapes already observed live in real last_name values
    assert normalize_last_name("Moran,") == "moran"
    assert normalize_last_name("King, Jr.") == "king"
    assert normalize_last_name("Justice, II") == "justice"
    assert normalize_last_name("Hagerty, IV") == "hagerty"
    assert normalize_last_name("McConnell, Jr.") == "mcconnell"
    assert normalize_last_name("Ossoff") == "ossoff"  # no artifact -- passes through

    # party lookup: picks each chamber type's most recent term independently,
    # so a rep-then-senator resolves correctly for both, not just the newer one
    fake_legislators = [{
        "name": {"last": "Example"},
        "terms": [
            {"type": "rep", "start": "2005-01-03", "party": "Democrat"},
            {"type": "rep", "start": "2015-01-03", "party": "Republican"},  # switched parties
            {"type": "sen", "start": "2021-01-03", "party": "Independent"},
        ],
    }]
    lookup = load_party_lookup(fake_legislators)
    assert lookup[("H", "example")] == "Republican"   # latest House term, not the 2005 one
    assert lookup[("S", "example")] == "Independent"
    assert ("H", "nobody") not in lookup

    print("selftest ok")


def arg(flag, default=None):
    return sys.argv[sys.argv.index(flag) + 1] if flag in sys.argv else default


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        n = arg("--limit")
        main(limit=int(n) if n else None, member=arg("--member"))
