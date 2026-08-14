"""Phase 4: daily incremental pull -- run all three scrapers back to back.

STOCK Act gives filers 30-45 days to disclose, so once-a-day is plenty. Each
step already does the incremental work itself (senate skips links already in
senate_trades, house skips doc_ids already in house_filings), so this file is
just the call order, not new scraping logic.

House only needs the current + previous year checked: the annual ZIP is keyed
by filing year, and a late Dec filing can land in next year's index in
January, so the prior year stays live for a few weeks after New Year's.
Older years never gain filings after publication.

Usage:
    py daily.py            # senate (all years) + house (this year, last year) + tickers
    py daily.py --selftest # offline check of the year window, no network

Meant to run under Windows Task Scheduler once a day; stdout/stderr redirect
to a log file at the scheduler level (`cmd /c ... >> daily.log 2>&1`) rather
than a logging setup in here -- one redirect beats a logging config for a
script with three print-heavy steps already.
"""

import datetime
import sys

import entities
import scrape_house
import scrape_senate
import scrape_short_volume


def house_years(today=None):
    y = (today or datetime.date.today()).year
    return [y - 1, y]


def main():
    print(f"=== Quantgress daily run: {datetime.datetime.now()} ===")

    print("\n--- Senate ---")
    scrape_senate.main()

    years = house_years()
    print(f"\n--- House {years} ---")
    scrape_house.main(years)

    print("\n--- Resolve entities ---")
    entities.main()

    # Phase 11: the first dataset in this file that's genuinely daily -- one
    # new file per trade date, posted by 6pm ET, no year-window bookkeeping
    # needed like Senate/House above (scrape_short_volume's own done-set skip
    # handles resumption). Its own default trailing window (DAYS_DEFAULT) is
    # plenty for a once-a-day cron.
    today = datetime.date.today()
    print("\n--- Off-exchange short volume ---")
    scrape_short_volume.main(today - datetime.timedelta(days=scrape_short_volume.DAYS_DEFAULT), today)


def selftest():
    assert house_years(datetime.date(2026, 1, 15)) == [2025, 2026]
    assert house_years(datetime.date(2026, 8, 14)) == [2025, 2026]
    print("selftest ok")


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        main()
