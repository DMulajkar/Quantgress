# Deployment cron files

Four `/etc/cron.d` files, one per cadence group (not one per phase — fewer files to install, fewer places to make the `chmod 644` mistake). Install on the target box:

```bash
sudo cp deploy/cron.d/quantgress-* /etc/cron.d/
sudo chmod 644 /etc/cron.d/quantgress-*
mkdir -p /home/ubuntu/quantgress/logs
```

cron silently ignores any `/etc/cron.d` file with group/other write permission — no error anywhere, the job just never runs. `chmod 644` after every copy, not just the first.

| File | Phases | Cadence |
|---|---|---|
| `quantgress-daily` | 1-3 (`daily.py`), 11 (`scrape_short_volume.py`), 9 (`scrape_insiders.py`) | daily, staggered 15 min apart |
| `quantgress-weekly` | 7 (`scrape_contracts.py`), 12 (`scrape_patents.py`) | weekly, Sunday |
| `quantgress-quarterly` | 10 (`scrape_13f.py`), 6 (`scrape_lobbying.py`) | quarterly (Jan/Apr/Jul/Oct 1st) |
| `quantgress-annual` | 13 (`scrape_donors.py`), 16 (`scrape_execcomp.py`), 18 (`scrape_senate_annual.py`) | annual (Jan 2nd) |

**Phase 17 (`scrape_trump.py`) has no cron file, deliberately.** ProPublica's DocumentCloud mirror for Trump's OGE 278-T filings has no fixed publication schedule — there's no real cadence to key a cron line off of. Run it manually (`py scrape_trump.py`) until a pattern in update frequency emerges.

**Phase 14 (`scrape_pageviews.py`) has no cron line, deliberately (confirmed live 2026-08-16).** It takes an explicit `--article` watchlist — there's no ticker→Wikipedia-title mapping yet, so an unattended call with no args just fails the same "usage: ..." error every time. It was in `quantgress-weekly` originally; pulled after a live full-scrape run hit exactly that failure. Run it by hand with `--article` until a watchlist exists.

Full reasoning and the "why not just run everything in `daily.py`" tradeoff: see the [Cron for Scheduled Python Scripts](../../../Second-Brain/05%20Patterns/Cron%20for%20Scheduled%20Python%20Scripts.md) wiki page.
