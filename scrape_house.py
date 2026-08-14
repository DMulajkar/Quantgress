"""Scrape House periodic transaction reports (PTRs) from disclosures-clerk.house.gov.

Phase 2 of Quantgress. The House publishes one ZIP per year holding a
tab-delimited index of every financial disclosure; rows with FilingType 'P' are
PTRs, and each one's PDF is at ptr-pdfs/{year}/{DocID}.pdf.

Most PTRs are born-digital, so pdfplumber's text layer is enough. Roughly an
eighth are scanned paper (worse in older years): those extract to nothing and
are recorded with status='scanned' instead of blocking the run. `--ocr-queue`
lists them. No OCR here -- see the note at the bottom of this docstring.

Usage:
    py scrape_house.py --selftest          # offline parser checks
    py scrape_house.py --year 2026 --limit 20
    py scrape_house.py --year 2026         # one year
    py scrape_house.py                     # 2012 -> present
    py scrape_house.py --ocr-queue         # filings that need OCR

Re-running skips filings already attempted, so an interrupted run resumes.

# ponytail: no OCR path. Measured ~13% of recent PTRs are scanned (~33% in
# 2016); they queue rather than block. Add pdf2image + pytesseract once the
# queued rows are worth the tesseract dependency and the QA pass they need --
# OCR'd amounts and tickers cannot be trusted unspot-checked.
"""

import datetime
import io
import re
import sys
import time
import zipfile

import duckdb
import pandas as pd
import pdfplumber
import requests

from schema import DB_PATH, ensure_schema, parse_amount

INDEX = "https://disclosures-clerk.house.gov/public_disc/financial-pdfs/{year}FD.zip"
PDF = "https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/{year}/{doc}.pdf"
FIRST_YEAR = 2012  # STOCK Act; earlier years have no PTRs
RATE_LIMIT_SECS = 2  # be polite to a .gov host

# A transaction line ends with type, transaction date, notification date, amount.
# The asset name is whatever precedes it, and may wrap onto following lines.
# Pre-2018 filings write single-digit days ("01/4/2016"), hence \d{1,2}.
ROW = re.compile(
    r"^(?:(?P<owner>[Ss][Pp]|[Jj][Tt]|[Dd][Cc])\s+)?(?P<asset>.+?)\s+"
    r"(?P<type>[PSEpse])(?P<partial>\s*\(partial\))?\s+"  # small caps -> lowercase
    r"(?P<tx_date>\d{1,2}/\d{1,2}/\d{4})\s+\d{1,2}/\d{1,2}/\d{4}\s+"
    r"(?P<amt>\$[\d,]+(?:\s*-\s*(?:\$[\d,]+)?|\s*\+)?)\s*\S*$"
)
# Page furniture: never part of an asset name, but it does NOT end one either.
# A long filing repeats the column header on every page, and an asset name can
# wrap across that break -- "Global Payments Inc. Common Stock" ends page 5 and
# "(GPN) [ST]" begins page 6. Treating the header as a terminator silently drops
# the ticker off every name unlucky enough to straddle a page.
# "gfedc"/"nmlkj" are the form's checkbox glyphs, which pdfplumber renders as
# those literal letters -- they land on their own line after a page break.
SKIP = re.compile(r"^(ID Owner Asset|Type Date|\$200\?|\*|Filing ID|"
                  r"Clerk of the House|Name:|Status:|State/District:|"
                  r"(?:gfedc|nmlkj)\w*\s*$)", re.I)

# What genuinely ends an asset name. Current filings render section labels in
# small caps, which pdfplumber emits as a letter followed by NULs ("D\x00\x00...
# : RSU distribution" = Description) -- parse_text treats any NUL as a stop.
# Pre-2018 filings have no NULs, so labels are caught by shape instead: up to
# three words then a colon ("FIlINg STATuS:", "SuBHOlDINg OF:"). Kept narrow
# deliberately -- an asset name can wrap onto a line ending in a colon
# ("... NYSEARCA:"), and that is five words, so it stays part of the name.
STOP = re.compile(r"^(Transactions|Filer Information|Asset Class Details|"
                  r"Initial Public Offering|Certification|"
                  r"(?:[A-Za-z]+ ){0,2}[A-Za-z]+:)", re.I)
OWNERS = {"SP": "Spouse", "JT": "Joint", "DC": "Dependent Child"}
# "... (INTU) [ST]". Pre-2018 small caps make this lowercase ("[gS]"), so match
# either case and upper() the code before looking it up.
ASSET_CODE = re.compile(r"\s*\[([A-Za-z0-9]{2})\]\s*$")

# https://fd.house.gov/reference/asset-type-codes.aspx
ASSET_TYPES = dict(l.split(" ", 1) for l in """\
4K 401K and Other Non-Federal Retirement Accounts
5C 529 College Savings Plan
5F 529 Portfolio
5P 529 Prepaid Tuition Plan
AB Asset-Backed Securities
BA Bank Accounts, Money Market Accounts and CDs
BK Brokerage Accounts
CO Collectibles
CS Corporate Securities (Bonds and Notes)
CT Cryptocurrency
DB Defined Benefit Pension
DO Debts Owed to the Filer
DS Delaware Statutory Trust
EF Exchange Traded Funds (ETF)
EQ Excepted/Qualified Blind Trust
ET Exchange Traded Notes
FA Farms
FE Foreign Exchange Position (Currency)
FN Fixed Annuity
FU Futures
GS Government Securities and Agency Debt
HE Hedge Funds & Private Equity Funds (EIF)
HN Hedge Funds & Private Equity Funds (non-EIF)
IC Investment Club
IH IRA (Held in Cash)
IP Intellectual Property & Royalties
IR IRA
MA Managed Accounts (e.g., SMA and UMA)
MF Mutual Funds
MO Mineral/Oil/Solar Energy Rights
OI Ownership Interest (Holding Investments)
OL Ownership Interest (Engaged in a Trade or Business)
OP Options
OT Other
PE Pensions
PM Precious Metals
PS Stock (Not Publicly Traded)
RE Real Estate Invest. Trust (REIT)
RF REIT (EIF)
RN REIT (non-EIF)
RP Real Property
RS Restricted Stock Units (RSUs)
SA Stock Appreciation Right
ST Stocks (including ADRs)
TR Trust
VA Variable Annuity
VI Variable Insurance
WU Whole/Universal Insurance""".splitlines())

TX_TYPES = {"P": "Purchase", "S": "Sale (Full)", "E": "Exchange"}


def _session():
    s = requests.Session()
    s.headers["User-Agent"] = "quantgress/0.1 (personal research)"
    return s


def index(s, year):
    """Yield one dict per PTR row in the year's disclosure index."""
    z = zipfile.ZipFile(io.BytesIO(s.get(INDEX.format(year=year), timeout=60).content))
    name = next(n for n in z.namelist() if n.lower().endswith(".txt"))
    lines = z.read(name).decode("utf-8", "replace").splitlines()
    for line in lines[1:]:
        f = line.split("\t")
        if len(f) < 9 or f[4] != "P":  # P = periodic transaction report
            continue
        m, d, y = f[7].split("/")  # index writes M/D/YYYY, unpadded
        yield {
            "doc_id": f[8].strip(), "year": year,
            "last_name": f[1].strip(), "first_name": f[2].strip(),
            "office": f[5].strip(), "filed": f"{int(m):02d}/{int(d):02d}/{y}",
            "link": PDF.format(year=year, doc=f[8].strip()),
        }


def parse_text(text):
    """Transaction rows from a PTR's extracted text. Empty list if none found."""
    rows = []
    open_row = False  # is the last row still allowed to absorb wrapped lines?
    for raw in text.splitlines():
        line = raw.replace("\x00", "").strip()
        if not line or SKIP.match(line):
            continue  # page furniture -- leaves an open asset name open
        m = ROW.match(line)
        if m:
            g = m.groupdict()
            g["type"] = g["type"].upper()
            asset = g["asset"]
            code = ASSET_CODE.search(asset)
            mm, dd, yy = g["tx_date"].split("/")  # pre-2018: "01/4/2016"
            rows.append({
                "tx_date": f"{int(mm):02d}/{int(dd):02d}/{yy}",
                "owner": OWNERS.get((g["owner"] or "").upper(), "Self"),
                "ticker": None,  # not on the House form; lives inside asset_name
                "asset_name": ASSET_CODE.sub("", asset) if code else asset,
                "asset_type": ASSET_TYPES.get(code.group(1).upper()) if code else None,
                "tx_type": ("Sale (Partial)" if g["partial"] and g["type"] == "S"
                            else TX_TYPES.get(g["type"])),
                "amount_raw": " ".join(g["amt"].split()),
            })
            open_row = True
        elif open_row and "\x00" not in raw and not STOP.match(line):
            # continuation: the asset name wrapped, and the amount may have too
            last = rows[-1]
            if last["amount_raw"].endswith("-"):
                tail = re.search(r"(\$[\d,]+)\s*$", line)
                if tail:
                    last["amount_raw"] += " " + tail.group(1)
                    line = line[: tail.start()].strip()
            code = ASSET_CODE.search(line)
            if code:
                last["asset_type"] = ASSET_TYPES.get(code.group(1).upper())
                line = ASSET_CODE.sub("", line)
            if line:
                last["asset_name"] = f"{last['asset_name']} {line}".strip()
        else:
            # a section label, a page header or form boilerplate: the asset name
            # is finished, so nothing after this may append to it
            open_row = False
    for r in rows:
        r["amount_low"], r["amount_high"] = parse_amount(r["amount_raw"])
    return rows


def parse_pdf(data):
    """(rows, status). status 'scanned' means no text layer -- queue for OCR."""
    with pdfplumber.open(io.BytesIO(data)) as pdf:
        text = "\n".join(p.extract_text() or "" for p in pdf.pages)
    if len(text.strip()) < 100:
        return [], "scanned"
    rows = parse_text(text)
    return rows, "ok" if rows else "empty"


def main(years, limit=None):
    con = duckdb.connect(DB_PATH)
    ensure_schema(con)
    con.execute("""CREATE TABLE IF NOT EXISTS house_filings (
        doc_id VARCHAR PRIMARY KEY, year INTEGER, last_name VARCHAR,
        first_name VARCHAR, office VARCHAR, filed VARCHAR, link VARCHAR,
        status VARCHAR, n_rows INTEGER)""")
    done = {r[0] for r in con.execute("SELECT doc_id FROM house_filings").fetchall()}

    s = _session()
    seen = {"ok": 0, "scanned": 0, "empty": 0, "error": 0}
    for year in years:
        try:
            ptrs = list(index(s, year))
        except Exception as e:
            print(f"{year}: index unavailable ({e})")
            continue
        print(f"{year}: {len(ptrs)} PTRs in index")
        for f in ptrs:
            if limit is not None and sum(seen.values()) >= limit:
                break
            if f["doc_id"] in done:
                continue
            time.sleep(RATE_LIMIT_SECS)
            try:
                r = s.get(f["link"], timeout=60)
                r.raise_for_status()
                rows, status = parse_pdf(r.content)
            except Exception as e:
                rows, status = [], "error"
                print(f"  {f['doc_id']} {f['last_name']}: {e}")
            if rows:
                df = pd.DataFrame(rows).assign(
                    first_name=f["first_name"], last_name=f["last_name"],
                    office=f["office"], filed=f["filed"], link=f["link"])
                con.execute(
                    "INSERT INTO house_trades (first_name, last_name, office,"
                    " filed, link, tx_date, owner, ticker, asset_name,"
                    " asset_type, tx_type, amount_raw, amount_low, amount_high)"
                    " SELECT first_name, last_name, office, filed, link,"
                    " tx_date, owner, ticker, asset_name, asset_type, tx_type,"
                    " amount_raw, amount_low, amount_high FROM df")
            con.execute(
                "INSERT INTO house_filings VALUES (?,?,?,?,?,?,?,?,?)",
                [f["doc_id"], year, f["last_name"], f["first_name"], f["office"],
                 f["filed"], f["link"], status, len(rows)])
            seen[status] += 1
            print(f"  {f['last_name']}, {f['first_name']} — {len(rows)} txns"
                  + ("" if status == "ok" else f" [{status}]"))
        if limit is not None and sum(seen.values()) >= limit:
            break

    total = con.execute("SELECT count(*) FROM house_trades").fetchone()[0]
    print(f"\n{total} House transactions in {DB_PATH}")
    print("this run: " + ", ".join(f"{k}={v}" for k, v in seen.items()))
    queued = con.execute(
        "SELECT count(*) FROM house_filings WHERE status = 'scanned'").fetchone()[0]
    if queued:
        print(f"{queued} filings queued for OCR — see `py scrape_house.py --ocr-queue`")


def ocr_queue():
    con = duckdb.connect(DB_PATH, read_only=True)
    print(con.execute(
        """SELECT year, last_name, first_name, office, filed, link
           FROM house_filings WHERE status = 'scanned'
           ORDER BY year DESC, last_name"""
    ).df().to_string())


def selftest():
    assert parse_amount("$50,000,001 +") == (50000001, None)

    # Verbatim pdfplumber output, NULs and all, from filings 20034945 / 20034024.
    text = (
        "Filing ID #20034945\n"
        "P\x00\x00 T\x00\x00 R\x00\x00\n"
        "Name: Hon. Richard W. Allen\n"
        "ID Owner Asset Transaction Date Notification Amount Cap.\n"
        "Type Date Gains >\n"
        "$200?\n"
        "SP Intuit Inc. - Common Stock (INTU) S 06/10/2026 07/07/2026 $15,001 -\n"
        "[ST] $50,000\n"
        "F\x00\x00\x00\x00\x00 S\x00\x00\x00\x00\x00: New\n"
        "S\x00\x00\x00\x00\x00\x00\x00\x00\x00 O\x00: LIVTR\n"
        "* For the complete list of asset type abbreviations, please visit x\n"
        "State Street Corporation Common S (partial) 02/17/2026 02/17/2026 $15,001 -\n"
        "Stock (STT) [ST] $50,000\n"
        "D\x00\x00\x00\x00: RSU distribution\n"
        "Listen Ventures IV, LP [HN] P 05/13/2026 05/13/2026 $250,001 -\n"
        "$500,000\n"
        # an asset name straddling a page break: the repeated column header
        # sits between the transaction line and the rest of its name
        "Global Payments Inc. Common Stock S 05/15/2026 06/05/2026 $1,001 - $15,000\n"
        "ID Owner Asset Transaction Date Notification Amount Cap.\n"
        "Type Date Gains >\n"
        "$200?\n"
        "gfedc\n"
        "(GPN) [ST]\n"
        "F\x00\x00\x00\x00\x00 S\x00\x00\x00\x00\x00: New\n"
    )
    rows = parse_text(text)
    assert len(rows) == 4, rows

    a, b, c, page_split = rows
    assert page_split["asset_name"] == "Global Payments Inc. Common Stock (GPN)", page_split["asset_name"]
    assert page_split["asset_type"] == "Stocks (including ADRs)"
    assert a["owner"] == "Spouse" and a["tx_type"] == "Sale (Full)"
    assert a["asset_name"] == "Intuit Inc. - Common Stock (INTU)", a["asset_name"]
    assert a["asset_type"] == "Stocks (including ADRs)"
    assert (a["amount_low"], a["amount_high"]) == (15001, 50000)
    assert a["tx_date"] == "06/10/2026"

    # blank owner column means the filer; wrapped name AND wrapped amount
    assert b["owner"] == "Self" and b["tx_type"] == "Sale (Partial)"
    assert b["asset_name"] == "State Street Corporation Common Stock (STT)", b["asset_name"]
    assert (b["amount_low"], b["amount_high"]) == (15001, 50000)

    # asset code inline, amount wrapping with no name continuation
    assert c["asset_name"] == "Listen Ventures IV, LP", c["asset_name"]
    assert c["asset_type"] == "Hedge Funds & Private Equity Funds (non-EIF)"
    assert (c["amount_low"], c["amount_high"]) == (250001, 500000)
    assert c["tx_type"] == "Purchase"

    # section labels and boilerplate never leak into an asset name
    for r in rows:
        assert "New" not in r["asset_name"] and "RSU" not in r["asset_name"]
        assert "complete list" not in r["asset_name"]

    # Pre-2018 layout (filing 20006390): no NULs, no [XX] asset code, and
    # single-digit days. Section labels are ordinary text, caught by shape.
    old = (
        "iD owner asset transaction Date notification amount\n"
        "type Date\n"
        "sP apple Inc. (aaPl) P 03/9/2016 03/9/2016 $1,001 - $15,000\n"
        "LB, LLC- Edgewood in the Pines golf s 07/1/2016 07/1/2016 $1,001 - $15,000\n"
        "Course\n"
        "DEsCRIPTIoN: sold share of ownership\n"
        "Cliffs Natural Resources Inc (ClF) P 12/2/2016 12/6/2016 $1,001 - $15,000\n"
        "FIlINg STATuS: New\n"
        "SuBHOlDINg OF: Brad Ashford Retirement Account > Brad Ashford SEP IRA\n"
        "Walt Disney Company (DIS) [gS] S 01/4/2016 01/4/2016 $15,001 - $50,000\n"
        "aSSet claSS DetailS\n"
        "Brad Ashford Retirement Account\n"
        "gfedcb I CERTIFY that the statements I have made are true\n"
    )
    c1, c2, d, e = parse_text(old)
    # the owner code is small-capped too, so it must not land in the asset name
    assert c1["owner"] == "Spouse", c1["owner"]
    assert c1["asset_name"] == "apple Inc. (aaPl)", c1["asset_name"]
    # small-caps rendering makes the type letter lowercase in some old filings
    assert c2["tx_type"] == "Sale (Full)", c2["tx_type"]
    assert c2["asset_name"] == "LB, LLC- Edgewood in the Pines golf Course", c2["asset_name"]
    assert d["asset_name"] == "Cliffs Natural Resources Inc (ClF)", d["asset_name"]
    assert d["tx_date"] == "12/02/2016" and d["asset_type"] is None
    assert (d["amount_low"], d["amount_high"]) == (1001, 15000)
    assert e["asset_name"] == "Walt Disney Company (DIS)", e["asset_name"]
    assert e["tx_date"] == "01/04/2016", "single-digit day must zero-pad"
    # small-capped asset code still resolves, and comes off the asset name
    assert e["asset_type"] == "Government Securities and Agency Debt"
    assert e["tx_type"] == "Sale (Full)"

    assert parse_text("nothing here at all\n") == []
    print("selftest ok")


def arg(flag, default=None):
    return sys.argv[sys.argv.index(flag) + 1] if flag in sys.argv else default


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    elif "--ocr-queue" in sys.argv:
        ocr_queue()
    else:
        y = arg("--year")
        years = [int(y)] if y else range(FIRST_YEAR, datetime.date.today().year + 1)
        n = arg("--limit")
        main(years, int(n) if n else None)
