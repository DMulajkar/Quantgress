"""Phase 5b: API key gate in front of api.py.

Every route on the `router` in api.py (i.e. everything except POST /signup)
requires a valid, unrevoked key in the X-API-Key header. `signup()` backs
the public self-serve form on the marketing site; `issue_key()` is the
lower-level primitive it (and manual CLI issuance) builds on. Tiers and
Stripe billing are still just plans (03 Concepts/Quantgress API
Monetization.md, Quantgress API Key Portal.md).

    py auth.py issue someone@example.com     # create + print a key (once)
    py auth.py revoke <raw key>               # disable a key
    py auth.py list                           # every issued key + status
    py auth.py --selftest                     # offline check, no server
"""

import hashlib
import secrets
import sys
from datetime import datetime, timezone

import duckdb
from fastapi import Header, HTTPException

from schema import DB_PATH


def ensure_keys_table(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS api_keys (
            key_hash VARCHAR PRIMARY KEY,
            email VARCHAR,
            tier VARCHAR DEFAULT 'free',
            created_at TIMESTAMP,
            revoked_at TIMESTAMP
        )
    """)


def init_db():
    """Called once at process start (api.py import) -- idempotent."""
    con = duckdb.connect(DB_PATH)
    try:
        ensure_keys_table(con)
    finally:
        con.close()


def _hash(raw_key):
    return hashlib.sha256(raw_key.encode()).hexdigest()


def issue_key(email, tier="free"):
    """Returns the raw key. This is the only time it's ever visible --
    only its hash is stored, same as a password."""
    raw = "qg_live_" + secrets.token_urlsafe(32)
    con = duckdb.connect(DB_PATH)
    try:
        ensure_keys_table(con)
        con.execute(
            "INSERT INTO api_keys VALUES (?, ?, ?, ?, NULL)",
            [_hash(raw), email, tier, datetime.now(timezone.utc)],
        )
    finally:
        con.close()
    return raw


def signup(email):
    """Self-serve signup path (POST /signup in api.py): one active key per
    email. Keys are shown once and never re-displayed, so letting the same
    email mint a second key would just orphan the first with no way back to
    it -- fail instead.
    # ponytail: check-then-insert has a tiny race under a concurrent double
    # -submit of the same email (worst case: two keys for one person,
    # harmless). Add a UNIQUE constraint if that ever actually matters.
    """
    con = duckdb.connect(DB_PATH)
    try:
        ensure_keys_table(con)
        existing = con.execute(
            "SELECT 1 FROM api_keys WHERE email = ? AND revoked_at IS NULL", [email]
        ).fetchone()
    finally:
        con.close()
    if existing:
        raise ValueError("email already has an active key")
    return issue_key(email)


def revoke_key(raw_key):
    con = duckdb.connect(DB_PATH)
    try:
        ensure_keys_table(con)
        con.execute(
            "UPDATE api_keys SET revoked_at = ? WHERE key_hash = ?",
            [datetime.now(timezone.utc), _hash(raw_key)],
        )
    finally:
        con.close()


def require_key(x_api_key: str = Header(None)):
    """FastAPI dependency. Wired app-wide in api.py so every route needs a
    key -- add an exemption there (not here) if a public route is ever
    wanted."""
    if not x_api_key:
        raise HTTPException(401, "missing X-API-Key header")
    con = duckdb.connect(DB_PATH, read_only=True)
    try:
        row = con.execute(
            "SELECT tier, revoked_at FROM api_keys WHERE key_hash = ?",
            [_hash(x_api_key)],
        ).fetchone()
    finally:
        con.close()
    if row is None or row[1] is not None:
        raise HTTPException(401, "invalid or revoked API key")
    return row[0]  # tier -- unused today, there for a future paid-tier check


def selftest():
    init_db()
    raw = issue_key("selftest@example.com")
    assert require_key(raw) == "free"
    try:
        require_key("not-a-real-key")
        assert False, "expected 401 on bad key"
    except HTTPException as e:
        assert e.status_code == 401
    revoke_key(raw)
    try:
        require_key(raw)
        assert False, "expected 401 on revoked key"
    except HTTPException as e:
        assert e.status_code == 401
    try:
        require_key(None)
        assert False, "expected 401 on missing key"
    except HTTPException as e:
        assert e.status_code == 401
    print("auth selftest OK")


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    elif len(sys.argv) > 1 and sys.argv[1] == "issue":
        print(issue_key(sys.argv[2], sys.argv[3] if len(sys.argv) > 3 else "free"))
    elif len(sys.argv) > 1 and sys.argv[1] == "revoke":
        revoke_key(sys.argv[2])
        print("revoked")
    elif len(sys.argv) > 1 and sys.argv[1] == "list":
        con = duckdb.connect(DB_PATH, read_only=True)
        for row in con.execute(
            "SELECT email, tier, created_at, revoked_at FROM api_keys ORDER BY created_at"
        ).fetchall():
            print(row)
        con.close()
    else:
        print(__doc__)
