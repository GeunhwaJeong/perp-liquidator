#!/usr/bin/env python3
# Copyright (c) 2026 Geunhwa Jeong
# SPDX-License-Identifier: Apache-2.0
"""Check perp-keeper's details path against two scratch Postgres databases.

    scripts/localnet/keeper_smoke.py --keeper <perp-keeper binary> \\
        --migration <perp-indexer>/crates/schema/migrations/2026-10-02-000000-0000_state/up.sql

Needs no network: the keeper only takes details in (no --deployment, --rpc-url or --key-file).
One scratch database is its own, the other stands in for the indexer's, with the real
`order_tickets` table from the indexer's migration. psql, createdb and dropdb must be on PATH
and reach a local server. Checks that:

1. it starts, creates its table and answers /health;
2. the details of the front end's test vectors are stored under the commitments the front end
   computes, and the same details again are known;
3. it refuses details while --max-unmatched wait, a bad trigger type (400), an unknown kind
   (422), a body that is not JSON (415) and one over --max-body-bytes (413);
4. stored details read back as handed in;
5. once a ticket holding a commitment reaches the indexer, those details are marked matched;
   the rest are dropped after --unmatched-ttl-secs, and new details are taken again;
6. it stops on SIGTERM with exit code 0.
"""

import argparse
import json
import re
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

DETAILS_DB, INDEXER_DB = "sigma_keeper_dev", "sigma_keeper_dev_indexer"
BASE = "http://127.0.0.1:9191"
SALT = "".join(f"{i:02x}" for i in range(32))
CH = "0xf77dda417d4003c017c96697809d3a25b2767aca586b5b7579e975e47508df77"
TTL = 12

results = []


def check(name, ok, detail=""):
    results.append(ok)
    print(("PASS " if ok else "FAIL ") + name + (f"  [{detail}]" if detail else ""))


def psql(db, sql):
    out = subprocess.run(["psql", "-X", "-qAt", "-d", db, "-c", sql], capture_output=True, text=True)
    if out.returncode != 0:
        raise RuntimeError(out.stderr)
    return out.stdout.strip()


def post(body, content_type="application/json"):
    data = body if isinstance(body, bytes) else json.dumps(body).encode()
    headers = {"Content-Type": content_type} if content_type else {}
    req = urllib.request.Request(BASE + "/v1/details", data=data, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            return r.status, json.loads(r.read() or b"null")
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            return e.code, json.loads(raw)
        except ValueError:
            return e.code, raw.decode()


def wait_for(predicate, timeout):
    deadline = time.time() + timeout
    while time.time() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.5)
    return predicate()


SLTP_LONG = {"kind": "stopLossTakeProfit", "details": {
    "clearingHouse": CH, "isLimitOrder": False, "triggerPriceType": 0,
    "stopLossPrice": "95000000000000000000000", "takeProfitPrice": "105000000000000000000000",
    "positionIsAsk": False, "size": "250000000", "price": "0", "orderType": "0", "salt": SALT}}
TP_SHORT = {"kind": "stopLossTakeProfit", "details": {
    "clearingHouse": CH, "expireTimestampMs": "1800000000000", "isLimitOrder": True,
    "triggerPriceType": 2, "takeProfitPrice": "90000000000000000000000", "positionIsAsk": True,
    "size": "100000000", "price": "90000000000", "orderType": "1", "salt": SALT}}
STANDALONE = {"kind": "standaloneStop", "details": {
    "clearingHouse": CH, "expireTimestampMs": "1800000000000", "isLimitOrder": True,
    "triggerPriceType": 1, "stopIndexPrice": "101000000000000000000000", "triggerAtOrAbove": True,
    "side": False, "size": "100000000", "price": "101500000000", "orderType": "0",
    "reduceOnly": True, "salt": SALT}}
TWAP = {"kind": "twap", "details": {
    "firstRunExpireTimestampMs": "1800000060000", "executionGapMs": "60000",
    "executionTimeUncertaintyMs": "10000", "chunksAmount": "4", "smallTailMergeThresholdBps": "0",
    "timeForRetryMs": "30000", "amountUncertaintyBps": "0", "maxOneExecutionAmountBps": "2500",
    "side": False, "size": "1000000000", "maxSlippageBps": "100", "reduceOnly": False, "salt": SALT}}
EXPECTED = [
    (SLTP_LONG, "330487179a9f27ca0f9adf7fca5bb677b5484ded8aaecabf9cc5ca5fe1e372b8"),
    (TP_SHORT, "b3391bdd5582bb2db8e7237f6c0b5805a78399feb0c33d7c1e21842bbd19a689"),
    (STANDALONE, "17c0fe7d24464e1d3df5dcf1cf54c9534e5044e6284a9ee22272a5e6c3f229ec"),
    (TWAP, "c132d3da096c43c38b2c96ad1f4bc9edac7441c1a272dd3f0d7bb927596539ec"),
]


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--keeper", required=True)
    parser.add_argument("--migration", required=True)
    parser.add_argument("--work", default=".", help="where the keeper's log goes")
    args = parser.parse_args()
    log_path = Path(args.work).resolve() / "keeper-smoke.log"

    # Scratch databases: the keeper's own, and an indexer with the real order_tickets table.
    for db in (DETAILS_DB, INDEXER_DB):
        subprocess.run(["dropdb", "--if-exists", db], check=True)
        subprocess.run(["createdb", db], check=True)
    ddl = re.search(r"CREATE TABLE order_tickets \(.*?\n\);", Path(args.migration).read_text(), re.S).group(0)
    psql(INDEXER_DB, ddl)

    keeper = subprocess.Popen(
        [str(Path(args.keeper).resolve()),
         "--details-database-url", f"postgres://localhost:5432/{DETAILS_DB}",
         "--database-url", f"postgres://localhost:5432/{INDEXER_DB}",
         "--max-unmatched", "4", "--match-interval-secs", "1", "--unmatched-ttl-secs", str(TTL)],
        stdout=open(log_path, "w"), stderr=subprocess.STDOUT)
    try:
        def healthy():
            try:
                with urllib.request.urlopen(BASE + "/health", timeout=2) as r:
                    return r.status == 200
            except OSError:
                return False
        check("starts, creates its table and answers /health", wait_for(healthy, 15))
        check("table created", psql(DETAILS_DB, "SELECT to_regclass('ticket_details') IS NOT NULL") == "t")

        for body, commitment in EXPECTED:
            status, answer = post(body)
            check(f"{body['kind']} stored under the front end's commitment",
                  status == 200 and answer == {"commitment": commitment, "known": False}, f"{status} {answer}")

        status, answer = post(SLTP_LONG)
        check("the same details again are known", status == 200 and answer.get("known") is True, f"{status} {answer}")

        other = json.loads(json.dumps(SLTP_LONG))
        other["details"]["salt"] = "ff" * 32
        status, answer = post(other)
        check("refused while --max-unmatched details wait", status == 503, f"{status} {answer}")

        bad = json.loads(json.dumps(SLTP_LONG))
        bad["details"]["triggerPriceType"] = 3
        status, answer = post(bad)
        check("bad trigger type: 400 with the reason", status == 400 and "triggerPriceType 3" in answer["error"], f"{status} {answer}")

        unknown = {"kind": "limit", "details": {}}
        status, answer = post(unknown)
        check("unknown kind: 422", status == 422, f"{status} {answer}")

        status, answer = post(json.dumps(SLTP_LONG).encode(), content_type=None)
        check("no JSON content type: 415", status == 415, f"{status} {answer}")

        huge = json.loads(json.dumps(SLTP_LONG))
        huge["details"]["salt"] = "00" * 3000
        status, answer = post(huge)
        check("body over --max-body-bytes: 413", status == 413, f"{status}")

        stored = psql(DETAILS_DB, f"SELECT kind || ' ' || details FROM ticket_details WHERE commitment = decode('{EXPECTED[3][1]}', 'hex')")
        check("stored details read back as submitted",
              stored.startswith("twap ") and json.loads(stored[5:])["kind"] == "twap"
              and {k: v for k, v in json.loads(stored[5:])["details"].items() if v is not None} == TWAP["details"], stored[:80])

        # A ticket holding the first commitment reaches the indexer.
        psql(INDEXER_DB, f"""INSERT INTO order_tickets (ticket_id, kind, account_id, collateral_type, status,
            executors, gas, stop_order_type, encrypted_details, created_checkpoint, created_at_ms,
            updated_checkpoint, updated_at_ms) VALUES ('0x01', 'stop', 7, '0x2::tusd::TUSD', 'open',
            '[]', 1000000, 0, decode('{EXPECTED[0][1]}', 'hex'), 1, 1, 1, 1)""")

        def matched():
            return psql(DETAILS_DB, "SELECT string_agg(left(encode(commitment, 'hex'), 8), ',' ORDER BY 1) FROM ticket_details WHERE matched_at_ms IS NOT NULL")
        check("the ticket's details are marked matched", wait_for(lambda: matched() == "33048717", 10), matched())

        def remaining():
            return psql(DETAILS_DB, "SELECT string_agg(left(encode(commitment, 'hex'), 8), ',' ORDER BY 1) FROM ticket_details")
        check(f"unmatched details dropped after {TTL} s, the matched one kept",
              wait_for(lambda: remaining() == "33048717", TTL + 10), remaining())

        status, answer = post(other)
        check("taken again once the waiting ones are gone", status == 200 and answer.get("known") is False, f"{status} {answer}")
    finally:
        keeper.send_signal(signal.SIGTERM)
        code = keeper.wait(timeout=15)
        log = log_path.read_text()
        check("stops on SIGTERM with exit 0", code == 0 and "Stopping" in log, f"exit {code}")
        for db in (DETAILS_DB, INDEXER_DB):
            subprocess.run(["dropdb", "--if-exists", db], check=False)

    print(f"{sum(results)}/{len(results)} passed")
    sys.exit(0 if all(results) else 1)


if __name__ == "__main__":
    main()
