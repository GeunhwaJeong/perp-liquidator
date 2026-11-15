#!/usr/bin/env python3
# Copyright (c) 2026 Geunhwa Jeong
# SPDX-License-Identifier: Apache-2.0

"""Check on a local network what the keeper says and holds back when it cannot act on a ticket.

    scripts/localnet/keeper_watch.py up   --work <new scratch directory> --postgres-bin <dir>
    scripts/localnet/keeper_watch.py run  --work <the same directory> --postgres-bin <dir>
    scripts/localnet/keeper_watch.py down --work <the same directory> --postgres-bin <dir>

`run` restarts the keeper with short waits and with an alert webhook that this script serves, and
checks four things on a long of 0.05 BTC with a stop loss ticket (`--phase` picks some of them):

1. `stuck`: a ticket whose details the keeper does not have is only counted at first; after
   `--stuck-alert-after-secs` it is a problem on `/status`, named in the log and in one alert,
   while `/health` stays 200. With its details handed in again the problem goes.
2. `lag`: the position is closed, the indexer is stopped, and the position is opened again, so
   that the indexer still shows none. With `--max-indexer-lag-secs` out of reach the keeper
   cancels the stop loss of a position that is there. With the limit in force it raises the
   `indexer_lag` alert, cancels nothing, and goes on as before once the indexer runs again.
3. `twap`: a TWAP ticket made with no gas and one of 121 chunks are counted
   (`twap_underfunded`, `twap_too_many_chunks`), run no chunk and stay open; one with the front
   end's gas runs its two chunks and is finalized.
4. `reading`: the indexer's database stops taking connections while the keeper's own still
   answers. The failure is a warning at first; `--reading-critical-after-secs` after the first
   failed round `/health` is 503 with the reason and an alert is posted; with the database back
   the keeper is healthy again.

`up` and `down` are sltp_thin_book.py's: mark_drag.py's stack, a PostgreSQL cluster on --pg-port,
the indexer and the keeper. The work directory has to be a new one each time.

On 2026-11-16, with the engine at 2d353bc: all four as described. In 2, without the limit the
keeper cancelled the ticket within the 27 s the indexer stood still, and the ticket was deleted
while the account held 0.05 BTC. In 4, a failed round takes the 10 s of the pool's connection
timeout, so `/health` turned 21 s after the database stopped with a wait of 10 s.

Needs grpcurl, Node 22, PostgreSQL's binaries, and built perp-indexer and perp-keeper binaries.
Refuses to run anywhere but a local chain.
"""

import argparse
import json
import os
import signal
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent))
import first_cap as fcap  # noqa: E402
import mark_drag as md  # noqa: E402
import sltp_thin_book as tb  # noqa: E402
from mark_drag import LOT, call, obj  # noqa: E402

KEEPER = tb.KEEPER
PHASES = ["stuck", "lag", "twap", "reading"]
# The keeper's waits in this check, in seconds: short, and the indexer's limit under the wait
# after which a ticket without its position is cancelled.
STUCK_SECS, LAG_SECS, READING_SECS, ORPHAN_SECS = 10, 8, 10, 15

ALERTS = []


class Sink(BaseHTTPRequestHandler):
    """The alert webhook: keeps what the keeper posts."""

    def do_POST(self):  # noqa: N802
        body = self.rfile.read(int(self.headers.get("Content-Length", 0))).decode(errors="replace")
        try:
            posted = json.loads(body)
            text = posted.get("content") or posted.get("text") or body
        except ValueError:
            text = body
        ALERTS.append(text)
        self.send_response(204)
        self.end_headers()

    def log_message(self, *a):
        pass


def get(path, timeout=3):
    """The status code and the body; 0 when the keeper does not answer in time."""
    try:
        with urllib.request.urlopen(KEEPER + path, timeout=timeout) as r:
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()
    except Exception as e:  # noqa: BLE001
        return 0, str(e)


def status():
    code, body = get("/status")
    return json.loads(body) if code == 200 else {}


def tickets():
    return status().get("tickets", {})


def problems():
    return status().get("problems", {})


def metric(name, **labels):
    """The sum of a metric's samples that carry the labels."""
    _, text = get("/metrics")
    total = 0.0
    for line in text.splitlines():
        head, _, value = line.rpartition(" ")
        if (head == name or head.startswith(name + "{")) and all(f'{k}="{v}"' in head for k, v in labels.items()):
            total += float(value)
    return total


def cancellations():
    return metric("perp_keeper_executions_total", kind="stop_cancel", result="executed")


def chunks_run():
    return metric("perp_keeper_executions_total", kind="twap_chunk", result="executed")


class Check(tb.Check):
    """sltp_thin_book's tickets and reads, with a keeper this check starts itself."""

    def __init__(self, s, pg, args):
        super().__init__(s, pg)
        self.out.close()
        self.out = open(s.logs / "keeper-watch-results.txt", "a")
        self.args = args
        self.pids = json.loads(s.pids_file.read_text())
        p = subprocess.run(["grpcurl", "-plaintext", "-max-time", "10", md.NODE, "haneul.rpc.v2.LedgerService/GetServiceInfo"], capture_output=True, text=True)
        self.chain_id = json.loads(p.stdout)["chainId"]
        self.keeper_log = s.logs / "keeper-watch.log"

    # ------------------------------------------------------------ the keeper, on this check's waits

    def restart_keeper(self, max_lag=LAG_SECS):
        try:
            os.kill(self.pids["keeper"], signal.SIGTERM)
        except ProcessLookupError:
            pass
        tb.wait(lambda: not md.port_open(tb.KEEPER_PORT), 15)
        s = self.s
        with open(self.keeper_log, "a") as out:
            out.write(f"\n===== the keeper, started with --max-indexer-lag-secs {max_lag} =====\n")
            out.flush()
            self.pids["keeper"] = subprocess.Popen(
                [str(Path(self.args.keeper).expanduser()), "--details-database-url", self.pg.url(tb.DETAILS_DB), "--database-url", self.pg.url(tb.INDEXER_DB),
                 "--deployment", str(s.fixture_dir / "public/configs/haneul/perp.localnet.json"),
                 "--rpc-url", "http://127.0.0.1:9000", "--chain-id", self.chain_id,
                 "--key-file", str(s.fixture_dir / ".localnet/keeper.key"), "--address", self.keeper_address,
                 "--oracle-updates-url", "http://127.0.0.1:8787/v1/updates", "--listen-address", f"127.0.0.1:{tb.KEEPER_PORT}",
                 "--poll-interval-ms", "500", "--match-interval-secs", "1", "--min-gas-balance", "1",
                 "--cancel-orphans-after-secs", str(ORPHAN_SECS), "--max-slippage-bps", "500",
                 "--stuck-alert-after-secs", str(STUCK_SECS), "--reading-critical-after-secs", str(READING_SECS),
                 "--max-indexer-lag-secs", str(max_lag),
                 "--alert-webhook-url", f"http://127.0.0.1:{self.args.sink_port}/", "--alert-format", "discord", "--alert-repeat-secs", "5"],
                stdout=out, stderr=subprocess.STDOUT, env=dict(os.environ, RUST_LOG="info,perp_keeper=debug", NO_COLOR="1"),
                cwd=s.work, start_new_session=True,
            ).pid
        # `down` stops the keeper this file names.
        s.pids_file.write_text(json.dumps(self.pids))
        if not tb.wait(tb.healthy, 30):
            sys.exit(f"the keeper did not come up; see {self.keeper_log}")

    def log(self):
        return self.keeper_log.read_text(errors="replace")

    def psql(self, database, query):
        return self.pg.tool("psql", "-h", "127.0.0.1", "-p", str(self.pg.port), "-U", "tester", "-d", database, "-tAc", query).strip()

    # ------------------------------------------------------------ tickets

    def post(self, body):
        """Hands the details in and returns the commitment the keeper answers with."""
        req = urllib.request.Request(KEEPER + "/v1/details", data=json.dumps(body).encode(), headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=5) as r:
            return bytes.fromhex(json.loads(r.read())["commitment"])

    def ticket_head(self, gas, commitment):
        cmds = ["--split-coins", "gas", f"[{gas}]", "--assign", "g"]
        cmds += ["--make-move-vec", "<address>", f"[{obj(self.keeper_address)}]", "--assign", "executors"]
        cmds += call("0x1::option::none", ["address"]) + ["--assign", "no_domain"]
        cmds += ["--make-move-vec", "<u8>", fcap.vec_u8(commitment), "--assign", "commitment"]
        return cmds

    def stop_ticket(self, label, trader, account, commitment):
        cmds = self.ticket_head(self.ticket_gas, commitment)
        cmds += call(f"{self.orders}::stop_orders::create_stop_order_ticket", [self.tusd, self.admin], obj(account["obj"]), obj(account["cap"]),
                     obj(self.registry), "executors", "no_domain", "g.0", "0u64", "commitment")
        j, _ = self.ptb(label, cmds, sender=trader)
        return self.events(j, "::events::CreatedStopOrderTicket")[0]["ticket_id"]

    def stop_loss(self):
        return self.details(50, stop=tb.STOP, worst=tb.WORST_STOP)

    def twap_details(self, lots, chunks, gap_ms=4_000):
        """A TWAP buy, each chunk an immediate-or-cancel order within 5% of the mark price."""
        return {"kind": "twap", "details": {
            "executionGapMs": str(gap_ms), "executionTimeUncertaintyMs": "1000", "chunksAmount": str(chunks),
            "smallTailMergeThresholdBps": "0", "timeForRetryMs": "30000", "amountUncertaintyBps": "0",
            "maxOneExecutionAmountBps": "10000", "side": False, "size": str(lots * LOT), "maxSlippageBps": "500",
            "reduceOnly": False, "salt": os.urandom(32).hex()}}

    def twap_ticket(self, label, trader, account, body, gas):
        cmds = self.ticket_head(gas, self.post(body))
        cmds += call(f"{self.orders}::twap_orders::create_twap_order_ticket", [self.tusd, self.admin], obj(account["obj"]), obj(account["cap"]),
                     obj(self.ch), obj(self.registry), "executors", "no_domain", "g.0", "commitment")
        j, _ = self.ptb(label, cmds, sender=trader)
        return self.events(j, "::events::CreatedTWAPOrderTicket")[0]["ticket_id"]

    # ------------------------------------------------------------ the position and the indexer

    def trade(self, trader, account, is_ask, lots):
        """The trader's order at 100,000 against one the maker rests for it."""
        self.maker_order("the maker's order", not is_ask, lots, 100_000)
        self.session_as("the trader's order", account, account["cap"], self.admin, [self.order(is_ask, lots, 100_000, 3)], trader)

    def indexer(self, sig):
        os.kill(self.pids["indexer"], sig)

    # ------------------------------------------------------------ the phases

    def stuck(self, trader, account):
        say, expect = self.say, self.expect
        say(f"--- 1. a ticket whose details the keeper does not have (alert after {STUCK_SECS} s)")
        body = self.stop_loss()
        commitment = self.post(body)
        self.psql(tb.DETAILS_DB, f"DELETE FROM ticket_details WHERE commitment = '\\x{commitment.hex()}'")
        before = len(ALERTS)
        ticket = self.stop_ticket("a stop loss ticket, its details lost", trader, account, commitment)
        tb.wait(lambda: tickets().get("no_details") == 1, 20)
        expect("the keeper counts it as no_details, and says nothing yet", tickets().get("no_details") == 1 and "stuck_tickets" not in problems())
        tb.wait(lambda: "stuck_tickets" in problems(), STUCK_SECS + 10)
        problem = problems().get("stuck_tickets", {})
        say(f"  /status after the wait: {problem.get('level')}: {problem.get('message')}")
        expect("then /status shows the ticket as a problem", ticket in problem.get("message", "") and "(no_details)" in problem.get("message", ""))
        tb.wait(lambda: len(ALERTS) > before, 10)
        posted = [a for a in ALERTS[before:] if "could not be executed" in a]
        say(f"  alert posted: {posted[0][:230] if posted else None}")
        expect("and one alert is posted that names it", len(posted) >= 1 and ticket in posted[0])
        expect("the log names the ticket once", self.log().count("The ticket cannot be executed and will not fire as it is") == 1)
        expect("the keeper stays healthy: the other tickets are executed", get("/health")[0] == 200)

        self.post(body)
        tb.wait(lambda: "stuck_tickets" not in problems(), 20)
        say(f"  with its details handed in again: tickets {tickets()}, problems {list(problems())}")
        expect("once the details arrive the ticket waits for its price and the problem is gone", tickets().get("waiting") == 1 and "stuck_tickets" not in problems())
        return ticket

    def standstill(self, trader, account, ticket, max_lag):
        """Closes the position, stops the indexer, opens the position again and waits past the keeper's wait for a ticket without its position."""
        say = self.say
        self.restart_keeper(max_lag=max_lag)
        cancelled, alerts = cancellations(), len(ALERTS)
        self.trade(trader, account, True, 50)
        tb.wait(lambda: self.base(account) == 0, 20)
        tb.wait(lambda: tickets().get("no_position") == 1, 10)
        say(f"     the position is closed, and the keeper sees the ticket without one: {tickets()}")
        self.indexer(signal.SIGSTOP)
        stopped = time.time()
        try:
            self.trade(trader, account, False, 50)
            say("     the indexer is stopped and the trader opens the position again: 0.05 BTC long on chain, none in the indexer")
            time.sleep(ORPHAN_SECS + 12)
            seen = status()
            result = {
                "cancelled": cancellations() - cancelled,
                "problems": list(seen.get("problems", {})),
                "lag_ms": seen.get("indexer_lag_ms") or 0,
                "alerts": [a for a in ALERTS[alerts:] if "behind the chain" in a],
                "waited": time.time() - stopped,
            }
        finally:
            self.indexer(signal.SIGCONT)
        tb.wait(lambda: self.base(account) != 0, 30)
        tb.wait(lambda: "indexer_lag" not in problems(), 20)
        result.update(ticket=self.status(ticket), base=self.base(account), problems_after=list(problems()))
        return result

    def lag(self, trader, account, ticket):
        say, expect = self.say, self.expect
        say(f"--- 2. the indexer stands still while a position is opened again (a ticket is cancelled {ORPHAN_SECS} s after its position is gone)")
        say("  a) with --max-indexer-lag-secs out of reach")
        r = self.standstill(trader, account, ticket, 100_000)
        say(f"     {r['waited']:.0f} s later: cancellations sent {r['cancelled']:.0f}, lag shown {r['lag_ms']} ms, problems {r['problems']}")
        say(f"     once the indexer runs again: ticket {r['ticket']}, position {r['base']} BTC")
        expect("without the limit the keeper cancels the stop loss of a position that is there",
               r["cancelled"] == 1 and r["ticket"] in ("canceled", "deleted") and r["base"] == 0.05)

        ticket = self.stop_ticket("a new stop loss ticket", trader, account, self.post(self.stop_loss()))
        tb.wait(lambda: self.status(ticket) == "open", 20)
        say(f"  b) with --max-indexer-lag-secs {LAG_SECS}")
        r = self.standstill(trader, account, ticket, LAG_SECS)
        say(f"     {r['waited']:.0f} s later: cancellations sent {r['cancelled']:.0f}, lag shown {r['lag_ms']} ms, problems {r['problems']}")
        say(f"     alert posted: {r['alerts'][0][:200] if r['alerts'] else None}")
        say(f"     once the indexer runs again: ticket {r['ticket']}, position {r['base']} BTC, problems {r['problems_after']}")
        expect("with the limit the keeper says the indexer is behind", "indexer_lag" in r["problems"] and len(r["alerts"]) >= 1 and r["lag_ms"] > LAG_SECS * 1000)
        expect("and cancels nothing: the stop loss is still there for the position", r["cancelled"] == 0 and r["ticket"] == "open" and r["base"] == 0.05)
        expect("the problem is gone once the indexer has caught up", "indexer_lag" not in r["problems_after"])
        tb.wait(lambda: tickets().get("waiting", 0) >= 1, 10)
        expect("and the ticket waits for its price again", tickets().get("waiting", 0) >= 1)

    def twap(self, trader, account):
        say, expect = self.say, self.expect
        say("--- 3. TWAP tickets: made with no gas, of too many chunks, and as the front end makes them")
        self.restart_keeper()
        before = chunks_run()
        self.maker_order("the maker's asks", True, 300, 100_000)
        no_gas = self.twap_ticket("a TWAP ticket made with no gas", trader, account, self.twap_details(2, 2), 0)
        many = self.twap_ticket("a TWAP ticket of 121 chunks", trader, account, self.twap_details(121, 121), self.ticket_gas)
        tb.wait(lambda: tickets().get("twap_underfunded") == 1 and tickets().get("twap_too_many_chunks") == 1, 20)
        time.sleep(8)
        say(f"  8 s later: tickets {tickets()}, chunks run {chunks_run() - before:.0f}")
        expect("the keeper runs no chunk of either and counts them",
               tickets().get("twap_underfunded") == 1 and tickets().get("twap_too_many_chunks") == 1 and chunks_run() == before)
        expect("both tickets stay open, for their owner to cancel", self.status(no_gas) == "open" and self.status(many) == "open")
        expect("and neither is reported as a ticket that cannot fire", "stuck_tickets" not in problems())

        funded = self.twap_ticket("a TWAP ticket of two chunks with the front end's gas", trader, account, self.twap_details(2, 2), self.ticket_gas)
        tb.wait(lambda: self.status(funded) not in ("open", ""), 60)
        say(f"  the ticket with the front end's gas: {self.status(funded)}, chunks run {chunks_run() - before:.0f}")
        expect("it runs its two chunks and is finalized", self.status(funded) == "finalized" and chunks_run() - before == 2)

    def reading(self):
        say, expect = self.say, self.expect
        say(f"--- 4. the indexer's database stops answering, the keeper's own still does (/health fails {READING_SECS} s after the first failed round)")
        before = len(ALERTS)
        self.psql("postgres", f"ALTER DATABASE {tb.INDEXER_DB} WITH ALLOW_CONNECTIONS false")
        self.psql("postgres", f"SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity WHERE datname = '{tb.INDEXER_DB}'")
        stopped = time.time()
        try:
            tb.wait(lambda: "reading" in problems(), 40)
            code, _ = get("/health", 15)
            problem = problems().get("reading", {})
            say(f"  {time.time() - stopped:.0f} s in: /health {code}, the reading problem is {problem.get('level')}: {str(problem.get('message'))[:120]}")
            expect("at first the failure is a warning and the keeper is still healthy", code == 200 and str(problem.get("level")).lower() == "warning")
            tb.wait(lambda: get("/health", 15)[0] == 503, READING_SECS + 40)
            code, body = get("/health", 15)
            say(f"  {time.time() - stopped:.0f} s in: /health {code} {body[:220]}")
            expect("past the wait /health fails and says why", code == 503 and "reading" in body)
            tb.wait(lambda: any("could not be read" in a for a in ALERTS[before:]), 10)
            posted = [a for a in ALERTS[before:] if "could not be read" in a]
            say(f"  alert posted: {posted[0][:200] if posted else None}")
            expect("and an alert is posted", len(posted) >= 1)
        finally:
            self.psql("postgres", f"ALTER DATABASE {tb.INDEXER_DB} WITH ALLOW_CONNECTIONS true")
        tb.wait(lambda: get("/health", 15)[0] == 200, 60)
        say(f"  with the database back: /health {get('/health', 15)[0]}, problems {list(problems())}")
        expect("with the database back the keeper is healthy again", get("/health", 15)[0] == 200 and "reading" not in problems())

    def run(self):
        self.s.assert_local()
        say = self.say
        phases = self.args.phase or PHASES
        say(f"=== what the keeper says and holds back ({', '.join(phases)}) ===")
        self.restart_keeper()
        trader = self.new_address("trader")
        self.fund(trader, 5000)
        account = self.create_account(trader, deposit_coin=self.tusd_coins(trader)[0][0])
        self.open_position(account, trader, allocate=3000)
        self.trade(trader, account, False, 50)
        tb.wait(lambda: self.base(account) != 0, 20)
        say(f"trader account #{account['id']}: long {self.base(account)} BTC at 100,000")
        ticket = None
        if "stuck" in phases:
            ticket = self.stuck(trader, account)
        if "lag" in phases:
            if ticket is None:
                ticket = self.stop_ticket("a stop loss ticket", trader, account, self.post(self.stop_loss()))
                tb.wait(lambda: self.status(ticket) == "open", 20)
            self.lag(trader, account, ticket)
        if "twap" in phases:
            self.twap(trader, account)
        if "reading" in phases:
            self.reading()
        say("=== " + ("all as expected" if not self.failed else f"{len(self.failed)} FAILED: " + "; ".join(self.failed)) + " ===")
        return not self.failed


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("step", choices=["up", "run", "down"])
    parser.add_argument("--work", required=True, help="a scratch directory; everything this script writes is inside it")
    parser.add_argument("--perp-dex", default="~/perp-dex", help="the engine checkout, read only")
    parser.add_argument("--web", default="~/perp-dex-web", help="the front end checkout, for its localnet fixture and node modules, read only")
    parser.add_argument("--haneul", help="the CLI binary; <perp-dex>/deps/bin/haneul by default")
    parser.add_argument("--node", default="node", help="a Node 22 binary")
    parser.add_argument("--postgres-bin", help="the directory of initdb, pg_ctl, createdb and psql; the one of initdb on PATH by default")
    parser.add_argument("--pg-port", type=int, default=55432, help="the port of this script's own PostgreSQL cluster")
    parser.add_argument("--indexer", default="~/perp-indexer/perp-indexer/target/debug/perp-indexer", help="the perp-indexer binary")
    parser.add_argument("--keeper", default=str(tb.REPO / "target/debug/perp-keeper"), help="the perp-keeper binary")
    parser.add_argument("--sink-port", type=int, default=18999, help="the port this script takes the keeper's alerts on")
    parser.add_argument("--phase", action="append", choices=PHASES, help="a phase to run; all four by default")
    args = parser.parse_args()
    stack = md.Stack(args)
    if args.step == "up":
        tb.up(stack, args)
        return
    if args.step == "down":
        tb.down(stack, args)
        return
    if not stack.pids_file.exists():
        sys.exit(f"no {stack.pids_file}: bring the stack up first")
    if md.port_open(args.sink_port):
        sys.exit(f"port {args.sink_port} is taken; pass --sink-port")
    server = HTTPServer(("127.0.0.1", args.sink_port), Sink)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    ok = Check(stack, tb.Postgres(args, stack.work), args).run()
    (stack.logs / "keeper-watch-alerts.txt").write_text("\n".join(ALERTS) + "\n")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
