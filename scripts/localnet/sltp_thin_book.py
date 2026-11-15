#!/usr/bin/env python3
# Copyright (c) 2026 Geunhwa Jeong
# SPDX-License-Identifier: Apache-2.0

"""Check on a local network how the keeper closes a position at its stop loss on a thin book.

    scripts/localnet/sltp_thin_book.py up   --work <new scratch directory> --postgres-bin <dir>
    scripts/localnet/sltp_thin_book.py run  --work <the same directory> --postgres-bin <dir>
    scripts/localnet/sltp_thin_book.py down --work <the same directory> --postgres-bin <dir>

A stop loss / take profit ticket commits to an order. The front end used to make it a market
order with no price, both levels in one ticket; since perp-web d4d08f42 it makes a ticket per
level, an immediate-or-cancel order at the level's worst price. `run` hands the keeper the details
of both kinds, as the front end would, for a long of 0.05 BTC whose stop loss the index has
crossed, with bids for 0.02 BTC:

1. the old ticket: the engine fails a market order that does not fill in full, so nothing closes,
   and the keeper counts its failed tries;
2. the new tickets: the stop loss closes the 0.02 BTC the book has, at the bid, and the take
   profit ticket stays;
3. a second stop loss for the 0.03 BTC left, with the only bid under its worst price: nothing
   sells and the ticket stays; with a bid within the price the keeper's next try closes the rest
   there;
4. the take profit ticket, left without a position, is canceled by the keeper.

`up` builds mark_drag.py's stack in the work directory and adds what the keeper needs: a
PostgreSQL cluster of its own on --pg-port, the indexer, and the keeper on the fixture's key.
`down` stops all of it. The work directory has to be a new one each time.

On 2026-11-15, with the engine at 2d353bc and the keeper at 9d01896: 1 kept the whole 0.05 BTC
through 15 s and seven failed tries; 2 closed 0.02 BTC at 98,900 within seconds; 3 sold nothing at
93,000 under a worst price of 94,050, then closed 0.03 BTC at 98,800; 4 was canceled 16 s after
the position closed. The keeper refuses a try that would not fill before it sends it, so the
failed tries cost no gas.

Needs grpcurl, Node 22, PostgreSQL's binaries, and built perp-indexer and perp-keeper binaries.
Refuses to run anywhere but a local chain.
"""

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent))
import first_cap as fcap  # noqa: E402
import mark_drag as md  # noqa: E402
from mark_drag import B9, LOT, ONE, call, obj  # noqa: E402

REPO = Path(__file__).resolve().parents[2]
KEEPER_PORT, INDEXER_METRICS_PORT = 9191, 9184
KEEPER = f"http://127.0.0.1:{KEEPER_PORT}"
INDEXER_DB, DETAILS_DB = "perp_indexer_thin_book", "sigma_keeper_thin_book"
INDEXER_PACKAGES = ["perpetuals", "perpetuals_orders", "oracle_aggregator", "market_making_vault", "perpetuals_fees", "oracle_haneul"]
ORPHAN_SECS = 15
STOP, TAKE = 99_000, 105_000
WORST_STOP, WORST_TAKE = 94_050, 99_750  # 5% under each trigger, as the front end computes on BTC-USD


class Postgres:
    """A cluster inside the work directory, reached over TCP: the directory's path is too long for a socket."""

    def __init__(self, args, work):
        found = shutil.which("initdb")
        self.bin = Path(args.postgres_bin).expanduser() if args.postgres_bin else (Path(found).parent if found else None)
        if self.bin is None or not (self.bin / "initdb").exists():
            sys.exit("no PostgreSQL binaries on PATH; pass --postgres-bin")
        self.port = args.pg_port
        self.data = work / "pg"
        self.log = work / "logs/postgres.log"

    def url(self, db):
        return f"postgres://tester@127.0.0.1:{self.port}/{db}"

    def tool(self, name, *args):
        p = subprocess.run([str(self.bin / name), *args], capture_output=True, text=True)
        if p.returncode != 0:
            sys.exit(f"{name} failed:\n{(p.stdout + p.stderr)[-1500:]}")
        return p.stdout

    def start(self):
        self.tool("initdb", "-D", str(self.data), "-U", "tester", "--auth=trust", "-E", "UTF8", "--no-locale")
        self.tool("pg_ctl", "-D", str(self.data), "-o", f"-p {self.port} -c listen_addresses=127.0.0.1 -c unix_socket_directories=''",
                  "-l", str(self.log), "-w", "start")
        for db in (INDEXER_DB, DETAILS_DB):
            self.tool("createdb", "-h", "127.0.0.1", "-p", str(self.port), "-U", "tester", db)

    def stop(self):
        if (self.data / "postmaster.pid").exists():
            self.tool("pg_ctl", "-D", str(self.data), "-m", "fast", "stop")

    def sql(self, query):
        return self.tool("psql", "-h", "127.0.0.1", "-p", str(self.port), "-U", "tester", "-d", INDEXER_DB, "-tAc", query).strip()


def wait(check, seconds):
    deadline = time.time() + seconds
    while time.time() < deadline:
        value = check()
        if value:
            return value
        time.sleep(0.5)
    return check()


def healthy():
    try:
        with urllib.request.urlopen(KEEPER + "/health", timeout=2) as r:
            return r.status == 200
    except Exception:  # noqa: BLE001
        return False


# ---------------------------------------------------------------- up and down


def up(stack, args):
    for what, port in {"the keeper": KEEPER_PORT, "the indexer's metrics": INDEXER_METRICS_PORT, "PostgreSQL": args.pg_port}.items():
        if md.port_open(port):
            sys.exit(f"port {port} is taken; {what} of this script needs it")
    indexer, keeper = Path(args.indexer).expanduser(), Path(args.keeper).expanduser()
    for binary in (indexer, keeper):
        if not binary.exists():
            sys.exit(f"no {binary}; build it or pass its path")
    pg = Postgres(args, stack.work)
    md.up(stack)
    pids = json.loads(stack.pids_file.read_text())
    pg.start()

    state = json.loads(stack.state_file.read_text())
    packages = [x for name in INDEXER_PACKAGES for x in ("--package", f"{name}={state['P'][name]}")]
    with open(stack.logs / "indexer.log", "w") as out:
        pids["indexer"] = subprocess.Popen(
            [str(indexer), "--rpc-api-url", "http://127.0.0.1:9000", "--metrics-address", f"127.0.0.1:{INDEXER_METRICS_PORT}", *packages],
            stdout=out, stderr=subprocess.STDOUT, env=dict(os.environ, DATABASE_URL=pg.url(INDEXER_DB)), cwd=stack.work, start_new_session=True,
        ).pid
    stack.pids_file.write_text(json.dumps(pids))
    if not wait(lambda: pg.sql("SELECT to_regclass('order_tickets') IS NOT NULL") == "t", 60):
        sys.exit(f"the indexer made no tables; see {stack.logs / 'indexer.log'}")

    # The fixture named this key's address as the tickets' executor when it wrote the deployment.
    address = stack.fixture("keeper").strip().splitlines()[-1]
    p = subprocess.run(["grpcurl", "-plaintext", "-max-time", "10", md.NODE, "haneul.rpc.v2.LedgerService/GetServiceInfo"], capture_output=True, text=True)
    chain_id = json.loads(p.stdout)["chainId"]
    with open(stack.logs / "keeper.log", "w") as out:
        pids["keeper"] = subprocess.Popen(
            [str(keeper), "--details-database-url", pg.url(DETAILS_DB), "--database-url", pg.url(INDEXER_DB),
             "--deployment", str(stack.fixture_dir / "public/configs/haneul/perp.localnet.json"),
             "--rpc-url", "http://127.0.0.1:9000", "--chain-id", chain_id,
             "--key-file", str(stack.fixture_dir / ".localnet/keeper.key"), "--address", address,
             "--oracle-updates-url", "http://127.0.0.1:8787/v1/updates", "--listen-address", f"127.0.0.1:{KEEPER_PORT}",
             "--poll-interval-ms", "500", "--match-interval-secs", "1", "--min-gas-balance", "1",
             "--cancel-orphans-after-secs", str(ORPHAN_SECS), "--max-slippage-bps", "500"],
            stdout=out, stderr=subprocess.STDOUT, env=dict(os.environ, RUST_LOG="info,perp_keeper=debug", NO_COLOR="1"),
            cwd=stack.work, start_new_session=True,
        ).pid
    stack.pids_file.write_text(json.dumps(pids))
    if not wait(healthy, 30):
        sys.exit(f"the keeper did not come up; see {stack.logs / 'keeper.log'}")
    print(f"up: the indexer and the keeper ({address[:10]}) run on the stack", flush=True)


def down(stack, args):
    md.down(stack)
    Postgres(args, stack.work).stop()
    print("PostgreSQL stopped")


# ---------------------------------------------------------------- the check


class Check(fcap.Check):
    """first_cap's accounts and sessions, with the tickets and the reads of this check."""

    def __init__(self, s, pg):
        super().__init__(s)
        self.out.close()
        self.out = open(s.logs / "sltp-thin-book-results.txt", "a")
        self.pg = pg
        config = json.loads((s.fixture_dir / "public/configs/haneul/perp.localnet.json").read_text())
        self.keeper_address = config["ticketExecutors"][0]

    # ------------------------------------------------------------ tickets, as the front end makes them

    def details(self, lots, stop=None, take=None, worst=None):
        """What the front end hands the keeper. Without `worst`: the market order it made before."""
        d = {"clearingHouse": self.ch, "isLimitOrder": worst is not None, "triggerPriceType": 0, "positionIsAsk": False,
             "size": str(lots * LOT), "price": str((worst or 0) * B9), "orderType": "3" if worst is not None else "0",
             "salt": os.urandom(32).hex()}
        if stop:
            d["stopLossPrice"] = str(stop * ONE)
        if take:
            d["takeProfitPrice"] = str(take * ONE)
        return {"kind": "stopLossTakeProfit", "details": d}

    def ticket(self, label, trader, account, body):
        """Hands the details in, then creates the ticket on the commitment the keeper answers with."""
        req = urllib.request.Request(KEEPER + "/v1/details", data=json.dumps(body).encode(), headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=5) as r:
            commitment = bytes.fromhex(json.loads(r.read())["commitment"])
        cmds = ["--split-coins", "gas", f"[{self.ticket_gas}]", "--assign", "g"]
        cmds += ["--make-move-vec", "<address>", f"[{obj(self.keeper_address)}]", "--assign", "executors"]
        cmds += call("0x1::option::none", ["address"]) + ["--assign", "no_domain"]
        cmds += ["--make-move-vec", "<u8>", fcap.vec_u8(commitment), "--assign", "commitment"]
        cmds += call(f"{self.orders}::stop_orders::create_stop_order_ticket", [self.tusd, self.admin], obj(account["obj"]), obj(account["cap"]),
                     obj(self.registry), "executors", "no_domain", "g.0", "0u64", "commitment")
        j, _ = self.ptb(label, cmds, sender=trader)
        return self.events(j, "::events::CreatedStopOrderTicket")[0]["ticket_id"]

    def cancel_ticket(self, trader, account, ticket_id):
        cmds = call("0x2::object::id_from_address", [], obj(ticket_id)) + ["--assign", "ticket"]
        cmds += call(f"{self.orders}::stop_orders::cancel", [self.tusd, self.admin], obj(account["obj"]), obj(account["cap"]), obj(self.registry), "ticket")
        cmds += ["--assign", "refund", "--transfer-objects", "[refund]", obj(trader)]
        self.ptb("cancel the ticket", cmds, sender=trader)

    # ------------------------------------------------------------ reads: the indexer's tables and the keeper's log

    def status(self, ticket_id):
        return self.pg.sql(f"SELECT status FROM order_tickets WHERE ticket_id = '{ticket_id}'")

    def base(self, account):
        value = self.pg.sql(f"SELECT base FROM positions WHERE account_id = {account['id']}")
        return float(value) if value else 0.0

    def sales(self, account):
        """The account's sales as (price, size), oldest first."""
        rows = self.pg.sql(f"SELECT price, size FROM fills WHERE account_id = {account['id']} AND is_ask ORDER BY checkpoint, tx_index, event_index, fill_index")
        return [tuple(float(x) for x in row.split("|")) for row in rows.splitlines() if row]

    def failures(self, ticket_id):
        """The keeper's count of tries in a row that failed on the ticket, from its log."""
        log = (self.s.logs / "keeper.log").read_text(errors="replace")
        counts = [int(m.group(1)) for line in log.splitlines() if ticket_id in line for m in [re.search(r"failures=(\d+)", line)] if m]
        return max(counts, default=0)

    # ------------------------------------------------------------ the book and the price

    def maker_order(self, label, is_ask, lots, price):
        """One resting order of the fixture's maker account. Returns its ID."""
        j, _ = self.session(label, self.maker, [self.order(is_ask, lots, price, 0)])
        return self.events(j, "::events::PostedOrder")[0]["order_id"]

    def set_price(self, usd):
        self.s.fixture("price", str(usd))
        time.sleep(5)
        self.say(f"the price service signs BTC/USD at {usd:,}")

    def run(self):
        self.s.assert_local()
        if not healthy():
            sys.exit(f"no keeper on {KEEPER}: bring the stack up with this script")
        say, expect = self.say, self.expect
        say("=== a stop loss at 99,000 on a long of 0.05 BTC, with bids for 0.02 BTC ===")
        trader = self.new_address("trader")
        self.fund(trader, 5000)
        account = self.create_account(trader, deposit_coin=self.tusd_coins(trader)[0][0])
        self.open_position(account, trader, allocate=2000)
        self.maker_order("the maker's ask", True, 50, 100_000)
        self.session_as("the trader buys", account, account["cap"], self.admin, [self.order(False, 50, 100_000, 3)], trader)
        wait(lambda: self.base(account) != 0, 20)
        say(f"trader account #{account['id']}: long {self.base(account)} BTC at 100,000, ten times")
        self.maker_order("the maker's bid", False, 20, 98_900)
        say("the book: one bid, 0.02 BTC at 98,900")

        say("--- 1. before: one ticket, a market order with no price")
        old = self.ticket("the old ticket", trader, account, self.details(50, stop=STOP, take=TAKE))
        wait(lambda: self.status(old) == "open", 20)
        self.set_price(98_950)
        time.sleep(15)
        say(f"  15 s under the stop loss: ticket {self.status(old)}, position {self.base(account)} BTC, the keeper's failed tries {self.failures(old)}")
        expect("the market order closes none of the position: the book cannot fill all of it", self.base(account) == 0.05 and self.status(old) == "open")
        self.cancel_ticket(trader, account, old)

        say(f"--- 2. after: a ticket per level, immediate-or-cancel at the worst price ({WORST_STOP:,} for the stop loss)")
        stop = self.ticket("the stop loss ticket", trader, account, self.details(50, stop=STOP, worst=WORST_STOP))
        take = self.ticket("the take profit ticket", trader, account, self.details(50, take=TAKE, worst=WORST_TAKE))
        wait(lambda: self.status(stop) == "executed", 30)
        wait(lambda: self.base(account) != 0.05, 10)
        say(f"  stop loss ticket {self.status(stop)}, take profit ticket {self.status(take)}, position {self.base(account)} BTC, sales {self.sales(account)}")
        expect("the stop loss closes what the book has: 0.02 BTC at 98,900",
               self.status(stop) == "executed" and abs(self.base(account) - 0.03) < 1e-9 and self.sales(account)[-1:] == [(98_900.0, 0.02)])
        expect("the take profit ticket is still there for what is left", self.status(take) == "open")

        say("--- 3. a stop loss for the 0.03 BTC left, with the only bid under its worst price")
        low = self.maker_order("a bid under the worst price", False, 30, 93_000)
        again = self.ticket("the second stop loss ticket", trader, account, self.details(30, stop=STOP, worst=WORST_STOP))
        wait(lambda: self.status(again) == "open", 20)
        time.sleep(15)
        say(f"  15 s with a bid at 93,000 only: ticket {self.status(again)}, position {self.base(account)} BTC, the keeper's failed tries {self.failures(again)}")
        expect(f"nothing sells under {WORST_STOP:,}, and the ticket stays", self.status(again) == "open" and abs(self.base(account) - 0.03) < 1e-9)
        self.maker_order("a bid within the worst price", False, 30, 98_800)
        wait(lambda: self.status(again) == "executed", 60)
        wait(lambda: self.base(account) == 0, 10)
        say(f"  with a bid at 98,800: ticket {self.status(again)}, position {self.base(account)} BTC, sales {self.sales(account)}")
        expect("the keeper's next try closes the rest at 98,800, not at 93,000",
               self.status(again) == "executed" and self.base(account) == 0 and self.sales(account)[-1:] == [(98_800.0, 0.03)])

        say("--- 4. the take profit ticket, left without a position")
        wait(lambda: self.status(take) != "open", ORPHAN_SECS + 45)
        say(f"  take profit ticket {self.status(take)}")
        expect("the keeper cancels the ticket left without its position", self.status(take) in ("canceled", "deleted"))

        self.ptb("the maker cancels its low bid", ["--make-move-vec", "<u128>", f"[{low}u128]", "--assign", "ids"] + call(
            f"{self.ch_mod}::cancel_orders", [self.tusd, self.admin], obj(self.ch), obj(self.maker["cap"]), obj(self.maker["obj"]), "ids"))
        self.set_price(100_000)
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
    parser.add_argument("--keeper", default=str(REPO / "target/debug/perp-keeper"), help="the perp-keeper binary")
    args = parser.parse_args()
    stack = md.Stack(args)
    if args.step == "up":
        up(stack, args)
    elif args.step == "down":
        down(stack, args)
    else:
        if not stack.pids_file.exists():
            sys.exit(f"no {stack.pids_file}: bring the stack up first")
        sys.exit(0 if Check(stack, Postgres(args, stack.work)).run() else 1)


if __name__ == "__main__":
    main()
