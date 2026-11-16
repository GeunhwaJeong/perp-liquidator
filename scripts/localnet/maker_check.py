#!/usr/bin/env python3
# Copyright (c) 2026 Geunhwa Jeong
# SPDX-License-Identifier: Apache-2.0

"""Check on a local network what the maker does when a round cannot go through as it is.

    scripts/localnet/maker_check.py up   --work <new scratch directory>
    scripts/localnet/maker_check.py run  --work <the same directory> [--phase <name>]
    scripts/localnet/maker_check.py down --work <the same directory>

`run` gives the maker a key and an account of its own, starts it anew for each phase (`--phase`
picks some of them), moves the price the fixture's service signs, and checks four things:

1. `cross`: an account rests one lot inside the spread, and the price moves 0.1% toward it. The
   maker leaves the level that would match out of its round and posts the rest, and no round
   fails. With the order gone ten levels rest again. Stopping the maker pulls its quotes.
2. `stale`: the same order, then a move of 0.5%. The maker's bids are under the new price within
   seconds, and an account that then sells 0.131 BTC down to that price finds the resting lot
   and nothing of the maker's.
3. `failing`: the price moves 10% at once. The signed source takes 0.5% at once and 0.5% more
   for each second its price has stood, so every round fails for some twenty seconds. The maker
   pulls its quotes at the first failure, tries again at the pace of its rounds, fails /health
   and posts one alert once the failures have lasted --failing-alert-after-secs, and quotes
   again, with one more alert, when a round goes through.
4. `answer`: the maker reaches the node through a proxy of this script, which cuts the
   connection when the round is sent to be executed. With the transaction passed on to the node
   first, the maker takes over at its next poll what the round posted, and ten orders rest, not
   twenty. With the transaction dropped, the quotes of before stay and the next try goes
   through.

The maker's orders are read from its position on chain, and what it says from /status, /health
and /metrics, so the check needs no indexer.

`up` and `down` are mark_drag.py's: a stack of its own inside the work directory, which has to be
a new directory each time. `down` also stops a maker this script left running. `run` can be
repeated on a stack: the accounts are kept in the work directory (`maker-check-actors.json`), and
an order that a run left resting is cancelled by the next. A maker that loses track of its
orders leaves them on its position, which counts them after they have expired too. After a run
of such a maker, delete the file, and the next run opens accounts of its own.

On 2026-11-16, with the engine at 2d353bc: the maker at 944a836 passed the 20 checks of the
four phases in about two minutes. The maker at 1537a7b, which has to be started with
`--maker-arg=--max-failures --maker-arg=3` because it does not know the newer option, failed 14
of them. In 1 it failed three times at the first round that would have matched and stopped for
a minute. In 2 its bids stayed where they were, and the sale took 0.130 BTC of them. In 3 it
tried three times within two seconds, its quotes rested through the failures, and it quoted
again 70 s after the move. In 4 twenty of its orders rested after the round that ran, and thirty
after the one that did not.

Needs grpcurl, Node 22 and a built perp-maker binary. Refuses to run anywhere but a local chain.
"""

import argparse
import asyncio
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
from mark_drag import ONE, call, obj  # noqa: E402

REPO = Path(__file__).resolve().parents[2]
PHASES = ["cross", "stale", "failing", "answer"]
MAKER_PORT = 9190
MAKER = f"http://127.0.0.1:{MAKER_PORT}"
# How long rounds fail in this check before the maker makes a critical problem of it, in
# seconds: under the twenty or so that the 10% move of `failing` is refused for.
FAILING_SECS = 6
# The maker's default: five levels a side.
LEVELS = 10
# A request of the maker of this size or more carries a transaction: the round, to be simulated
# and then to be executed.
LARGE = 1_000

ALERTS = []


class Sink(BaseHTTPRequestHandler):
    """The alert webhook: keeps what the maker posts."""

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


class Proxy(threading.Thread):
    """A TCP proxy in front of the node that cuts the maker's connection around one request.

    Armed, it counts the maker's large requests and acts on the second, which is the round sent
    to be executed. `after`: the request goes on to the node and the maker's side of the
    connection is closed at once, while the node's side stays open, so that the node runs the
    transaction and its answer goes nowhere. `before`: the request is dropped and both sides are
    closed.
    """

    def __init__(self, listen, target):
        super().__init__(daemon=True)
        self.listen, self.target = listen, target
        self.mode, self.seen, self.cuts = None, 0, []
        self.ready = threading.Event()

    def arm(self, mode):
        self.seen, self.mode = 0, mode

    def run(self):
        asyncio.run(self.serve())

    async def serve(self):
        server = await asyncio.start_server(self.connection, "127.0.0.1", self.listen)
        self.ready.set()
        async with server:
            await server.serve_forever()

    async def connection(self, maker_reader, maker_writer):
        try:
            node_reader, node_writer = await asyncio.open_connection("127.0.0.1", self.target)
        except OSError:
            maker_writer.close()
            return
        cut = {"maker": False}

        async def from_maker():
            try:
                while True:
                    data = await maker_reader.read(65536)
                    if not data:
                        break
                    if len(data) >= LARGE and self.mode is not None:
                        self.seen += 1
                        if self.seen >= 2:
                            mode, self.mode = self.mode, None
                            self.cuts.append((time.time(), mode))
                            if mode == "after":
                                cut["maker"] = True
                                node_writer.write(data)
                                await node_writer.drain()
                                maker_writer.close()
                                await asyncio.sleep(6)
                            break
                    node_writer.write(data)
                    await node_writer.drain()
            except ConnectionError:
                pass
            finally:
                maker_writer.close()
                node_writer.close()

        async def from_node():
            try:
                while True:
                    data = await node_reader.read(65536)
                    if not data:
                        break
                    if not cut["maker"]:
                        maker_writer.write(data)
                        await maker_writer.drain()
            except ConnectionError:
                pass
            finally:
                if not cut["maker"]:
                    maker_writer.close()
                    node_writer.close()

        await asyncio.gather(from_maker(), from_node(), return_exceptions=True)


def wait(check, seconds):
    deadline = time.time() + seconds
    while time.time() < deadline:
        value = check()
        if value:
            return value
        time.sleep(0.5)
    return check()


def get(path, timeout=3):
    """The status code and the body; 0 when the maker does not answer in time."""
    try:
        with urllib.request.urlopen(MAKER + path, timeout=timeout) as r:
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()
    except Exception as e:  # noqa: BLE001
        return 0, str(e)


def status():
    code, body = get("/status")
    return json.loads(body) if code == 200 else {}


def health():
    return get("/health")[0]


def metric(name, **labels):
    """The sum of a metric's samples that carry the labels."""
    _, text = get("/metrics")
    total = 0.0
    for line in text.splitlines():
        head, _, value = line.rpartition(" ")
        if (head == name or head.startswith(name + "{")) and all(f'{k}="{v}"' in head for k, v in labels.items()):
            total += float(value)
    return total


def rounds(result):
    return metric("perp_maker_rounds_total", result=result)


class Check(fcap.Check):
    """first_cap's accounts and transactions, with a maker this check starts itself."""

    def __init__(self, s, args):
        super().__init__(s)
        self.out.close()
        unused = s.logs / "first-cap-results.txt"
        if unused.exists() and unused.stat().st_size == 0:
            unused.unlink()
        self.out = open(s.logs / "maker-check-results.txt", "a")
        self.args = args
        self.pids = json.loads(s.pids_file.read_text())
        p = subprocess.run(["grpcurl", "-plaintext", "-max-time", "10", md.NODE, "haneul.rpc.v2.LedgerService/GetServiceInfo"], capture_output=True, text=True)
        self.chain_id = json.loads(p.stdout)["chainId"]
        self.maker_log = s.logs / "maker-check.log"
        self.actors_file = s.work / "maker-check-actors.json"
        self.fields = {}
        self.proxy = None
        self.process = None

    # ------------------------------------------------------------ the price

    def price(self):
        return int(json.loads(self.s.state_file.read_text())["btcPrice"]) // ONE

    def move(self, usd):
        """The fixture's service signs this price from its next round on, three seconds at most."""
        self.s.fixture("price", str(usd))

    # ------------------------------------------------------------ the maker

    def start_maker(self, through_proxy=False):
        s = self.s
        rpc = f"http://127.0.0.1:{self.args.proxy_port}" if through_proxy else "http://127.0.0.1:9000"
        with open(self.maker_log, "a") as out:
            out.write(f"\n===== the maker, started for `{self.phase}` =====\n")
            out.flush()
            self.process = subprocess.Popen(
                [str(Path(self.args.maker).expanduser()),
                 "--deployment", str(s.fixture_dir / "public/configs/haneul/perp.localnet.json"),
                 "--rpc-url", rpc, "--chain-id", self.chain_id,
                 "--key-file", str(s.cfg / "haneul.keystore"), "--address", self.bot["address"],
                 "--account", self.bot["obj"], "--account-cap", self.bot["cap"], "--market", "BTC-USD",
                 "--oracle-updates-url", "http://127.0.0.1:8787/v1/updates", "--listen-address", f"127.0.0.1:{MAKER_PORT}",
                 "--alert-webhook-url", f"http://127.0.0.1:{self.args.sink_port}/", "--alert-format", "discord", "--alert-repeat-secs", "5",
                 *self.args.maker_arg],
                stdout=out, stderr=subprocess.STDOUT, env=dict(os.environ, RUST_LOG="info", NO_COLOR="1"),
                cwd=s.work, start_new_session=True,
            )
        # `down` stops the maker this file names.
        self.pids["maker"] = self.process.pid
        s.pids_file.write_text(json.dumps(self.pids))
        if not wait(lambda: status().get("resting_orders") == LEVELS, 40):
            sys.exit(f"the maker did not quote its {LEVELS} levels; see {self.maker_log}")

    def stop_maker(self):
        """The maker pulls its quotes before it exits, after its port has closed: the process
        is waited for, and through it the proxy it may still be sending through."""
        if self.process is None or self.process.poll() is not None:
            return
        self.process.send_signal(signal.SIGTERM)
        try:
            self.process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            self.process.kill()

    def log(self):
        """What the maker logged since it was last started."""
        return self.maker_log.read_text(errors="replace").rsplit("===== the maker, started", 1)[-1]

    def best(self, side):
        prices = [float(level["price"]) for level in status().get("ladder", []) if level["side"] == side]
        return (max(prices) if side == "bid" else min(prices)) if prices else None

    # ------------------------------------------------------------ the chain

    def position(self, account):
        """The account's position: its size, and the orders of it that rest."""
        if account["id"] not in self.fields:
            self.fields[account["id"]] = self.position_field(account["id"])
        value = self.get_object(self.fields[account["id"]])["json"]["value"]
        return {
            "base": md.signed(value["base_asset_amount"]) / ONE,
            "bids": int(value["bids_quantity"]) / ONE,
            "asks": int(value["asks_quantity"]) / ONE,
            "orders": int(value["pending_orders"]),
        }

    def position_field(self, account_id):
        """The clearing house keeps each position in a field of its own, named by the account."""
        token = None
        while True:
            body = {"parent": self.ch, "page_size": 200, "read_mask": {"paths": ["field_id", "value_type"]}}
            if token:
                body["page_token"] = token
            reply = self.grpc("haneul.rpc.v2.StateService/ListDynamicFields", body)
            for field in reply.get("dynamicFields", []):
                if "::position::Position" not in field.get("valueType", ""):
                    continue
                name = self.get_object(field["fieldId"])["json"]["name"]
                if int(name["account_id"]) == account_id:
                    return field["fieldId"]
            token = reply.get("nextPageToken")
            if not token:
                sys.exit(f"account {account_id} has no position on the market")

    def on_chain(self):
        """How many of the maker's orders rest, as its position counts them."""
        return self.position(self.bot)["orders"]

    # ------------------------------------------------------------ the other accounts

    def actor(self, alias, tusd, allocate):
        address = self.new_address(alias)
        self.fund(address, tusd)
        account = self.create_account(address, deposit_coin=self.tusd_coins(address)[0][0])
        self.open_position(account, address, ten_times=False, allocate=allocate)
        return dict(account, address=address)

    def actors(self):
        """The maker's account and the two others, made once for the stack. An order of O1 that
        an earlier run left resting is cancelled, so that this run starts on an empty book."""
        if self.actors_file.exists():
            kept = json.loads(self.actors_file.read_text())
            self.bot, self.o1, self.o2 = kept["bot"], kept["o1"], kept["o2"]
            if kept.get("o1_order"):
                self.cancel_o1(kept["o1_order"])
            return
        self.bot = self.actor("maker", 300_000, 200_000)
        self.o1 = self.actor("o1", 5_000, 2_000)
        self.o2 = self.actor("o2", 50_000, 40_000)
        self.keep(None)

    def keep(self, o1_order):
        self.actors_file.write_text(json.dumps({"bot": self.bot, "o1": self.o1, "o2": self.o2, "o1_order": o1_order}))

    def rest_inside(self):
        """O1 rests one lot inside the spread: at the price, or a dollar under the maker's best
        ask when its inventory has skewed the ladder that far down."""
        at = min(self.price(), int(self.best("ask")) - 1)
        j, _ = self.session("O1's bid", self.o1, [self.order(False, 1, at, 0)], sender=self.o1["address"])
        posted = self.events(j, "::events::PostedOrder")
        if not posted:
            sys.exit(f"O1's bid at {at:,} did not rest")
        self.keep(posted[0]["order_id"])
        return at, int(posted[0]["order_id"])

    def cancel_o1(self, order_id):
        """Cancels O1's order when it still rests; an order that is gone is skipped."""
        cmds = ["--make-move-vec", "<u128>", f"[{order_id}u128]", "--assign", "ids"]
        cmds += call(f"{self.ch_mod}::try_cancel_orders", [self.tusd, self.admin], obj(self.ch), obj(self.o1["cap"]), obj(self.o1["obj"]), "ids")
        self.ptb("O1 cancels its bid", cmds, sender=self.o1["address"])
        self.keep(None)

    # ------------------------------------------------------------ the phases

    def cross(self):
        say, expect = self.say, self.expect
        say("--- cross: a level that would match is left out, and the rest of the round lands")
        self.start_maker()
        at, order_id = self.rest_inside()
        down = round(self.price() * 0.999)
        self.move(down)
        say(f"  O1 rests 0.001 BTC at {at:,}; the price moves to {down:,}")
        left = wait(lambda: next((r for r in status().get("recent_rounds", []) if r.get("left_out")), None), 50)
        time.sleep(2)
        st, resting = status(), self.on_chain()
        say(f"  round: {left}; /status: {st.get('resting_orders')} resting, {st.get('failures')} failures, problems {list(st.get('problems', {}))}; on chain {resting} orders")
        expect("a round leaves the level that would match out", bool(left) and left["outcome"] == "executed")
        expect("the other levels rest, on /status and on chain",
               bool(left) and st.get("resting_orders") == LEVELS - left["left_out"] == resting and st.get("quoting") is True)
        expect("no round failed", rounds("refused") + rounds("failed") == 0 and st.get("failures") == 0 and health() == 200)
        executed = rounds("executed")
        self.cancel_o1(order_id)
        # A round after the cancellation: the position counts expired orders too, so that ten
        # orders on chain alone would not say that ten are quoted.
        back = wait(lambda: rounds("executed") > executed and status().get("resting_orders") == LEVELS and self.on_chain() == LEVELS, 40)
        say(f"  with O1's bid gone: {status().get('resting_orders')} resting, on chain {self.on_chain()}")
        expect("with the order gone ten levels rest again", bool(back))
        self.stop_maker()
        gone = wait(lambda: self.on_chain() == 0, 10)
        say(f"  the maker stopped: on chain {self.on_chain()} orders")
        expect("stopping pulls the quotes", bool(gone))

    def stale(self):
        say, expect = self.say, self.expect
        say("--- stale: after a move the bids are not left above the price")
        self.start_maker()
        at, order_id = self.rest_inside()
        before = self.position(self.bot)["base"]
        down = round(self.price() * 0.995)
        executed = rounds("executed")
        self.move(down)
        moved = time.time()
        under = wait(lambda: rounds("executed") > executed and (self.best("bid") or down + 1) < down and status().get("quoting"), 20)
        took = time.time() - moved
        say(f"  O1 rests 0.001 BTC at {at:,}; the price moves to {down:,}; the maker's best bid {self.best('bid')} after {took:.1f} s")
        expect("the maker's bids are under the new price within 20 s", bool(under))
        j, text = self.session("O2 sells", self.o2, [self.order(True, 131, down, 3)], sender=self.o2["address"], may_abort=True)
        sold = sum(int(e["base_asset_delta_ask"]) for e in self.events(j, "::events::FilledTakerOrder")) / ONE if j else 0.0
        time.sleep(3)
        after = self.position(self.bot)["base"]
        say(f"  O2 sells 0.131 BTC down to {down:,}: {sold:g} BTC sold; the maker's position {before:g} -> {after:g}")
        expect("the sale finds the resting lot and nothing of the maker's", abs(after - before) < 1e-9 and sold <= 0.001 + 1e-9)
        self.cancel_o1(order_id)
        self.stop_maker()

    def failing(self):
        say, expect = self.say, self.expect
        say("--- failing: rounds that fail pull the quotes, are tried again, and are told of")
        self.start_maker()
        alerts = len(ALERTS)
        down = round(self.price() * 0.9)
        self.move(down)
        started = time.time()
        say(f"  the price moves to {down:,} at once")
        first = wait(lambda: rounds("refused") + rounds("failed") >= 1, 20)
        failed_at = time.time()
        # Rounds are three seconds apart: a second try may fall into this wait, a third may not.
        time.sleep(2.5)
        burst = rounds("refused") + rounds("failed")
        pulled = wait(lambda: status().get("resting_orders") == 0 and status().get("quoting") is False and self.on_chain() == 0, 10)
        say(f"  first failure after {failed_at - started:.1f} s, {burst:g} failed tries 2.5 s later; then /status: {status().get('resting_orders')} resting, "
            f"on chain {self.on_chain()} orders")
        expect("the round fails", bool(first))
        expect("a failed try is held to the pace of the rounds", burst <= 2)
        expect("the quotes are pulled at the first failure", bool(pulled))
        critical = wait(lambda: health() == 503 and "failing" in status().get("problems", {}), FAILING_SECS + 10)
        waited = time.time() - failed_at
        tries = rounds("refused") + rounds("failed")
        told = [a for a in ALERTS[alerts:] if "No round has gone through" in a]
        say(f"  {waited:.1f} s after the first failure: /health {health()}, {tries:g} failed tries, alert: {told[0][:110] if told else None}")
        expect(f"after {FAILING_SECS} s of failures /health fails and one alert is posted", bool(critical) and len(told) == 1)
        expect("the round is tried again while it fails", tries >= 3)
        again = wait(lambda: health() == 200 and status().get("resting_orders") == LEVELS and self.on_chain() == LEVELS, 90)
        over = wait(lambda: any("Rounds go through again" in a for a in ALERTS[alerts:]), 5)
        say(f"  {time.time() - started:.0f} s after the move: /health {health()}, {status().get('resting_orders')} resting, on chain {self.on_chain()}, "
            f"{rounds('refused') + rounds('failed'):g} failed tries in all")
        expect("the maker quotes again once a round goes through, and says so", bool(again) and bool(over))
        expect("the failure was posted once", len([a for a in ALERTS[alerts:] if "No round has gone through" in a]) == 1)
        self.stop_maker()

    def answer(self):
        say, expect = self.say, self.expect
        say("--- answer: a round that got no answer is asked for, and taken over when it ran")
        self.start_maker(through_proxy=True)

        up = round(self.price() * 1.0005)
        self.proxy.arm("after")
        self.move(up)
        cut = wait(lambda: len(self.proxy.cuts) == 1, 30)
        taken = wait(lambda: "The transaction that got no answer ran" in self.log() and status().get("unanswered") == 0, 15)
        time.sleep(3)
        st, resting = status(), self.on_chain()
        say(f"  executed but unanswered: /status {st.get('resting_orders')} resting, {st.get('unanswered')} unanswered, {st.get('failures')} failures; on chain {resting} orders")
        expect("the connection is cut with the round on its way to the node", bool(cut) and rounds("unknown") == 1)
        expect("the maker takes over the round that ran", bool(taken) and st.get("failures") == 0)
        expect("ten orders rest, not twenty", resting == LEVELS and st.get("resting_orders") == LEVELS)

        executed = rounds("executed")
        up = round(self.price() * 1.0005)
        self.proxy.arm("before")
        self.move(up)
        cut = wait(lambda: len(self.proxy.cuts) == 2, 30)
        fewest = LEVELS
        deadline = time.time() + 20
        while time.time() < deadline and rounds("executed") == executed:
            fewest = min(fewest, status().get("resting_orders", 0))
            time.sleep(0.5)
        time.sleep(2)
        st, resting = status(), self.on_chain()
        say(f"  never sent on: fewest resting meanwhile {fewest}; then {st.get('resting_orders')} resting, {st.get('unanswered')} unanswered; on chain {resting} orders")
        expect("the connection is cut before the round reaches the node", bool(cut) and rounds("unknown") == 2)
        expect("the quotes of before stay", fewest == LEVELS)
        expect("the next try goes through, and the maker still asks for the lost one",
               rounds("executed") > executed and resting == LEVELS and st.get("unanswered") == 1)
        self.stop_maker()

    def run(self):
        self.s.assert_local()
        say = self.say
        phases = self.args.phase or PHASES
        say(f"=== what the maker does when a round cannot go through ({', '.join(phases)}) ===")
        self.actors()
        say(f"maker account #{self.bot['id']}, O1 #{self.o1['id']}, O2 #{self.o2['id']}; the price is {self.price():,}")
        if "answer" in phases:
            self.proxy = Proxy(self.args.proxy_port, 9000)
            self.proxy.start()
            self.proxy.ready.wait(10)
        for phase in PHASES:
            if phase in phases:
                self.phase = phase
                getattr(self, phase)()
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
    parser.add_argument("--maker", default=str(REPO / "target/debug/perp-maker"), help="the perp-maker binary")
    parser.add_argument("--maker-arg", action="append", default=None,
                        help=f"an argument for the maker, as --maker-arg=<word>, once per word; `--failing-alert-after-secs {FAILING_SECS}` by default")
    parser.add_argument("--sink-port", type=int, default=18999, help="the port this script takes the maker's alerts on")
    parser.add_argument("--proxy-port", type=int, default=9001, help="the port of this script's proxy in front of the node")
    parser.add_argument("--phase", action="append", choices=PHASES, help="a phase to run; all four by default")
    args = parser.parse_args()
    if args.maker_arg is None:
        args.maker_arg = ["--failing-alert-after-secs", str(FAILING_SECS)]
    stack = md.Stack(args)
    if args.step == "up":
        md.up(stack)
        return
    if args.step == "down":
        md.down(stack)
        return
    if not stack.pids_file.exists():
        sys.exit(f"no {stack.pids_file}: bring the stack up first")
    for what, port in (("the maker", MAKER_PORT), ("--sink-port", args.sink_port), ("--proxy-port", args.proxy_port)):
        if md.port_open(port):
            sys.exit(f"port {port} is taken; {what} needs it")
    server = HTTPServer(("127.0.0.1", args.sink_port), Sink)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    check = Check(stack, args)
    try:
        ok = check.run()
    finally:
        check.stop_maker()
        (stack.logs / "maker-check-alerts.txt").write_text("\n".join(ALERTS) + "\n")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
