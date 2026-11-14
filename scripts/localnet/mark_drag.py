#!/usr/bin/env python3
# Copyright (c) 2026 Geunhwa Jeong
# SPDX-License-Identifier: Apache-2.0

"""Measure how far two one-lot orders move the mark price from the index on a local network.

    scripts/localnet/mark_drag.py up   --work <new scratch directory>
    scripts/localnet/mark_drag.py run  --work <the same directory> [--quick]
    scripts/localnet/mark_drag.py down --work <the same directory>

The mark price is the median of the funding price, the index TWAP plus the spread TWAP, and the
book's mid price. The mid price is the mean of the best bid and the best ask whatever their
sizes, and the spread TWAP samples it clipped to `max_book_index_spread` around the index. With
the makers gone, two one-lot orders set the mid price, and the mark price follows it up to that
limit while the index does not move.

`up` builds a stack of its own inside the work directory and writes nothing outside it: a copy
of the engine sources, a copy of the front end's localnet fixture set to the mainnet funding
schedule and feed TWAP period and without the maker ladder, a CLI configuration of its own, a
local network, the engine published with a BTC/USD market at 100,000, and the fixture's price
relay, which keeps the index at that price.

`run` opens a victim account with a ten times short of 0.1 BTC, then:

A. limit 5%: rests a one-lot bid 6% and a one-lot ask 8% above the index, leaves the market
   alone for 65 seconds (the spread TWAP period is 60), calls `update_twaps` once, and reads
   the mark price and whether the victim can be liquidated;
B. limit 5%: the same orders with `update_twaps` every few seconds, for the time the mark
   price takes on a market that is sampled often;
C. limit 1%: the same as A, then 45 more seconds of sampling;
D. limit 5% again: the mark price is moved once more and the victim is liquidated for real.

`--quick` runs A and C only. The market ends at 5% with the orders canceled, but the victim
stays, so each `up` takes one `run`. `down` stops the network and the relay and leaves the work
directory with its logs.

On 2026-11-15, with the engine at 2d353bc: A moved the mark price to 104,999.92 with the index
at 100,000 and the liquidation would have gone through; B reached that point after 192 seconds;
C stopped at 100,999.98 and the liquidation was refused; D liquidated 0.06 BTC of the short at
104,999.94. Mainnet's limit was lowered from 5% to 1% that day.

Needs grpcurl and Node 22. Refuses to run anywhere but a local chain.
"""

import argparse
import base64
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

NODE = "127.0.0.1:9000"
PORTS = {"the local network": 9000, "the faucet": 9123, "the price relay": 8787}
MAINNET_CHAIN_ID = "a0053d9e"
ONE = 10**18
B9 = 10**9
LOT = 1_000_000  # 0.001 BTC
TUSD_UNIT = 10**6
QUIET_SECS = 65

# The fixture as the front end keeps it, changed in four places. Each must match exactly once,
# so that a fixture that has moved on fails here instead of building another market.
FIXTURE_EDITS = [
    # The feed TWAP period mainnet runs.
    ("const TWAP_PERIOD_MS = 1n;", "const TWAP_PERIOD_MS = 10_000n;"),
    # The funding schedule mainnet runs: hourly over eight hours.
    ("u64(60_000), u64(21_600_000)));", "u64(3_600_000), u64(28_800_000)));"),
    # The vendor capability and key, which `set_risk_limit_params` takes.
    (
        "    btcPrice: BTC0.toString(), maker: null,\n  };",
        "    btcPrice: BTC0.toString(), maker: null, perpVk, VK,\n  };",
    ),
    # The maker account without its ladder: the book starts empty.
    (
        "  const btc = BigInt(s.btcPrice) / ONE;\n  cmds = relayCommands(s, currentUpdates(signer, s));",
        "  if (process.env.NO_LADDER) return;\n"
        "  const btc = BigInt(s.btcPrice) / ONE;\n  cmds = relayCommands(s, currentUpdates(signer, s));",
    ),
]


def port_open(port):
    try:
        socket.create_connection(("127.0.0.1", port), timeout=1).close()
        return True
    except OSError:
        return False


def signed(v):
    """An ifixed value, which is a two's complement u256."""
    v = int(v)
    return v - (1 << 256) if v >= (1 << 255) else v


def usd(v):
    return f"{signed(v) / ONE:,.2f}"


def obj(i):
    return "@" + i


def call(target, type_args, *args):
    cmd = ["--move-call", target]
    if type_args:
        cmd.append("<" + ",".join(type_args) + ">")
    return cmd + list(args)


class Stack:
    """The paths and the environment of one work directory."""

    def __init__(self, args):
        self.work = Path(args.work).expanduser().resolve()
        self.perp_dex = Path(args.perp_dex).expanduser().resolve()
        self.web = Path(args.web).expanduser().resolve()
        self.haneul = str(Path(args.haneul).expanduser().resolve()) if args.haneul else str(self.perp_dex / "deps/bin/haneul")
        self.node = shutil.which(args.node) or sys.exit(f"no '{args.node}' on PATH; pass --node")
        self.cfg = self.work / "cfg"
        self.logs = self.work / "logs"
        self.fixture_dir = self.work / "web"
        self.state_file = self.fixture_dir / ".localnet/fixture-state.json"
        self.pids_file = self.work / "pids.json"
        self.env = dict(
            os.environ,
            HANEUL_CONFIG_DIR=str(self.cfg),
            HANEUL=self.haneul,
            PERP_DEX_ROOT=str(self.work / "perp-dex"),
            NO_LADDER="1",
            PATH=str(Path(self.node).parent) + os.pathsep + os.environ["PATH"],
        )

    def cli(self, *args):
        p = subprocess.run([self.haneul, "client", *args], capture_output=True, text=True, env=self.env, cwd=self.work)
        return p.stdout + p.stderr

    def fixture(self, *args, log=None):
        cmd = [self.node, "scripts/haneul-localnet/fixture.mjs", *args]
        if log:
            with open(log, "w") as out:
                return subprocess.run(cmd, stdout=out, stderr=subprocess.STDOUT, env=self.env, cwd=self.fixture_dir).returncode
        p = subprocess.run(cmd, capture_output=True, text=True, env=self.env, cwd=self.fixture_dir)
        if p.returncode != 0:
            sys.exit(f"fixture {' '.join(args)} failed:\n{(p.stdout + p.stderr)[-1500:]}")
        return p.stdout

    def assert_local(self):
        chain = (self.cli("chain-identifier").strip().split("\n") or [""])[-1].split()[-1:]
        if not chain or chain[0] == MAINNET_CHAIN_ID:
            sys.exit(f"chain '{chain}' is mainnet or unknown; this only runs on a local network")
        return chain[0]


# ---------------------------------------------------------------- up and down


def up(s):
    for what, port in PORTS.items():
        if port_open(port):
            sys.exit(f"port {port} is taken; {what} of this script needs it")
    if s.pids_file.exists():
        sys.exit(f"{s.pids_file} exists: this work directory is up already")
    major = int(subprocess.run([s.node, "--version"], capture_output=True, text=True).stdout.strip().lstrip("v").split(".")[0])
    if major < 22:
        sys.exit(f"{s.node} is Node {major}; the fixture needs Node 22")
    for d in (s.cfg, s.logs, s.work / "net", s.fixture_dir / "scripts/haneul-localnet"):
        d.mkdir(parents=True, exist_ok=True)

    # The engine sources, so that the build output lands in the work directory.
    def skip(directory, names):
        out = {n for n in names if n in (".git", "work", "tools", "build", "__pycache__") or n.startswith("Pub.")}
        if Path(directory).name == "deps":
            out.add("bin")
        return out

    shutil.copytree(s.perp_dex, s.work / "perp-dex", ignore=skip, dirs_exist_ok=True)

    # The fixture, which writes its state next to itself, with the node modules of the front end.
    source = (s.web / "scripts/haneul-localnet/fixture.mjs").read_text()
    for old, new in FIXTURE_EDITS:
        if source.count(old) != 1:
            sys.exit(f"the fixture no longer has exactly one of:\n{old}")
        source = source.replace(old, new)
    (s.fixture_dir / "scripts/haneul-localnet/fixture.mjs").write_text(source)
    link = s.fixture_dir / "node_modules"
    if not link.exists():
        link.symlink_to(s.web / "node_modules")

    # A CLI configuration that knows the local network only.
    (s.cfg / "haneul.keystore").write_text("[]\n")
    (s.cfg / "client.yaml").write_text(
        "---\n"
        f"keystore:\n  File: {s.cfg / 'haneul.keystore'}\n"
        'envs:\n  - alias: local\n    rpc: "http://127.0.0.1:9000"\n    ws: ~\n    basic_auth: ~\n'
        "active_env: local\nactive_address: ~\n"
    )

    pids = {}
    with open(s.logs / "localnet.log", "w") as out:
        pids["localnet"] = subprocess.Popen(
            [s.haneul, "start", "--with-faucet", "--force-regenesis"],
            stdout=out, stderr=subprocess.STDOUT, env=s.env, cwd=s.work / "net", start_new_session=True,
        ).pid
    s.pids_file.write_text(json.dumps(pids))
    deadline = time.time() + 120
    while not (port_open(9000) and port_open(9123)):
        if time.time() > deadline:
            sys.exit(f"the local network did not come up; see {s.logs / 'localnet.log'}")
        time.sleep(1)
    s.cli("new-address", "ed25519", "admin")
    s.cli("switch", "--address", "admin")
    print(f"local network up, chain {s.assert_local()}", flush=True)

    print("publishing the engine and opening the market (a few minutes)", flush=True)
    if s.fixture("setup", log=s.logs / "setup.log") != 0 or not s.state_file.exists():
        sys.exit(f"the fixture's setup failed; see {s.logs / 'setup.log'}")
    state = json.loads(s.state_file.read_text())
    if not state.get("maker") or not state.get("perpVk"):
        sys.exit(f"the fixture's setup left no maker account or vendor capability; see {s.logs / 'setup.log'}")

    with open(s.logs / "push.log", "w") as out:
        pids["relay"] = subprocess.Popen(
            [s.node, "scripts/haneul-localnet/fixture.mjs", "push"],
            stdout=out, stderr=subprocess.STDOUT, env=s.env, cwd=s.fixture_dir, start_new_session=True,
        ).pid
    s.pids_file.write_text(json.dumps(pids))
    deadline = time.time() + 30
    while True:
        try:
            with urllib.request.urlopen("http://127.0.0.1:8787/healthz", timeout=2) as r:
                if json.loads(r.read()).get("healthy"):
                    break
        except Exception:  # noqa: BLE001
            pass
        if time.time() > deadline:
            sys.exit(f"the price relay did not come up; see {s.logs / 'push.log'}")
        time.sleep(1)
    print(f"up: market {state['clearingHouse']}, index {int(state['btcPrice']) // ONE:,}", flush=True)


def down(s):
    if not s.pids_file.exists():
        sys.exit(f"no {s.pids_file}: nothing to stop")
    for name, pid in json.loads(s.pids_file.read_text()).items():
        try:
            os.killpg(pid, signal.SIGTERM)
            print(f"stopped {name} (pid {pid})")
        except ProcessLookupError:
            print(f"{name} (pid {pid}) was not running")
    deadline = time.time() + 20
    while any(port_open(p) for p in PORTS.values()) and time.time() < deadline:
        time.sleep(1)
    left = [str(p) for p in PORTS.values() if port_open(p)]
    s.pids_file.unlink()
    print("ports free" if not left else f"still listening: {', '.join(left)}")


# ---------------------------------------------------------------- the measurement


class Run:
    def __init__(self, s):
        self.s = s
        st = json.loads(s.state_file.read_text())
        self.perp, self.tusd, self.admin, self.vk = st["P"]["perpetuals"], st["TUSD"], st["ADMIN"], st["VK"]
        self.ch, self.registry = st["clearingHouse"], st["registry"]
        self.pfs_btc, self.pfs_tusd, self.perp_vk = st["pfsBtc"], st["pfsTusd"], st["perpVk"]
        self.me, self.maker = st["me"], st["maker"]
        self.index_usd = int(st["btcPrice"]) // ONE
        self.t0 = time.time()
        self.out = open(s.logs / "mark-drag-results.txt", "a")
        self.shared = {}

    def say(self, *a):
        line = f"[{time.time() - self.t0:6.1f}s] " + " ".join(str(x) for x in a)
        print(line, flush=True)
        self.out.write(line + "\n")
        self.out.flush()

    # ------------------------------------------------------------ transactions

    def _ptb(self, args, tries=4):
        """An epoch change or a version race is retried; a Move abort is the answer."""
        p = None
        for _ in range(tries):
            p = subprocess.run([self.s.haneul, "client", "ptb", *args], capture_output=True, text=True, env=self.s.env, cwd=self.s.work)
            text = p.stdout + p.stderr
            if p.returncode == 0 or "MoveAbort" in text or "aborted within function" in text:
                return p
            time.sleep(2)
        return p

    def ptb(self, label, cmds, sender=None, may_abort=False):
        args = list(cmds) + (["--sender", obj(sender)] if sender else [])
        p = self._ptb(args + ["--gas-budget", "2000000000", "--json"])
        text = p.stdout + p.stderr
        j = json.loads(p.stdout[p.stdout.find("{"):]) if p.returncode == 0 else None
        if j is None or j.get("effects", {}).get("status", {}).get("status") != "success":
            if may_abort:
                return None, text
            sys.exit(f"{label} failed:\n{text[-1500:]}")
        return j, text

    @staticmethod
    def events(j, suffix):
        return [e["parsedJson"] for e in (j.get("events") or []) if e["type"].split("<")[0].endswith(suffix)]

    def session(self, label, account, actions, pre=None, sender=None, may_abort=False, dry=False):
        ch_mod = f"{self.perp}::clearing_house"
        cmds = list(pre or [])
        cmds += call("0x1::option::none", [f"{self.perp}::account::IntegratorInfo"]) + ["--assign", "no_integrator"]
        cmds += call(f"{ch_mod}::start_session", [self.tusd, self.admin], obj(self.ch), obj(account["cap"]), obj(account["obj"]),
                     obj(self.pfs_btc), obj(self.pfs_tusd), "no_integrator", "@0x6") + ["--assign", "hp"]
        for action in actions:
            cmds += action
        cmds += call(f"{ch_mod}::end_session", [self.tusd, self.admin], "hp", obj(account["cap"]), obj(account["obj"]),
                     "false", "false") + ["--assign", "res"]
        cmds += call(f"{ch_mod}::share", [self.tusd], "res.0")
        if dry:
            args = list(cmds) + (["--sender", obj(sender)] if sender else [])
            p = self._ptb(args + ["--gas-budget", "2000000000", "--dry-run"])
            return p.stdout + p.stderr
        return self.ptb(label, cmds, sender=sender, may_abort=may_abort)

    def order(self, is_ask, lots, price_usd, order_type):
        """Order types: 0 good-till-cancel, 2 post-only, 3 immediate-or-cancel."""
        return call(f"{self.perp}::clearing_house::place_limit_order", [self.tusd], "hp", "true" if is_ask else "false",
                    f"{lots * LOT}u64", f"{price_usd * B9}u64", f"{order_type}u64", "none", "false", "none")

    def update_twaps(self):
        j, _ = self.ptb("update_twaps", call(f"{self.perp}::clearing_house::update_twaps", [self.tusd], obj(self.ch), obj(self.pfs_btc), "@0x6"))
        ev = self.events(j, "::events::UpdatedSpreadTwap")
        return ev[0] if ev else None

    def set_limit(self, bps):
        value = ONE * bps // 10_000
        j, _ = self.ptb("set the limit", call(
            f"{self.perp}::clearing_house::set_risk_limit_params", [self.vk, self.admin, self.tusd], obj(self.ch), obj(self.perp_vk),
            obj(self.registry), "none", "none", "none", "none", "none", f"some({value}u256)", "none", "none", "none", "none"))
        ev = self.events(j, "::events::SetRiskLimitParams")[0]
        self.say(f"max_book_index_spread set to {int(ev['max_book_index_spread']) / ONE * 100:.1f}%")

    def cancel(self, ids):
        vec = "[" + ", ".join(f"{i}u128" for i in ids) + "]"
        self.ptb("cancel", ["--make-move-vec", "<u128>", vec, "--assign", "ids"] + call(
            f"{self.perp}::clearing_house::cancel_orders", [self.tusd, self.admin], obj(self.ch), obj(self.maker["cap"]), obj(self.maker["obj"]), "ids"))

    # ------------------------------------------------------------ reads

    def grpc(self, method, body):
        p = subprocess.run(["grpcurl", "-plaintext", "-max-time", "20", "-d", "@", NODE, method], input=json.dumps(body), capture_output=True, text=True)
        if p.returncode != 0:
            sys.exit(f"{method} failed: {p.stderr[-600:]}")
        return json.loads(p.stdout)

    def get_object(self, i):
        return self.grpc("haneul.rpc.v2.LedgerService/GetObject", {"object_id": i, "read_mask": {"paths": ["json", "owner", "version"]}})["object"]

    def shared_version(self, i):
        if i not in self.shared:
            self.shared[i] = self.get_object(i)["owner"]["version"]
        return self.shared[i]

    def mark_price(self):
        """The engine's own mark price: its public view, simulated."""
        inputs = [{"kind": "SHARED", "object_id": i, "version": self.shared_version(i), "mutable": False} for i in (self.ch, self.pfs_btc)]
        inputs.append({"kind": "SHARED", "object_id": "0x6", "version": "1", "mutable": False})
        d = self.grpc("haneul.rpc.v2.TransactionExecutionService/SimulateTransaction", {
            "transaction": {"sender": self.me, "kind": {"kind": "PROGRAMMABLE_TRANSACTION", "programmable_transaction": {
                "inputs": inputs,
                "commands": [{"move_call": {"package": self.perp, "module": "clearing_house", "function": "mark_price",
                                            "type_arguments": [self.tusd], "arguments": [{"kind": "INPUT", "input": i} for i in range(3)]}}]}}},
            "do_gas_selection": True,
            "read_mask": {"paths": ["transaction.effects.status", "command_outputs"]}})
        raw = base64.b64decode(d["commandOutputs"][0]["returnValues"][0]["value"]["value"])
        return int.from_bytes(raw, "little")

    def state(self):
        j = self.get_object(self.ch)["json"]
        feed = self.get_object(self.pfs_btc)["json"]["feeds"][0]
        return {
            "index": int(feed["price"]),
            "spread_twap": signed(j["market_state"]["spread_twap"]),
            "premium_twap": signed(j["market_state"]["premium_twap"]),
            "best_bid": j["orderbook"]["best_bid_price"],
            "best_ask": j["orderbook"]["best_ask_price"],
            "limit": int(j["market_params"]["limits_params"]["max_book_index_spread"]),
        }

    def report(self, label):
        s, mark = self.state(), self.mark_price()
        self.say(f"{label}: index {s['index'] / ONE:,.2f}  mark {mark / ONE:,.2f} ({(mark - s['index']) / s['index'] * 100:+.3f}% from index)  "
                 f"spread_twap {s['spread_twap'] / ONE:+,.2f}  premium_twap {s['premium_twap'] / ONE:+,.2f}  limit {s['limit'] / ONE * 100:.1f}%")
        return mark

    # ------------------------------------------------------------ the actors

    def tusd_coin(self, address):
        """The address's TUSD coin. This CLI lists raw objects, whose ID is their first 32 bytes."""
        out = self.s.cli("objects", address, "--json")
        for o in json.loads(out[out.find("["):]):
            move = o["data"].get("Move", {})
            kind = move.get("type_")
            if isinstance(kind, dict) and kind.get("Coin", {}).get("struct", {}).get("name") == "TUSD":
                return "0x" + bytes(move["contents"][:32]).hex()
        sys.exit(f"{address} holds no TUSD")

    def open_victim(self):
        """A ten times short of 0.1 BTC at the index, on 1,010 TUSD of collateral."""
        out = self.s.cli("new-address", "ed25519", f"victim{int(time.time())}", "--json")
        address = json.loads(out[out.find("{"):])["address"]
        self.s.fixture("fund", address, "10000")
        account_mod, ch_mod = f"{self.perp}::account", f"{self.perp}::clearing_house"
        cmds = call(f"{account_mod}::create_account", [self.tusd], obj(self.registry)) + ["--assign", "acc"]
        cmds += call(f"{account_mod}::deposit_collateral", [self.tusd, self.admin], "acc.0", "acc.2", obj(self.registry), obj(self.tusd_coin(address)))
        cmds += call(f"{account_mod}::consume_policy_and_share_account", [self.tusd], "acc.0", "acc.1")
        cmds += ["--transfer-objects", "[acc.2]", obj(address)]
        j, _ = self.ptb("the victim's account", cmds, sender=address)
        created = self.events(j, "::events::CreatedAccount")[0]
        cap = next(c["objectId"] for c in j["objectChanges"]
                   if c["type"] == "created" and "AuthorityCap<" in c["objectType"] and "::authority::ACCOUNT" in c["objectType"])
        victim = {"obj": created["account_obj_id"], "cap": cap, "id": int(created["account_id"]), "address": address}
        # A new position starts at one time leverage: its own initial margin ratio sets ten times.
        cmds = call(f"{ch_mod}::create_market_position", [self.tusd, self.admin], obj(self.ch), obj(cap), obj(victim["obj"]))
        cmds += call(f"{ch_mod}::set_position_initial_margin_ratio", [self.tusd, self.admin], obj(self.ch), obj(cap), obj(victim["obj"]), f"{ONE // 10}u256")
        cmds += call(f"{ch_mod}::allocate_collateral", [self.tusd, self.admin], obj(self.ch), obj(cap), obj(victim["obj"]), f"{1_010 * TUSD_UNIT}u64")
        self.ptb("the victim's position", cmds, sender=address)
        self.session("a bid at the index", self.maker, [self.order(False, 100, self.index_usd, 0)])
        j, _ = self.session("the victim sells", victim, [self.order(True, 100, self.index_usd, 3)], sender=address)
        if not self.events(j, "::events::FilledTakerOrder"):
            sys.exit("the victim's sale did not fill")
        self.say(f"victim: account {victim['id']}, short 0.1 BTC at {self.index_usd:,}, ten times leverage, 1,010 TUSD allocated")
        return victim

    def liquidation(self, victim, real=False):
        pre = ["--make-move-vec", "<u128>", "[]", "--assign", "ids"]
        act = [call(f"{self.perp}::clearing_house::liquidate", [self.tusd], "hp", f"{victim['id']}u64", "ids")]
        if real:
            return self.session("liquidate", self.maker, act, pre=pre, may_abort=True)
        text = self.session("liquidate", self.maker, act, pre=pre, dry=True)
        return "would SUCCEED" if "execution status: success" in text else "would ABORT"

    def far_orders(self):
        bid, ask = self.index_usd * 106 // 100, self.index_usd * 108 // 100
        j, _ = self.session("the two orders", self.maker, [self.order(False, 1, bid, 2), self.order(True, 1, ask, 2)])
        self.say(f"rested a one-lot bid at {bid:,} and a one-lot ask at {ask:,} (0.001 BTC each)")
        return [e["order_id"] for e in self.events(j, "::events::PostedOrder")]

    def quiet(self, why):
        self.say(f"quiet for {QUIET_SECS} s ({why}); only the relay runs, and it writes the price feed, not the market")
        time.sleep(QUIET_SECS)

    def reset(self, ids):
        self.cancel(ids)
        self.quiet("the spread TWAP window passes with an empty book")
        ev = self.update_twaps()
        self.say(f"reset: spread_twap {usd(ev['spread_twap']) if ev else 'n/a'} after one update")

    def one_update(self, label, victim):
        self.quiet("the spread TWAP period is 60 s")
        ev = self.update_twaps()
        self.say(f"{label} one update_twaps: book {usd(ev['actual_book_price'])}, clipped {usd(ev['clipped_book_price'])}, spread_twap {usd(ev['spread_twap'])}")
        self.report(f"{label} after the update")
        self.say(f"{label} liquidating the victim: {self.liquidation(victim)}")

    def run(self, quick):
        self.s.assert_local()
        start = self.state()
        if start["best_bid"] is not None or start["best_ask"] is not None or start["limit"] != ONE * 5 // 100:
            sys.exit("the book is not empty or the limit is not 5%: this market has been run on; take it down and up again")
        self.say("=== two one-lot orders against the mark price ===")
        self.report("start")
        victim = self.open_victim()
        self.say(f"liquidating the victim now: {self.liquidation(victim)}")

        self.say("--- A: limit 5%, the market left alone for one TWAP window, then one update")
        ids = self.far_orders()
        self.report("A right after the orders")
        self.one_update("A", victim)

        if not quick:
            self.say("--- B: limit 5%, the TWAP sampled every few seconds")
            self.reset(ids)
            ids = self.far_orders()
            began, first, rounds = time.time(), None, 0
            while time.time() - began < 200:
                self.update_twaps()
                rounds += 1
                elapsed = time.time() - began
                if rounds % 5 == 0 or elapsed > 195:
                    self.report(f"B t={elapsed:5.1f}s")
                    answer = self.liquidation(victim)
                    self.say(f"B t={elapsed:5.1f}s liquidating the victim: {answer}")
                    if first is None and answer == "would SUCCEED":
                        first = elapsed
                time.sleep(1.5)
            self.say(f"B the liquidation would first succeed {'never in 200 s' if first is None else f'{first:.1f} s after the orders'}")

        self.say("--- C: limit 1%, the same as A")
        self.reset(ids)
        self.set_limit(100)
        ids = self.far_orders()
        self.one_update("C", victim)
        began = time.time()
        while time.time() - began < 45:
            self.update_twaps()
            time.sleep(1.5)
        self.report("C after 45 s more of sampling")
        self.say(f"C liquidating the victim: {self.liquidation(victim)}")

        self.set_limit(500)
        if not quick:
            self.say("--- D: limit 5% again, and the liquidation sent")
            self.quiet("the spread TWAP period is 60 s")
            ev = self.update_twaps()
            self.say(f"D one update_twaps: spread_twap {usd(ev['spread_twap'])}")
            self.report("D after the update")
            j, text = self.liquidation(victim, real=True)
            if j is None:
                self.say(f"D the liquidation FAILED: {text[-400:]}")
            for e in self.events(j or {}, "::events::LiquidatedPosition"):
                self.say(f"D LIQUIDATED: account {e['liqee_account_id']}, base {usd(e['base_liquidated'])} BTC at mark {usd(e['mark_price'])}, "
                         f"pnl {usd(e['liqee_pnl'])}, liquidation fees {usd(e['liquidation_fees'])}, "
                         f"insurance fees {usd(e['insurance_fund_fees'])}, bad debt {usd(e['bad_debt'])}")
        self.cancel(ids)
        self.report("end")
        self.say("=== done ===")


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("step", choices=["up", "run", "down"])
    parser.add_argument("--work", required=True, help="a scratch directory; everything this script writes is inside it")
    parser.add_argument("--perp-dex", default="~/perp-dex", help="the engine checkout, read only")
    parser.add_argument("--web", default="~/perp-dex-web", help="the front end checkout, for its localnet fixture and node modules, read only")
    parser.add_argument("--haneul", help="the CLI binary; <perp-dex>/deps/bin/haneul by default")
    parser.add_argument("--node", default="node", help="a Node 22 binary")
    parser.add_argument("--quick", action="store_true", help="run A and C only")
    args = parser.parse_args()
    stack = Stack(args)
    if args.step == "up":
        up(stack)
    elif args.step == "down":
        down(stack)
    else:
        if not stack.pids_file.exists():
            sys.exit(f"no {stack.pids_file}: bring the stack up first")
        Run(stack).run(args.quick)


if __name__ == "__main__":
    main()
