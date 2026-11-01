#!/usr/bin/env python3
# Copyright (c) 2026 Geunhwa Jeong
# SPDX-License-Identifier: Apache-2.0
"""Run the keeper against a local network and check how it executes stop and TWAP tickets.

    scripts/localnet/keeper_check.py --perp-dex <engine copy> --work <localnet work dir> \\
        --database-url <indexer database> --keeper <perp-keeper binary>

Expects the stack the indexer's `scripts/localnet/run_all.sh` leaves up when run with HOLD, as
`check.py` does, on a fresh network. On top of what the scenario left, it opens a maker that
keeps a ladder around the price, a long, a short and an account without a position, and gives
the keeper a key of its own and a scratch database for the details. Every ticket is made the
way the front end makes one: its details go to the keeper first, and the ticket commits to the
commitment the keeper answers with, so every execution is also the engine checking the keeper's
encoding of the details. Then:

1. the startup checks pass; another chain's ID, a deployment without `perpetualsOrders` and half
   of the execution flags are refused;
2. a stop loss / take profit on the long, on the index price, waits at the price; once the price
   falls through its stop loss a dry run only simulates it, and live the keeper executes it and
   the long is closed;
3. a take profit on the short, on the book price, fires once the maker's ladder moves below it;
4. a standalone stop on the mark price places its resting limit order;
5. a stop loss without a position, an expired ticket, a ticket without details and one naming
   another executor are left alone, each counted for what it is;
6. a market order the book is too thin for is retried by simulation alone, backed off, and
   costs no gas;
7. a market order whose fill strays beyond --max-slippage-bps from the index price is held back
   and alerted on;
8. TWAP tickets: one buying in three chunks runs each chunk once the gap since the last one has
   passed and is finalized; a reduce-only one without a position to reduce and one without
   details are left alone; an expired one and one past its first-run deadline are cancelled; one
   that fills nothing spoils after its retry window and is cancelled; a reduce-only one closes the
   long it opened. The net gas of each kind of TWAP transaction is printed;
9. with the market on a signed source whose price goes stale after ten seconds and nothing
   relaying, a stop that only the oracle service's newer price triggers is executed with the
   signed prices relayed in front of it;
10. it stops cleanly on SIGTERM.

Refuses to run anywhere but a local chain.
"""

import argparse
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

STATE_FILE = Path(".localnet-scenario.json")
STATUS_URL = "http://127.0.0.1:9191"
ORACLE_PORT = 18790
DETAILS_DB = "sigma_keeper_localnet"
# What the front end puts in every ticket: the registry's minimum.
TICKET_GAS = 1_000_000


def load_lib(perp_dex):
    sys.path.insert(0, str(Path(perp_dex).expanduser() / "e2e"))
    import localnet_e2e as lib  # noqa: E402

    return lib


def http_json(path):
    try:
        with urllib.request.urlopen(STATUS_URL + path, timeout=2) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())
    except Exception:  # noqa: BLE001
        return None, None


def http_text(path):
    with urllib.request.urlopen(STATUS_URL + path, timeout=2) as r:
        return r.read().decode()


def metric(text, name, **labels):
    """The value of one sample of `name` with exactly these labels, 0 when absent."""
    want = ",".join(f'{k}="{v}"' for k, v in labels.items())
    for line in text.splitlines():
        if line.startswith("#"):
            continue
        key, _, value = line.rpartition(" ")
        series = key.split("{", 1)
        if series[0] != name:
            continue
        got = series[1].rstrip("}") if len(series) > 1 else ""
        if sorted(got.split(",")) == sorted(want.split(",")) or (not want and not got):
            return float(value)
    return 0.0


def post_details(body):
    request = urllib.request.Request(STATUS_URL + "/v1/details", data=json.dumps(body).encode(),
                                     headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(request, timeout=5) as r:
        return json.loads(r.read())


def vec_u8(data):
    return "vector[" + ",".join(f"{x}u8" for x in data) + "]"


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--perp-dex", required=True)
    parser.add_argument("--work", required=True)
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--keeper", required=True)
    parser.add_argument("--grpc", default="127.0.0.1:9000")
    parser.add_argument(
        "--engine-head",
        default="~/perp-dex",
        help="an engine checkout with the oracle_haneul package and e2e/oracle_signing.py, read only",
    )
    args = parser.parse_args()

    work = Path(args.work).resolve()
    binary = str(Path(args.keeper).resolve())
    lib = load_lib(args.perp_dex)
    os.chdir(Path(args.perp_dex).expanduser())
    lib.safety_check()

    call, obj, u64, u256, b, fx, px = lib.call, lib.obj, lib.u64, lib.u256, lib.b, lib.fx, lib.px
    events, section, check, CLOCK = lib.events, lib.section, lib.check, lib.CLOCK
    ASK, BID, GTC = lib.ASK, lib.BID, lib.GTC
    TUSD_UNIT, B9 = lib.TUSD_UNIT, lib.B9
    # The price pusher and the check send from the same address: one transaction at a time, or
    # two of them pick the same gas coin and one is refused.
    admin = threading.Lock()

    def ptb(*a, **k):
        with admin:
            return lib.ptb(*a, **k)

    state = json.loads(STATE_FILE.read_text())
    P, ids, oracle = state["packages"], state["publish_tx"], state["oracle"]
    AUTH, PERP, ORDERS, E2E = P["authority_cap"], P["perpetuals"], P["perpetuals_orders"], P["perp_e2e"]
    ADMIN = f"{AUTH}::authority::ADMIN"
    TUSD = f"{E2E}::tusd::TUSD"
    registry = lib.shared_created(ids["perpetuals"], "::registry::Registry")
    perp_pkg_admin = lib.owned_created(ids["perpetuals"], "::authority::AuthorityCap<")
    tusd_treasury = lib.owned_created(ids["perp_e2e"], "::coin::TreasuryCap<", "::tusd::TUSD>")
    source, oracle_config, pfs_btc, pfs_tusd = oracle["source"], oracle["config"], oracle["pfs_btc"], oracle["pfs_tusd"]
    deployment = json.loads((work / "deployment.json").read_text())
    ch = deployment["markets"]["BTC-USD"]["clearingHouse"]
    me = lib.cli("client", "active-address").stdout.strip()

    head = Path(args.engine_head).expanduser()
    sys.path.insert(0, str(head / "e2e"))
    import oracle_signing as signing  # noqa: E402

    def grpc(method, request):
        out = subprocess.run(["grpcurl", "-plaintext", "-d", json.dumps(request), args.grpc, method],
                             capture_output=True, text=True, check=True)
        return json.loads(out.stdout or "{}")

    def object_json(oid):
        return grpc("haneul.rpc.v2.LedgerService/GetObject", {"object_id": oid, "read_mask": {"paths": ["json"]}})["object"]["json"]

    def sql(query):
        out = subprocess.run(["psql", args.database_url, "-AtXc", query], capture_output=True, text=True, check=True)
        return out.stdout.strip()

    def wait_indexed(seconds=60):
        """Until the indexer has the chain's latest checkpoint."""
        tip = grpc("haneul.rpc.v2.LedgerService/GetServiceInfo", {})["checkpointHeight"]
        deadline = time.time() + seconds
        while time.time() < deadline:
            if int(sql("SELECT coalesce(min(checkpoint_hi_inclusive), -1) FROM watermarks") or -1) >= int(tip):
                return
            time.sleep(0.5)
        sys.exit("the indexer did not catch up")

    chain_id = grpc("haneul.rpc.v2.LedgerService/GetServiceInfo", {})["chainId"]

    # The keeper's own key: it needs gas and nothing else.
    created = json.loads(lib.cli("client", "new-address", "ed25519", "--json").stdout)
    keeper = created["address"]
    lib.cli("client", "faucet", "--address", keeper)
    keystore = Path(os.environ["HANEUL_CONFIG_DIR"]) / "haneul.keystore"

    # The keeper's own database, next to the indexer's.
    details_url = args.database_url.rsplit("/", 1)[0] + f"/{DETAILS_DB}"
    sql(f"DROP DATABASE IF EXISTS {DETAILS_DB}")
    sql(f"CREATE DATABASE {DETAILS_DB}")

    # The market's prices: the scenario's mock source, pushed again now and then so that they do
    # not go stale, until phase 8 moves the market to a signed source.
    price = [fx(30_000)]
    pushing = threading.Event()
    pushing.set()
    signed = {}

    def push_prices():
        cmds = (call(f"{E2E}::mock_source::set_price", [], obj(source), obj(oracle_config), obj(pfs_btc), lib.u128(price[0]), CLOCK)
                + call(f"{E2E}::mock_source::set_price", [], obj(source), obj(oracle_config), obj(pfs_tusd), lib.u128(lib.ONE), CLOCK))
        ptb("refresh prices", cmds)

    def set_price(usd):
        price[0] = fx(usd)
        push_prices()

    def pusher():
        while True:
            time.sleep(20)
            if pushing.is_set() and not signed:
                try:
                    push_prices()
                except Exception as e:  # noqa: BLE001
                    print(f"   price push failed: {str(e)[:200]}")

    # ------------------------------------------------------------------ accounts
    section("Accounts")
    set_price(30_000)
    threading.Thread(target=pusher, daemon=True).start()
    # Allocation per market and deposit, in TUSD. W trades only through standalone stops, which
    # allocate what they need from the account.
    plan = {"M": (300_000, 600_000), "U": (10_000, 20_000), "S": (10_000, 20_000), "W": (0, 50_000)}
    cmds = []
    for name, (_, deposit) in plan.items():
        cmds += call("0x2::coin::mint", [TUSD], obj(tusd_treasury), u64(deposit * TUSD_UNIT), assign=f"coin{name}")
        cmds += call(f"{PERP}::account::create_account", [TUSD], obj(registry), assign=f"acc{name}")
        cmds += call(f"{PERP}::account::deposit_collateral", [TUSD, ADMIN], f"acc{name}.0", f"acc{name}.2", obj(registry), f"coin{name}")
        cmds += call(f"{PERP}::account::consume_policy_and_share_account", [TUSD], f"acc{name}.0", f"acc{name}.1")
    cmds += ["--transfer-objects", "[" + ", ".join(f"acc{n}.2" for n in plan) + "]", obj(me)]
    j = ptb("create and fund the accounts", cmds)
    accounts = events(j, "::events::CreatedAccount")
    caps = [c["objectId"] for c in j["objectChanges"] if c["type"] == "created" and f"AuthorityCap<{PERP}::authority::ACCOUNT, {ADMIN}>" in c["objectType"]]
    cap_for = {}
    for cid in caps:
        o = json.loads(lib.cli("client", "object", cid, "--json").stdout)
        cap_for[o["content"]["for"]] = cid
    acct = {}
    for name, ev in zip(plan, accounts):
        acct[name] = dict(id=int(ev["account_id"]), obj=ev["account_obj_id"], cap=cap_for[ev["account_obj_id"]])
    cmds = []
    for name, (alloc, _) in plan.items():
        a = acct[name]
        cmds += call(f"{PERP}::clearing_house::create_market_position", [TUSD, ADMIN], obj(ch), obj(a["cap"]), obj(a["obj"]))
        if alloc:
            cmds += call(f"{PERP}::clearing_house::allocate_collateral", [TUSD, ADMIN], obj(ch), obj(a["cap"]), obj(a["obj"]), u64(alloc * TUSD_UNIT))
        cmds += call(f"{PERP}::clearing_house::set_position_initial_margin_ratio", [TUSD, ADMIN], obj(ch), obj(a["cap"]), obj(a["obj"]), u256(lib.IMR))
    ptb("positions, allocations, 10x for every account", cmds)

    def session(label, who, actions, alloc=False):
        a = acct[who]
        cmds = call("0x1::option::none", [f"{PERP}::account::IntegratorInfo"], assign="integrator")
        cmds += call(f"{PERP}::clearing_house::start_session", [TUSD, ADMIN], obj(ch), obj(a["cap"]), obj(a["obj"]), obj(pfs_btc), obj(pfs_tusd), "integrator", CLOCK, assign="hp")
        for action in actions:
            cmds += action
        cmds += call(f"{PERP}::clearing_house::end_session", [TUSD, ADMIN], "hp", obj(a["cap"]), obj(a["obj"]), b(alloc), "false", assign="res")
        cmds += call(f"{PERP}::clearing_house::share", [TUSD], "res.0")
        return ptb(label, cmds)

    def limit(side, size, at):
        return call(f"{PERP}::clearing_house::place_limit_order", [TUSD], "hp", b(side), u64(size), u64(px(at)), u64(GTC), "none", "false", "none")

    def market_order(side, size):
        return call(f"{PERP}::clearing_house::place_market_order", [TUSD], "hp", b(side), u64(size), "false")

    makers = []

    def maker_orders(label, orders):
        j = session(label, "M", orders)
        makers.extend(e["order_id"] for e in events(j, "::events::PostedOrder"))

    def ladder(center):
        """Six asks and six bids of 0.2, $10 apart, around `center`."""
        maker_orders(f"M posts a ladder around {center:,}",
                     [limit(ASK, LEVEL, center + 10 * i) for i in range(1, 7)] + [limit(BID, LEVEL, center - 10 * i) for i in range(1, 7)])

    def clear_book():
        if not makers:
            return
        cmds = ["--make-move-vec", "<u128>", "[" + ", ".join(f"{i}u128" for i in makers) + "]", "--assign", "oids"]
        cmds += call(f"{PERP}::clearing_house::try_cancel_orders", [TUSD, ADMIN], obj(ch), obj(acct["M"]["cap"]), obj(acct["M"]["obj"]), "oids")
        ptb("M takes its orders off the book", cmds)
        makers.clear()

    def cancel_open_orders(who):
        wait_indexed()
        order_ids = sql(f"SELECT string_agg(order_id::TEXT, ',') FROM orders WHERE market = '{ch}' AND account_id = {acct[who]['id']} AND status = 'open'")
        if not order_ids:
            return
        a = acct[who]
        cmds = ["--make-move-vec", "<u128>", "[" + ", ".join(f"{i}u128" for i in order_ids.split(",")) + "]", "--assign", "oids"]
        cmds += call(f"{PERP}::clearing_house::cancel_orders", [TUSD, ADMIN], obj(ch), obj(a["cap"]), obj(a["obj"]), "oids")
        ptb(f"{who} cancels its resting orders", cmds)

    def base(name):
        return float(sql(f"SELECT coalesce((SELECT base FROM positions WHERE market = '{ch}' AND account_id = {acct[name]['id']}), 0)"))

    LEVEL, LONG, TENTH, TWENTIETH = B9 // 5, B9 // 5, B9 // 10, B9 // 20
    section("Positions at 30,000")
    # Orders earlier runs left resting would trade with what this run checks. All of them are
    # held through capabilities of the active address.
    wait_indexed()
    leftovers = sql(
        f"SELECT a.object_id, c.cap_id, string_agg(o.order_id::TEXT, ',') FROM orders o "
        f"JOIN accounts a ON a.account_id = o.account_id "
        f"JOIN account_caps c ON c.account_object_id = a.object_id AND c.role = 'admin' AND c.owner = '{me}' "
        f"WHERE o.market = '{ch}' AND o.status = 'open' GROUP BY 1, 2")
    for line in filter(None, leftovers.splitlines()):
        account, cap, order_ids = line.split("|")
        cmds = ["--make-move-vec", "<u128>", "[" + ", ".join(f"{i}u128" for i in order_ids.split(",")) + "]", "--assign", "oids"]
        cmds += call(f"{PERP}::clearing_house::cancel_orders", [TUSD, ADMIN], obj(ch), obj(cap), obj(account), "oids")
        ptb(f"cancel {len(order_ids.split(','))} orders left by an earlier run", cmds)
    ladder(30_000)
    session("U buys 0.2", "U", [market_order(BID, LONG)])
    session("S sells 0.2", "S", [market_order(ASK, LONG)])
    clear_book()
    ladder(30_000)
    wait_indexed()
    check("U long 0.2, S short 0.2, W flat", base("U") == 0.2 and base("S") == -0.2 and base("W") == 0,
          f"U {base('U')} S {base('S')} W {base('W')}")

    # ------------------------------------------------------------------ tickets
    def salt():
        return os.urandom(32).hex()

    def sltp(*, trigger, short, size, sl=None, tp=None, expire=None):
        """A stop loss / take profit closing at market."""
        d = {"clearingHouse": ch, "isLimitOrder": False, "triggerPriceType": trigger, "positionIsAsk": short,
             "size": str(size), "price": "0", "orderType": "0", "salt": salt()}
        if sl is not None:
            d["stopLossPrice"] = str(fx(sl))
        if tp is not None:
            d["takeProfitPrice"] = str(fx(tp))
        if expire is not None:
            d["expireTimestampMs"] = str(expire)
        return {"kind": "stopLossTakeProfit", "details": d}

    def standalone(*, trigger, level, at_or_above, side, size, limit_at=None, expire=None):
        """A standalone stop: a GTC limit order at `limit_at`, or a market order without it."""
        d = {"clearingHouse": ch, "isLimitOrder": limit_at is not None, "triggerPriceType": trigger,
             "stopIndexPrice": str(fx(level)), "triggerAtOrAbove": at_or_above, "side": side, "size": str(size),
             "price": str(px(limit_at) if limit_at is not None else 0), "orderType": str(GTC), "reduceOnly": False,
             "salt": salt()}
        if expire is not None:
            d["expireTimestampMs"] = str(expire)
        return {"kind": "standaloneStop", "details": d}

    def create_ticket(label, who, stop_order_type, commitment, executors=None):
        a = acct[who]
        cmds = ["--split-coins", "gas", f"[{TICKET_GAS}]", "--assign", "g"]
        cmds += ["--make-move-vec", "<address>", "[" + ", ".join(obj(e) for e in executors or [keeper]) + "]", "--assign", "execs"]
        cmds += call(f"{ORDERS}::stop_orders::create_stop_order_ticket", [TUSD, ADMIN], obj(a["obj"]), obj(a["cap"]), obj(registry),
                     "execs", "none", "g.0", u64(stop_order_type), vec_u8(bytes.fromhex(commitment)))
        j = ptb(label, cmds)
        return events(j, "::events::CreatedStopOrderTicket")[0]["ticket_id"]

    def ticket(label, who, body, executors=None):
        """The front end's way: the details to the keeper, then a ticket on its commitment."""
        answer = post_details(body)
        stop_order_type = 0 if body["kind"] == "stopLossTakeProfit" else 1
        return create_ticket(label, who, stop_order_type, answer["commitment"], executors)

    def cancel_ticket(who, ticket_id):
        a = acct[who]
        cmds = call(f"{ORDERS}::stop_orders::cancel", [TUSD, ADMIN], obj(a["obj"]), obj(a["cap"]), obj(registry), obj(ticket_id), assign="g")
        cmds += ["--transfer-objects", "[g]", obj(me)]
        ptb(f"{who} cancels its ticket", cmds)

    def twap(*, side, size, chunks, gap_ms, uncertainty_ms=1_000, retry_ms=30_000, max_one_bps=10_000,
             reduce_only=False, expire=None, first_run_expire=None):
        """A TWAP order, each chunk an immediate-or-cancel order within 5% of the mark price."""
        d = {"executionGapMs": str(gap_ms), "executionTimeUncertaintyMs": str(uncertainty_ms), "chunksAmount": str(chunks),
             "smallTailMergeThresholdBps": "0", "timeForRetryMs": str(retry_ms), "amountUncertaintyBps": "0",
             "maxOneExecutionAmountBps": str(max_one_bps), "side": side, "size": str(size), "maxSlippageBps": "500",
             "reduceOnly": reduce_only, "salt": salt()}
        if expire is not None:
            d["expireTimestampMs"] = str(expire)
        if first_run_expire is not None:
            d["firstRunExpireTimestampMs"] = str(first_run_expire)
        return {"kind": "twap", "details": d}

    def create_twap(label, who, commitment, executors=None):
        a = acct[who]
        cmds = ["--split-coins", "gas", f"[{TICKET_GAS}]", "--assign", "g"]
        cmds += ["--make-move-vec", "<address>", "[" + ", ".join(obj(e) for e in executors or [keeper]) + "]", "--assign", "execs"]
        cmds += call(f"{ORDERS}::twap_orders::create_twap_order_ticket", [TUSD, ADMIN], obj(a["obj"]), obj(a["cap"]), obj(ch), obj(registry),
                     "execs", "none", "g.0", vec_u8(bytes.fromhex(commitment)))
        j = ptb(label, cmds)
        return events(j, "::events::CreatedTWAPOrderTicket")[0]["ticket_id"]

    def twap_ticket(label, who, body):
        """The front end's way, as for a stop ticket."""
        return create_twap(label, who, post_details(body)["commitment"])

    def user_cancel_twap(who, ticket_id):
        a = acct[who]
        cmds = call(f"{ORDERS}::twap_orders::user_cancel_twap_order", [TUSD, ADMIN], obj(a["obj"]), obj(a["cap"]), obj(ch), obj(pfs_btc),
                    obj(pfs_tusd), obj(ticket_id), CLOCK, obj(registry), assign="g")
        cmds += ["--transfer-objects", "[g]", obj(me)]
        ptb(f"{who} cancels its TWAP ticket", cmds)

    def ticket_status(ticket_id):
        return sql(f"SELECT status FROM order_tickets WHERE ticket_id = '{ticket_id}'")

    def wait_status(ticket_id, status, seconds=40):
        deadline = time.time() + seconds
        while time.time() < deadline:
            if ticket_status(ticket_id) == status:
                return True
            time.sleep(0.5)
        return None

    def executors_of(ticket_id):
        return sql(f"SELECT executors::TEXT FROM order_tickets WHERE ticket_id = '{ticket_id}'")

    # ------------------------------------------------------------------ the keeper
    def write_deployment(path, with_orders=True):
        packages = {"perpetuals": PERP}
        if with_orders:
            packages["perpetualsOrders"] = ORDERS
        path.write_text(json.dumps({
            "network": "localnet",
            "packages": packages,
            "registry": registry,
            "collateral": {"coinType": TUSD, "decimals": 6, "priceFeedStorage": pfs_tusd},
            "markets": {"BTC-USD": {"marketId": "BTC-USD", "clearingHouse": ch, "basePriceFeedStorage": pfs_btc}},
        }, indent=2))
        return path

    keeper_deployment = write_deployment(work / "keeper.json")
    base_args = [
        binary, "--details-database-url", details_url, "--database-url", args.database_url,
        "--deployment", str(keeper_deployment), "--rpc-url", f"http://{args.grpc}", "--chain-id", chain_id,
        "--key-file", str(keystore), "--address", keeper,
        "--poll-interval-ms", "500", "--match-interval-secs", "1", "--min-gas-balance", "1",
    ]
    procs = []

    def start(name, extra=()):
        log = open(work / f"keeper-{name}.log", "w")
        p = subprocess.Popen(base_args + list(extra), stdout=log, stderr=subprocess.STDOUT,
                             env={**os.environ, "RUST_LOG": "info,perp_keeper=debug", "NO_COLOR": "1"})
        procs.append(p)
        return p

    def stop(p):
        p.send_signal(signal.SIGTERM)
        try:
            return p.wait(timeout=30)
        except subprocess.TimeoutExpired:
            p.kill()
            return None

    def wait_for(cond, seconds=40):
        deadline = time.time() + seconds
        while time.time() < deadline:
            value = cond()
            if value:
                return value
            time.sleep(0.5)
        return None

    def healthy():
        code, _ = http_json("/health")
        return code == 200

    def metrics():
        try:
            return http_text("/metrics")
        except Exception:  # noqa: BLE001
            return ""

    def executions(kind, result):
        return metric(metrics(), "perp_keeper_executions_total", kind=kind, result=result)

    def gas():
        """Net gas of the executions so far: (spent, rebated)."""
        m = metrics()
        return metric(m, "perp_keeper_gas_spent_total"), metric(m, "perp_keeper_gas_rebated_total")

    def tickets():
        _, status = http_json("/status")
        return (status or {}).get("tickets", {})

    def log_of(name):
        return (work / f"keeper-{name}.log").read_text()

    server = None
    try:
        # -------------------------------------------------------------- 1. startup
        section("1. Startup checks")
        out = subprocess.run(base_args + ["--check-only"], capture_output=True, text=True, env={**os.environ, "RUST_LOG": "info"})
        check("the startup checks pass", out.returncode == 0, (out.stdout + out.stderr)[-400:])
        at = base_args.index("--chain-id") + 1
        wrong = subprocess.run([*base_args[:at], "wrong-chain", *base_args[at + 1:], "--check-only"], capture_output=True, text=True)
        check("refuses another chain's ID", wrong.returncode == 1 and "not wrong-chain" in wrong.stdout + wrong.stderr)
        at = base_args.index("--deployment") + 1
        no_orders = write_deployment(work / "keeper-no-orders.json", with_orders=False)
        out = subprocess.run([*base_args[:at], str(no_orders), *base_args[at + 1:], "--check-only"], capture_output=True, text=True)
        check("refuses a deployment without perpetualsOrders", out.returncode == 1 and "perpetualsOrders" in out.stdout + out.stderr,
              (out.stdout + out.stderr)[-300:])
        half = [binary, "--details-database-url", details_url, "--database-url", args.database_url, "--deployment", str(keeper_deployment)]
        out = subprocess.run(half, capture_output=True, text=True)
        check("refuses half of the execution flags", out.returncode != 0 and "together" in out.stdout + out.stderr,
              (out.stdout + out.stderr)[-300:])

        # -------------------------------------------------------------- 2. stop loss, index price
        section("2. A stop loss / take profit on the long, on the index price")
        p = start("dry", ["--dry-run"])
        check("becomes healthy", wait_for(healthy, 60) is not None, log_of("dry")[-500:])
        _, status = http_json("/status")
        check("in dry-run mode, as its own address", (status or {}).get("mode") == "dry-run" and (status or {}).get("address") == keeper, str(status)[:200])
        ticket_a = ticket("A: U's stop loss at 29,000 and take profit at 31,000", "U", sltp(trigger=0, short=False, size=LONG, sl=29_000, tp=31_000))
        waiting = wait_for(lambda: tickets().get("waiting"), 30)
        check("the keeper sees it waiting at 30,000", waiting is not None, f"{tickets()}; executors {executors_of(ticket_a)}")
        set_price(28_900)
        simulated = wait_for(lambda: executions("stop_loss_take_profit", "simulated") or None, 30)
        check("once the index falls through the stop loss, a dry run simulates it", simulated is not None, log_of("dry")[-600:])
        time.sleep(3)
        wait_indexed()
        check("and sends nothing", ticket_status(ticket_a) == "open" and base("U") == 0.2, f"{ticket_status(ticket_a)}, U {base('U')}")
        check("stops cleanly", stop(p) == 0)
        p = start("live")
        check("becomes healthy live", wait_for(healthy, 60) is not None, log_of("live")[-500:])
        executed = wait_status(ticket_a, "executed")
        check("live it executes it: the engine takes the keeper's stop loss / take profit encoding", executed is not None, log_of("live")[-800:])
        wait_indexed()
        check("the long is closed", base("U") == 0, f"U {base('U')}")
        check("counted as executed", executions("stop_loss_take_profit", "executed") == 1)
        spent, rebated = gas()
        # Deleting the ticket earns a storage rebate, which can outweigh what the execution costs.
        check("and its net gas, spent or rebated", spent + rebated > 0, f"spent {spent:.0f}, rebated {rebated:.0f}")
        set_price(30_000)

        # -------------------------------------------------------------- 3. take profit, book price
        section("3. A take profit on the short, on the book price")
        ticket_b = ticket("B: S's take profit at 29,500 and stop loss at 31,500, on the book price", "S",
                          sltp(trigger=1, short=True, size=LONG, sl=31_500, tp=29_500))
        time.sleep(4)
        check("waits while the book is around 30,000", ticket_status(ticket_b) == "open" and tickets().get("waiting") == 1, str(tickets()))
        clear_book()
        ladder(29_400)
        executed = wait_status(ticket_b, "executed")
        check("fires once the ladder moves to 29,400", executed is not None, log_of("live")[-800:])
        wait_indexed()
        check("the short is closed", base("S") == 0, f"S {base('S')}")
        clear_book()
        ladder(30_000)

        # -------------------------------------------------------------- 4. standalone, mark price
        section("4. A standalone stop on the mark price")
        ticket_c = ticket("C: W bids 0.1 at 20,000 once the mark price is at or above 1", "W",
                          standalone(trigger=2, level=1, at_or_above=True, side=BID, size=TENTH, limit_at=20_000))
        executed = wait_status(ticket_c, "executed")
        check("executed", executed is not None, log_of("live")[-800:])
        wait_indexed()
        resting = sql(f"SELECT count(*) FROM orders WHERE market = '{ch}' AND account_id = {acct['W']['id']} AND status = 'open'")
        check("its limit order rests", resting == "1", resting)
        cancel_open_orders("W")
        check("counted as executed", executions("standalone_stop", "executed") == 1)

        # -------------------------------------------------------------- 5. left alone
        section("5. Tickets the keeper leaves alone")
        executed_before = executions("stop_loss_take_profit", "executed") + executions("standalone_stop", "executed")
        ticket_d = ticket("D: a stop loss for W, which holds no position", "W", sltp(trigger=0, short=False, size=TENTH, sl=40_000))
        ticket_e = ticket("E: an expired stop", "W",
                          standalone(trigger=0, level=1, at_or_above=True, side=BID, size=TENTH, limit_at=20_000, expire=int(time.time() * 1000) - 1_000))
        ticket_g = create_ticket("G: a ticket whose details the keeper never got", "W", 1, os.urandom(32).hex())
        ticket_f = ticket("F: a ticket naming another executor", "W",
                          standalone(trigger=0, level=1, at_or_above=True, side=BID, size=TENTH, limit_at=20_000), executors=[me])
        counted = wait_for(lambda: (lambda t: t if t.get("no_position") and t.get("expired") and t.get("no_details") else None)(tickets()), 30)
        t = counted or tickets()
        check("D counted as without a position", t.get("no_position") == 1, str(t))
        check("E as expired", t.get("expired") == 1, str(t))
        check("G as without details", t.get("no_details") == 1, str(t))
        check("F not seen at all: it names another executor", sum(t.values()) == 3, str(t))
        time.sleep(3)
        still_open = [ticket_status(x) for x in (ticket_d, ticket_e, ticket_g, ticket_f)]
        executed_after = executions("stop_loss_take_profit", "executed") + executions("standalone_stop", "executed")
        check("none of them is executed", still_open == ["open"] * 4 and executed_after == executed_before, str(still_open))
        # D would fire once W holds a long, as a ticket left over from a closed position does.
        for left in (ticket_d, ticket_e, ticket_g, ticket_f):
            cancel_ticket("W", left)

        # -------------------------------------------------------------- 6. too thin a book
        section("6. A market order the book is too thin for")
        gas_before = gas()
        refused_before = executions("standalone_stop", "refused")
        ticket_h = ticket("H: W buys 5 at market once the index is at or above 1", "W",
                          standalone(trigger=0, level=1, at_or_above=True, side=BID, size=5 * B9))
        refused = wait_for(lambda: executions("standalone_stop", "refused") >= refused_before + 2 or None, 40)
        check("refused by the simulation, try after try", refused is not None, log_of("live")[-800:])
        check("and says why", "too thin" in log_of("live"))
        check("backs off between tries", wait_for(lambda: tickets().get("backoff"), 20) is not None, str(tickets()))
        check("spends no gas on it", gas() == gas_before, f"{gas_before} -> {gas()}")
        check("the ticket stays open", ticket_status(ticket_h) == "open")
        cancel_ticket("W", ticket_h)

        # -------------------------------------------------------------- 7. a fill far from the index
        section("7. A fill far from the index price")
        clear_book()
        maker_orders("M offers 0.1 at 39,000 and bids 0.1 at 29,000, nothing else", [limit(ASK, TENTH, 39_000), limit(BID, TENTH, 29_000)])
        guarded_before = executions("standalone_stop", "guarded")
        ticket_i = ticket("I: W buys 0.05 at market once the index is at or above 1", "W",
                          standalone(trigger=0, level=1, at_or_above=True, side=BID, size=TWENTIETH))
        guarded = wait_for(lambda: executions("standalone_stop", "guarded") > guarded_before or None, 30)
        check("a fill at 39,000, 30% from the index, is held back", guarded is not None, log_of("live")[-800:])
        check("and alerted on", metric(metrics(), "perp_keeper_alerts_total", level="warning") >= 1 and "held back" in log_of("live"))
        wait_indexed()
        check("the ticket stays open and W bought nothing", ticket_status(ticket_i) == "open" and base("W") == 0, f"W {base('W')}")
        cancel_ticket("W", ticket_i)
        clear_book()
        ladder(30_000)

        # -------------------------------------------------------------- 8. TWAP tickets
        section("8. TWAP tickets")
        THIRD = 3 * TENTH
        first_checkpoint = int(grpc("haneul.rpc.v2.LedgerService/GetServiceInfo", {})["checkpointHeight"])
        ticket_k = twap_ticket("K: W buys 0.3 in three chunks, 3 s apart", "W", twap(side=BID, size=THIRD, chunks=3, gap_ms=3_000))
        finalized = wait_status(ticket_k, "finalized", 90)
        check("runs its three chunks and finalizes it: the engine takes the keeper's TWAP encoding", finalized is not None,
              log_of("live")[-1200:])
        wait_indexed()
        check("W is long 0.3", base("W") == 0.3, f"W {base('W')}")
        chunks, finalized = executions("twap_chunk", "executed"), executions("twap_finalize", "executed")
        check("counted as three chunks and a finalization", (chunks, finalized) == (3, 1), f"{chunks} chunks, {finalized} finalized")
        check("no chunk refused: each is sent only once it is due", executions("twap_chunk", "refused") == 0, log_of("live")[-800:])
        attempts = [int(x) for x in sql(
            "SELECT string_agg(data->>'last_attempt_timestamp_ms', ',' ORDER BY checkpoint, tx_index) FROM raw_events "
            f"WHERE name = 'ProcessedTWAPOrderTicket' AND checkpoint >= {first_checkpoint} "
            f"AND (data->>'account_id')::BIGINT = {acct['W']['id']}").split(",") if x]
        gaps = [later - earlier for earlier, later in zip(attempts, attempts[1:])]
        check("each chunk on chain at least the 3 s gap after the one before", len(attempts) == 3 and all(g >= 3_000 for g in gaps),
              f"attempts {attempts}, gaps {gaps}")

        ticket_l = twap_ticket("L: a reduce-only bid, which does not reduce W's long", "W",
                               twap(side=BID, size=TENTH, chunks=1, gap_ms=0, reduce_only=True))
        ticket_m = create_twap("M: a TWAP whose details the keeper never got", "W", os.urandom(32).hex())
        counted = wait_for(lambda: (lambda t: t if t.get("twap_no_position") and t.get("twap_no_details") else None)(tickets()), 30)
        t = counted or tickets()
        check("L counted as without a position to reduce, M as without details",
              t.get("twap_no_position") == 1 and t.get("twap_no_details") == 1, str(t))
        time.sleep(3)
        check("neither runs", [ticket_status(ticket_l), ticket_status(ticket_m)] == ["open", "open"]
              and executions("twap_chunk", "executed") == 3, str(tickets()))
        user_cancel_twap("W", ticket_l)
        user_cancel_twap("W", ticket_m)

        now_ms = int(time.time() * 1000)
        ticket_n = twap_ticket("N: a TWAP that expired 15 s ago", "W", twap(side=BID, size=TENTH, chunks=1, gap_ms=0, expire=now_ms - 15_000))
        ticket_o = twap_ticket("O: a TWAP whose first chunk was due by 15 s ago", "W",
                               twap(side=BID, size=TENTH, chunks=1, gap_ms=0, first_run_expire=now_ms - 15_000))
        both = wait_for(lambda: ticket_status(ticket_n) == "canceled" and ticket_status(ticket_o) == "canceled", 40)
        check("both cancelled by the keeper: the engine no longer runs them", both is not None, log_of("live")[-800:])
        check("counted as cancellations, with no chunk run",
              executions("twap_cancel", "executed") == 2 and executions("twap_chunk", "executed") == 3,
              f"{executions('twap_cancel', 'executed')} cancelled, {executions('twap_chunk', 'executed')} chunks")

        clear_book()
        ticket_p = twap_ticket("P: W buys 0.1 in two chunks 2 s apart with no ask on the book, retried for 2 s", "W",
                               twap(side=BID, size=TENTH, chunks=2, gap_ms=2_000, retry_ms=2_000))
        spoiled = wait_status(ticket_p, "canceled", 60)
        check("fills nothing, spoils after its retry window, and the keeper cancels it", spoiled is not None, log_of("live")[-1200:])
        wait_indexed()
        attempted = executions("twap_chunk", "executed") - 3
        check("its attempts ran and W bought nothing", attempted >= 2 and base("W") == 0.3, f"{attempted} attempts, W {base('W')}")
        ladder(30_000)

        ticket_q = twap_ticket("Q: a reduce-only TWAP sells W's long 0.3 in one chunk", "W",
                               twap(side=ASK, size=THIRD, chunks=1, gap_ms=0, reduce_only=True))
        closed = wait_status(ticket_q, "finalized", 60)
        check("runs and is finalized", closed is not None, log_of("live")[-800:])
        wait_indexed()
        check("W is flat", base("W") == 0, f"W {base('W')}")
        by_kind = {}
        for kind, used in re.findall(r'kind="?(twap_\w+)"?[^\n]*?gas_used=(-?\d+)', log_of("live")):
            by_kind.setdefault(kind, []).append(int(used))
        check("the net gas of every TWAP transaction is logged", sorted(by_kind) == ["twap_cancel", "twap_chunk", "twap_finalize"], str(by_kind))
        print(f"   net gas per transaction, GEUNHWA (negative: the rebate outweighed the cost): {by_kind}")
        print(f"   each ticket held {TICKET_GAS:,} GEUNHWA, paid to the keeper as its size filled or when it was cancelled")
        check("stops cleanly", stop(p) == 0)

        # -------------------------------------------------------------- 9. signed prices
        section("9. A stop only the oracle service's newer price triggers")
        package_dir = Path(args.perp_dex).expanduser() / "packages/oracle_haneul"
        if not package_dir.exists():
            shutil.copytree(head / "packages/oracle_haneul", package_dir)
        with admin:
            out = subprocess.run([lib.HANEUL, "client", "test-publish", "--build-env", "mainnet", "--pubfile-path", str(lib.PUBFILE),
                                  "--gas-budget", str(lib.GAS_BUDGET), "--json"], capture_output=True, text=True, cwd=package_dir)
        if out.returncode != 0:
            sys.exit(f"publishing oracle_haneul failed:\n{(out.stdout + out.stderr)[-2000:]}")
        SIGNED = next(c["packageId"] for c in json.loads(out.stdout)["objectChanges"] if c["type"] == "published")
        ORACLE = P["oracle_aggregator"]
        oracle_pkg_admin = lib.owned_created(ids["oracle_aggregator"], "::authority::AuthorityCap<")
        VK = f"{E2E}::vendor_key::E2E"

        def owned_of(object_type):
            token = None
            while True:
                request = {"owner": me, "read_mask": {"paths": ["object_id", "object_type"]}, "page_size": 500}
                if token:
                    request["page_token"] = token
                reply = grpc("haneul.rpc.v2.StateService/ListOwnedObjects", request)
                for o in reply.get("objects", []):
                    if o.get("objectType", "").replace(" ", "") == object_type.replace(" ", ""):
                        return o["objectId"]
                token = reply.get("nextPageToken")
                if not token:
                    sys.exit(f"the active address owns no {object_type}")

        oracle_vk = owned_of(f"{AUTH}::authority::AuthorityCap<{ORACLE}::authority::VENDOR<{VK}>, {ADMIN}>")
        perp_vk = owned_of(f"{AUTH}::authority::AuthorityCap<{PERP}::authority::VENDOR<{VK}>, {ADMIN}>")
        seed = bytes([0x77] * 32)
        cmds = call(f"{SIGNED}::source::create", [ADMIN], obj(oracle_config), obj(oracle_pkg_admin), assign="src")
        cmds += call(f"{SIGNED}::source::authorize", [ADMIN], "src", obj(oracle_config), obj(oracle_pkg_admin))
        cmds += call(f"{SIGNED}::source::set_signer", [ADMIN], "src", obj(oracle_config), obj(oracle_pkg_admin), vec_u8(signing.public_key(seed)), u64(2**64 - 1), CLOCK)
        cmds += ["--move-call", "0x2::transfer::public_share_object", f"<{ORACLE}::source::Source<{SIGNED}::source::HANEUL>>", "src"]
        j = ptb("a signed source with a throwaway signer", cmds)
        signed_source = lib.shared_created(j, "::source::HANEUL>")
        source_id = int(events(j, "::events::CreatedSource")[0]["source_id"])
        storage = {pfs_btc: int(object_json(pfs_btc)["storage_id"]), pfs_tusd: int(object_json(pfs_tusd)["storage_id"])}

        def sign(pfs, value, ts):
            return signing.sign_price_update(seed, signed_source, storage[pfs], value, 0, ts)

        ts = int(time.time() * 1000) - 300
        cmds = []
        for pfs, value in ((pfs_btc, price[0]), (pfs_tusd, lib.ONE)):
            cmds += call(f"{SIGNED}::price_feed_storage::new_price_feed", [VK, ADMIN], obj(signed_source), obj(oracle_vk), obj(oracle_config), obj(pfs),
                         lib.u128(value), lib.u128(0), u64(ts), vec_u8(signing.public_key(seed)), vec_u8(sign(pfs, value, ts)), u64(1), CLOCK)
        cmds += call(f"{PERP}::clearing_house::set_base_oracle_params", [VK, ADMIN, TUSD], obj(ch), obj(perp_vk), obj(registry), obj(pfs_btc),
                     f"some({source_id}u16)", f"some({u64(10_000)})")
        cmds += call(f"{PERP}::clearing_house::set_collateral_oracle_params", [TUSD, ADMIN], obj(ch), obj(perp_pkg_admin), obj(registry), obj(pfs_tusd),
                     f"some({source_id}u16)", f"some({u64(30_000)})")
        ptb("feeds of the signed source at 30,000, and the market switched to it", cmds)
        signed.update(source=signed_source)
        # 1% down: well inside the source's step limit.
        served = fx(29_700)

        class Updates(BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802
                ts = int(time.time() * 1000) - 300
                body = {
                    "packageId": SIGNED, "sourceId": signed_source, "aggregatorConfigId": oracle_config,
                    "updates": [
                        {"symbol": symbol, "storageId": storage[pfs], "priceFeedStorageId": pfs, "price": str(value), "confidence": "0",
                         "timestampMs": str(ts), "publicKey": signing.public_key(seed).hex(), "signature": sign(pfs, value, ts).hex()}
                        for symbol, pfs, value in (("BTC/USD", pfs_btc, served), ("TUSD/USD", pfs_tusd, lib.ONE))
                    ],
                }
                data = json.dumps(body).encode()
                self.send_response(200)
                self.send_header("content-type", "application/json")
                self.end_headers()
                self.wfile.write(data)

            def log_message(self, *_):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", ORACLE_PORT), Updates)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        print("   waiting 12 s so the price on chain is older than the market's 10 s tolerance")
        time.sleep(12)
        p = start("oracle", ["--oracle-updates-url", f"http://127.0.0.1:{ORACLE_PORT}/v1/updates"])
        check("becomes healthy with the oracle service", wait_for(healthy, 60) is not None, log_of("oracle")[-500:])
        ticket_j = ticket("J: W bids 0.1 at 20,000 once the index is at or below 29,800", "W",
                          standalone(trigger=0, level=29_800, at_or_above=False, side=BID, size=TENTH, limit_at=20_000))
        executed = wait_status(ticket_j, "executed")
        check("executed on the signed 29,700 while the chain still says 30,000", executed is not None, log_of("oracle")[-800:])
        check("with the signed prices relayed in front of it", metric(metrics(), "perp_keeper_oracle_updates_included_total") >= 2)
        feed = next(f for f in object_json(pfs_btc)["feeds"] if int(f["source_id"]) == source_id)
        check("the base feed holds the signed price now", int(feed["price"]) == served and int(time.time() * 1000) - int(feed["timestamp_ms"]) < 15_000, str(feed))
        cancel_open_orders("W")

        # -------------------------------------------------------------- 10. shutdown
        section("10. Shutdown")
        check("stops cleanly on SIGTERM", stop(p) == 0 and "Stopping" in log_of("oracle"))
    finally:
        pushing.clear()
        for p in procs:
            if p.poll() is None:
                p.kill()
        if server is not None:
            server.shutdown()
        try:
            sql(f"DROP DATABASE IF EXISTS {DETAILS_DB}")
        except subprocess.CalledProcessError as e:
            print(f"   dropping {DETAILS_DB} failed: {e.stderr.strip()[:200]}")

    failed = [n for n, ok in lib.RESULTS if not ok]
    print(f"\n{len(lib.RESULTS) - len(failed)}/{len(lib.RESULTS)} checks passed")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
