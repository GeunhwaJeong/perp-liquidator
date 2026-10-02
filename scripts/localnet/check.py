#!/usr/bin/env python3
# Copyright (c) 2026 Geunhwa Jeong
# SPDX-License-Identifier: Apache-2.0

"""Run the liquidator against a local network and check what it does.

    scripts/localnet/check.py --perp-dex <engine copy> --work <localnet work dir> \\
        --database-url <indexer database> --liquidator <perp-liquidator binary>

Expects the stack the indexer's `scripts/localnet/run_all.sh` leaves up when run with HOLD: a
local network with the engine published, the BTC-USD market of its scenario, and the indexer
following the chain. The engine copy is the one the scenario ran from (its
`.localnet-scenario.json` has the package and object IDs).

On top of what the scenario left, this opens its own accounts: the liquidator's (with an
assistant capability), a maker, three accounts that a price drop makes liquidatable (one of them
with a resting order), a short that a price rise leaves in bad debt beyond what socialization
may take, and healthy longs that must never be touched. Then:

1. a startup check fails while the liquidator has no position, and says how to fix it;
2. it starts live with --create-positions, opens its position and becomes healthy;
3. in a dry run after the price drops, it simulates the liquidations and sends nothing;
4. live, it liquidates the three, cancels the resting order and sells what it took over back
   into the maker's bids in the same transactions; what the bids cannot take it keeps, alerts
   on, and sells once new bids arrive;
5. after the price rises, the short's bad debt can only be closed by ADL: without the ADL
   capability it reports the plan and turns unhealthy;
6. with the capability, it auto-deleverages the short against the profitable longs;
7. with the market on a signed source whose price goes stale after ten seconds and no relayer,
   it finds a position the oracle service's newer price makes liquidatable and liquidates it
   with the signed prices in front of the liquidation;
8. it stops cleanly on SIGTERM.

Refuses to run anywhere but a local chain.
"""

import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

STATE_FILE = Path(".localnet-scenario.json")
STATUS_URL = "http://127.0.0.1:9188"


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


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--perp-dex", required=True)
    parser.add_argument("--work", required=True)
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--liquidator", required=True)
    parser.add_argument("--grpc", default="127.0.0.1:9000")
    parser.add_argument(
        "--engine-head",
        default="~/perp-dex",
        help="an engine checkout with the oracle_haneul package and e2e/oracle_signing.py, read only",
    )
    args = parser.parse_args()

    work = Path(args.work).resolve()
    binary = str(Path(args.liquidator).resolve())
    lib = load_lib(args.perp_dex)
    os.chdir(Path(args.perp_dex).expanduser())
    lib.safety_check()

    call, ptb, obj, u64, u256, b, fx, px = lib.call, lib.ptb, lib.obj, lib.u64, lib.u256, lib.b, lib.fx, lib.px
    events, section, check, CLOCK = lib.events, lib.section, lib.check, lib.CLOCK
    ASK, BID, GTC = lib.ASK, lib.BID, lib.GTC
    TUSD_UNIT = lib.TUSD_UNIT

    state = json.loads(STATE_FILE.read_text())
    P, ids, oracle = state["packages"], state["publish_tx"], state["oracle"]
    AUTH, PERP, E2E = P["authority_cap"], P["perpetuals"], P["perp_e2e"]
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

    def owned_of(object_type):
        """The first object of `object_type` the active address owns."""
        token = None
        while True:
            request = {"owner": me, "read_mask": {"paths": ["object_id", "object_type"]}, "page_size": 500}
            if token:
                request["page_token"] = token
            reply = json.loads(subprocess.run(
                ["grpcurl", "-plaintext", "-d", json.dumps(request), args.grpc, "haneul.rpc.v2.StateService/ListOwnedObjects"],
                capture_output=True, text=True, check=True).stdout or "{}")
            for o in reply.get("objects", []):
                if o.get("objectType", "").replace(" ", "") == object_type.replace(" ", ""):
                    return o["objectId"]
            token = reply.get("nextPageToken")
            if not token:
                sys.exit(f"the active address owns no {object_type}")

    def sql(query):
        out = subprocess.run(["psql", args.database_url, "-AtXc", query], capture_output=True, text=True, check=True)
        return out.stdout.strip()

    def wait_indexed(seconds=60):
        """Until the indexer has the chain's latest checkpoint."""
        tip = json.loads(subprocess.run(
            ["grpcurl", "-plaintext", "-d", "{}", args.grpc, "haneul.rpc.v2.LedgerService/GetServiceInfo"],
            capture_output=True, text=True, check=True).stdout)["checkpointHeight"]
        deadline = time.time() + seconds
        while time.time() < deadline:
            if int(sql("SELECT coalesce(min(checkpoint_hi_inclusive), -1) FROM watermarks") or -1) >= int(tip):
                return
            time.sleep(0.5)
        sys.exit("the indexer did not catch up")

    chain_id = json.loads(subprocess.run(
        ["grpcurl", "-plaintext", "-d", "{}", args.grpc, "haneul.rpc.v2.LedgerService/GetServiceInfo"],
        capture_output=True, text=True, check=True).stdout)["chainId"]

    price = [fx(30_000)]

    # Set once the market reads the signed source of phase 7: (package, source object, signing
    # seed, storage ids by feed).
    signed = {}

    def refresh_prices():
        if signed:
            return signed_updates(price[0])
        return (call(f"{E2E}::mock_source::set_price", [], obj(source), obj(oracle_config), obj(pfs_btc), lib.u128(price[0]), CLOCK)
                + call(f"{E2E}::mock_source::set_price", [], obj(source), obj(oracle_config), obj(pfs_tusd), lib.u128(lib.ONE), CLOCK))

    def sign(pfs, value, timestamp_ms):
        return signing.sign_price_update(signed["seed"], signed["source"], signed["storage"][pfs], value, 0, timestamp_ms)

    def signed_updates(btc):
        """`update_price_feed` calls for both feeds, signed just now."""
        ts = int(time.time() * 1000) - 300
        cmds = []
        for pfs, value in ((pfs_btc, btc), (pfs_tusd, lib.ONE)):
            cmds += call(f"{signed['package']}::price_feed_storage::update_price_feed", [], obj(signed["source"]), obj(oracle_config), obj(pfs),
                         lib.u128(value), lib.u128(0), u64(ts), vec_u8(signing.public_key(signed["seed"])), vec_u8(sign(pfs, value, ts)), CLOCK)
        return cmds

    def vec_u8(data):
        return "vector[" + ",".join(f"{x}u8" for x in data) + "]"

    def set_price(usd):
        price[0] = fx(usd)
        ptb(f"index price to {usd:,}", refresh_prices())

    # ------------------------------------------------------------------ accounts
    section("Accounts")
    set_price(30_000)
    # Allocation per market, in TUSD; the liquidator keeps its collateral unallocated.
    plan = {"K": 0, "M2": 300_000, "A": 3_300, "A2": 2_150, "F": 3_300, "B": 6_200, "C": 3_000, "D": 20_000, "E": 50_000}
    deposits = {name: max(alloc, 1) * 2 for name, alloc in plan.items()}
    deposits["K"] = 100_000
    cmds = []
    for name, amount in deposits.items():
        cmds += call("0x2::coin::mint", [TUSD], obj(tusd_treasury), u64(amount * TUSD_UNIT), assign=f"coin{name}")
        cmds += call(f"{PERP}::account::create_account", [TUSD], obj(registry), assign=f"acc{name}")
        cmds += call(f"{PERP}::account::deposit_collateral", [TUSD, ADMIN], f"acc{name}.0", f"acc{name}.2", obj(registry), f"coin{name}")
        cmds += call(f"{PERP}::account::consume_policy_and_share_account", [TUSD], f"acc{name}.0", f"acc{name}.1")
    cmds += ["--transfer-objects", "[" + ", ".join(f"acc{n}.2" for n in deposits) + "]", obj(me)]
    j = ptb("create and fund the accounts", cmds)
    created = events(j, "::events::CreatedAccount")
    caps = [c["objectId"] for c in j["objectChanges"] if c["type"] == "created" and f"AuthorityCap<{PERP}::authority::ACCOUNT, {ADMIN}>" in c["objectType"]]
    cap_for = {}
    for cid in caps:
        o = json.loads(lib.cli("client", "object", cid, "--json").stdout)
        cap_for[o["content"]["for"]] = cid
    acct = {}
    for name, ev in zip(deposits, created):
        acct[name] = dict(id=int(ev["account_id"]), obj=ev["account_obj_id"], cap=cap_for[ev["account_obj_id"]])

    cmds = []
    for name, alloc in plan.items():
        if name == "K":
            continue
        a = acct[name]
        cmds += call(f"{PERP}::clearing_house::create_market_position", [TUSD, ADMIN], obj(ch), obj(a["cap"]), obj(a["obj"]))
        cmds += call(f"{PERP}::clearing_house::allocate_collateral", [TUSD, ADMIN], obj(ch), obj(a["cap"]), obj(a["obj"]), u64(alloc * TUSD_UNIT))
        # Positions start at 1x; these trade at the market's 10x.
        cmds += call(f"{PERP}::clearing_house::set_position_initial_margin_ratio", [TUSD, ADMIN], obj(ch), obj(a["cap"]), obj(a["obj"]), u256(lib.IMR))
    cmds += call(f"{PERP}::account::new_assistant_account_cap", [TUSD], obj(acct["K"]["obj"]), obj(acct["K"]["cap"]), obj(registry), assign="assistant")
    cmds += call(f"{PERP}::registry::create_package_adl_cap", [], obj(registry), obj(perp_pkg_admin), assign="adl")
    cmds += ["--transfer-objects", "[assistant, adl]", obj(me)]
    j = ptb("positions, allocations, the liquidator's assistant capability, an ADL capability", cmds)
    assistant_cap = lib.owned_created(j, f"AuthorityCap<{PERP}::authority::ACCOUNT, {AUTH}::authority::ASSISTANT>")
    adl_cap = lib.owned_created(j, f"{PERP}::authority::ADL>")

    def session(label, who, actions, alloc=False):
        a = acct[who]
        cmds = refresh_prices()
        cmds += call("0x1::option::none", [f"{PERP}::account::IntegratorInfo"], assign="integrator")
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

    B9 = lib.B9
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
    session("M2 offers 4.6 at 30,000", "M2", [limit(ASK, 4_600_000_000, 30_000)])
    session("A buys 1.0 at 10x", "A", [market_order(BID, 1 * B9)])
    session("A2 buys 0.5 and bids 0.2 at 20,000", "A2", [market_order(BID, B9 // 2), limit(BID, B9 // 5, 20_000)])
    session("C buys 0.1 at 1x", "C", [market_order(BID, B9 // 10)])
    session("D buys 1.0", "D", [market_order(BID, 1 * B9)])
    session("E buys 1.0", "E", [market_order(BID, 1 * B9)])
    session("F buys 1.0 at 10x", "F", [market_order(BID, 1 * B9)])
    session("M2 bids 2.0 at 30,000", "M2", [limit(BID, 2 * B9, 30_000)])
    session("B sells 2.0 at 10x", "B", [market_order(ASK, 2 * B9)])
    wait_indexed()
    base = lambda name: sql(f"SELECT coalesce((SELECT base FROM positions WHERE market = '{ch}' AND account_id = {acct[name]['id']}), 0)")
    check("A long 1, A2 long 0.5 with a resting bid, B short 2", float(base("A")) == 1 and float(base("A2")) == 0.5 and float(base("B")) == -2,
          f"A {base('A')} A2 {base('A2')} B {base('B')}")
    check("A2's bid rests", sql(f"SELECT count(*) FROM orders WHERE account_id = {acct['A2']['id']} AND status = 'open'") == "1")

    liq_deployment = work / "liquidator.json"
    liq_deployment.write_text(json.dumps({
        "network": "localnet",
        "packages": {"perpetuals": PERP},
        "registry": registry,
        "collateral": {"coinType": TUSD, "decimals": 6, "priceFeedStorage": pfs_tusd},
        "markets": {"BTC-USD": {"marketId": "BTC-USD", "clearingHouse": ch, "basePriceFeedStorage": pfs_btc}},
    }, indent=2))

    keystore = Path(os.environ["HANEUL_CONFIG_DIR"]) / "haneul.keystore"
    base_args = [
        binary, "--deployment", str(liq_deployment), "--database-url", args.database_url,
        "--rpc-url", f"http://{args.grpc}", "--chain-id", chain_id,
        "--key-file", str(keystore), "--address", me,
        "--account", acct["K"]["obj"], "--account-cap", assistant_cap,
        "--min-gas-balance", "1", "--max-inventory-usd", "1000",
    ]
    procs = []

    def start(name, extra):
        log = open(work / f"liquidator-{name}.log", "w")
        p = subprocess.Popen(base_args + extra, stdout=log, stderr=subprocess.STDOUT, env={**os.environ, "RUST_LOG": "info,perp_liquidator=debug"})
        procs.append(p)
        return p

    def stop(p):
        p.send_signal(signal.SIGTERM)
        try:
            return p.wait(timeout=30)
        except subprocess.TimeoutExpired:
            p.kill()
            return None

    def wait_for(what, cond, seconds=40):
        deadline = time.time() + seconds
        while time.time() < deadline:
            value = cond()
            if value:
                return value
            time.sleep(0.5)
        return None

    def healthy():
        code, body = http_json("/health")
        return code == 200

    def liquidated(name):
        return int(sql(f"SELECT count(*) FROM fills WHERE account_id = {acct[name]['id']} AND kind = 'liquidated'"))

    try:
        # -------------------------------------------------------------- 1. startup check
        section("1. Startup check without a position")
        out = subprocess.run(base_args + ["--check-only"], capture_output=True, text=True, env={**os.environ, "RUST_LOG": "info"})
        check("refuses to start", out.returncode == 1, f"exit {out.returncode}")
        check("says to run with --create-positions", "--create-positions" in out.stdout + out.stderr, (out.stdout + out.stderr)[-300:])
        at = base_args.index("--chain-id") + 1
        wrong = subprocess.run([*base_args[:at], "wrong-chain", *base_args[at + 1:], "--check-only"], capture_output=True, text=True)
        check("refuses another chain's ID", wrong.returncode == 1 and "not wrong-chain" in wrong.stdout + wrong.stderr)

        # -------------------------------------------------------------- 2. live, opening its position
        section("2. Live start, opening its position")
        p = start("open", ["--create-positions"])
        check("becomes healthy", wait_for("health", healthy, 60) is not None, (work / "liquidator-open.log").read_text()[-500:])
        opened = wait_for("position", lambda: sql(f"SELECT count(*) FROM positions WHERE account_id = {acct['K']['id']}") == "1", 20)
        check("opened its position", opened is not None)
        _, status = http_json("/status")
        check("reports live mode, its account and the market", status["mode"] == "live" and status["account_id"] == acct["K"]["id"] and status["markets"][0]["ticker"] == "BTC-USD",
              json.dumps(status)[:300])
        time.sleep(3)
        check("nothing of ours touched at 30,000", sum(liquidated(n) for n in ("A", "A2", "F", "B", "C", "D", "E")) == 0)
        check("stops cleanly", stop(p) == 0)
        wait_indexed()
        # What the scenario's own accounts may already have handed it.
        held_before = abs(float(base("K")))

        # -------------------------------------------------------------- 3. dry run
        section("3. Dry run after a drop to 27,000")
        set_price(27_000)
        session("M2 bids 2.0 just under the price", "M2", [limit(BID, B9, 26_990), limit(BID, B9, 26_980)])
        wait_indexed()
        p = start("dry", ["--dry-run"])
        def simulated():
            _, s = http_json("/status")
            done = {a["account_id"] for a in (s or {}).get("recent", []) if a["kind"] == "liquidate" and a["outcome"] == "simulated"}
            return done if {acct["A"]["id"], acct["A2"]["id"]} <= done else None
        check("simulates both liquidations", wait_for("simulations", simulated) is not None, (work / "liquidator-dry.log").read_text()[-800:])
        _, s = http_json("/status")
        details = [a["detail"] for a in s["recent"] if a["account_id"] == acct["A"]["id"] and a["outcome"] == "simulated"]
        check("the simulation takes over and unwinds", bool(details) and "took over" in details[0] and "unwound 0," not in details[0], str(details[:1]))
        check("dry-run mode reported", s["mode"] == "dry-run" and "perp_liquidator_dry_run 1" in http_text("/metrics"))
        time.sleep(2)
        wait_indexed()
        check("nothing sent", liquidated("A") + liquidated("A2") == 0)
        check("dry run stops cleanly", stop(p) == 0)

        # -------------------------------------------------------------- 4. live liquidation
        section("4. Live liquidation with unwinding")
        set_price(27_000)
        p = start("live", [])
        ok = wait_for("liquidations", lambda: liquidated("A") > 0 and liquidated("A2") > 0 and liquidated("F") > 0 or None, 60)
        wait_indexed()
        check("liquidates A, A2 and F", ok is not None, (work / "liquidator-live.log").read_text()[-1500:])
        k = acct["K"]["id"]
        check("as the liquidator", sql(f"SELECT count(*) FROM fills WHERE kind = 'liquidated' AND counterparty_account_id = {k} AND account_id IN ({acct['A']['id']}, {acct['A2']['id']})") != "0")
        check("A2's resting bid was force-canceled", sql(f"SELECT count(*) FROM orders WHERE account_id = {acct['A2']['id']} AND status = 'canceled' AND cancel_reason = 1") == "1")
        same_tx = sql(
            f"SELECT count(*) FROM fills l JOIN fills t ON t.tx_digest = l.tx_digest "
            f"WHERE l.account_id = {k} AND l.kind = 'liquidation' AND t.account_id = {k} AND t.kind = 'trade' AND t.liquidity = 'taker'")
        check("sold what it took over in the same transaction", same_tx != "0", same_tx)
        # The maker's 2.0 of bids cannot take all of 2.3 taken over.
        check("keeps what the book could not take", abs(float(base("K"))) > held_before, f"{base('K')} against {held_before}")
        inventory_problem = lambda: (http_json("/status")[1] or {}).get("problems", {}).get("inventory:BTC-USD")
        check("alerts on the inventory", wait_for("inventory alert", inventory_problem, 10) is not None)
        session("M2 bids again", "M2", [limit(BID, B9, 26_990)])
        unwound = wait_for("inventory", lambda: abs(float(base("K"))) <= held_before or None, 30)
        check("unwinds the rest once bids come back", unwound is not None, f"{base('K')} against {held_before}")
        _, s = http_json("/status")
        check("as a separate unwind", any(a["kind"] == "unwind" and a["outcome"] == "executed" for a in s["recent"]))
        check("and clears the alert", wait_for("alert cleared", lambda: inventory_problem() is None or None, 10) is not None)
        sold_at = sql(f"SELECT min(price) FROM fills WHERE account_id = {k} AND kind = 'trade' AND liquidity = 'taker'")
        check("within the slippage of the mark", float(sold_at) >= 27_000 * 0.995 - 500, sold_at)
        metrics = http_text("/metrics")
        check("counts the executions", 'perp_liquidator_attempts_total{kind="liquidate",market="BTC-USD",outcome="executed"}' in metrics)
        check("healthy, untouched accounts stay untouched", sum(liquidated(n) for n in ("C", "D", "E")) == 0)
        _, s = http_json("/status")
        retried = [a for a in s["recent"] if a["outcome"] in ("rejected", "unknown")]
        check("every transaction was built on the previous one's results", not retried, str(retried[:2]))

        # -------------------------------------------------------------- 5. bad debt beyond the limits
        section("5. Bad debt only ADL can close, without the capability")
        oi = float(sql(f"SELECT open_interest FROM markets WHERE market = '{ch}'"))
        print(f"   open interest {oi}")
        set_price(45_000)
        problem = f"adl:BTC-USD:{acct['B']['id']}"
        def reported():
            _, s = http_json("/status")
            return (s or {}).get("problems", {}).get(problem)
        got = wait_for("ADL report", reported, 40)
        check("reports the position as needing ADL", got is not None and got["level"] == "critical", (work / "liquidator-live.log").read_text()[-1500:])
        check("with a plan", got is not None and "takes" in got["message"], str(got))
        code, body = http_json("/health")
        check("turns unhealthy", code == 503 and any(problem in r for r in body["reasons"]), str(body))
        check("counts the refusals", 'reason="bad_debt_beyond_limits"' in http_text("/metrics"))
        check("B still holds its short", float(base("B")) == -2, base("B"))
        check("stops cleanly", stop(p) == 0)

        # -------------------------------------------------------------- 6. ADL
        section("6. ADL with the capability")
        set_price(45_000)
        p = start("adl", ["--adl-cap", adl_cap])
        adl = lambda: int(sql(f"SELECT count(*) FROM fills WHERE kind = 'adl' AND account_id = {acct['B']['id']}")) or None
        check("auto-deleverages B", wait_for("ADL", adl, 60) is not None, (work / "liquidator-adl.log").read_text()[-1500:])
        wait_indexed()
        check("B is closed", float(base("B")) == 0, base("B"))
        winners = sql(
            f"SELECT count(*), bool_and(is_ask), bool_and(abs(price - 45000) < 450) FROM fills WHERE kind = 'adl' AND account_id <> {acct['B']['id']} "
            f"AND tx_digest IN (SELECT tx_digest FROM fills WHERE kind = 'adl' AND account_id = {acct['B']['id']})")
        count, sold, at_mark = winners.split("|")
        check("against longs, at the mark price", int(count) >= 1 and sold == "t" and at_mark == "t", winners)
        check("healthy again", wait_for("health", healthy, 20) is not None, str(http_json("/health")))
        check("C, D and E were never liquidated", sum(liquidated(n) for n in ("C", "D", "E")) == 0)

        check("stops cleanly", stop(p) == 0)

        # -------------------------------------------------------------- 7. signed prices
        section("7. Signed prices relayed in the liquidation itself")
        package_dir = Path(args.perp_dex).expanduser() / "packages/oracle_haneul"
        if not package_dir.exists():
            shutil.copytree(head / "packages/oracle_haneul", package_dir)
        out = subprocess.run([lib.HANEUL, "client", "test-publish", "--build-env", "mainnet", "--pubfile-path", str(lib.PUBFILE),
                              "--gas-budget", str(lib.GAS_BUDGET), "--json"], capture_output=True, text=True, cwd=package_dir)
        if out.returncode != 0:
            sys.exit(f"publishing oracle_haneul failed:\n{(out.stdout + out.stderr)[-2000:]}")
        published = json.loads(out.stdout)
        SIGNED = next(c["packageId"] for c in published["objectChanges"] if c["type"] == "published")
        ORACLE = P["oracle_aggregator"]
        oracle_pkg_admin = lib.owned_created(ids["oracle_aggregator"], "::authority::AuthorityCap<")
        VK = f"{E2E}::vendor_key::E2E"
        oracle_vk = owned_of(f"{AUTH}::authority::AuthorityCap<{ORACLE}::authority::VENDOR<{VK}>, {ADMIN}>")
        perp_vk = owned_of(f"{AUTH}::authority::AuthorityCap<{PERP}::authority::VENDOR<{VK}>, {ADMIN}>")
        seed = bytes([0x66] * 32)
        cmds = call(f"{SIGNED}::source::create", [ADMIN], obj(oracle_config), obj(oracle_pkg_admin), assign="src")
        cmds += call(f"{SIGNED}::source::authorize", [ADMIN], "src", obj(oracle_config), obj(oracle_pkg_admin))
        cmds += call(f"{SIGNED}::source::set_signer", [ADMIN], "src", obj(oracle_config), obj(oracle_pkg_admin), vec_u8(signing.public_key(seed)), u64(2**64 - 1), CLOCK)
        # Any step at once: the check moves the price further than a live feed would in a second.
        cmds += call(f"{SIGNED}::source::set_default_step_limit", [ADMIN], "src", obj(oracle_config), obj(oracle_pkg_admin), u64(10_000), u64(0), u64(10_000))
        cmds += ["--move-call", "0x2::transfer::public_share_object", f"<{ORACLE}::source::Source<{SIGNED}::source::HANEUL>>", "src"]
        j = ptb("a signed source with a throwaway signer", cmds)
        signed_source = lib.shared_created(j, "::source::HANEUL>")
        source_id = int(events(j, "::events::CreatedSource")[0]["source_id"])
        storage = {
            pfs_btc: int(sql(f"SELECT base_storage_id FROM markets WHERE market = '{ch}'")),
            pfs_tusd: int(sql(f"SELECT collateral_storage_id FROM markets WHERE market = '{ch}'")),
        }
        signed.update(package=SIGNED, source=signed_source, seed=seed, storage=storage)
        ts = int(time.time() * 1000) - 300
        cmds = []
        for pfs, value in ((pfs_btc, fx(45_000)), (pfs_tusd, lib.ONE)):
            cmds += call(f"{SIGNED}::price_feed_storage::new_price_feed", [VK, ADMIN], obj(signed_source), obj(oracle_vk), obj(oracle_config), obj(pfs),
                         lib.u128(value), lib.u128(0), u64(ts), vec_u8(signing.public_key(seed)), vec_u8(sign(pfs, value, ts)), u64(1), CLOCK)
        # The market now reads the signed source, and a price older than ten seconds stops it.
        cmds += call(f"{PERP}::clearing_house::set_base_oracle_params", [VK, ADMIN, TUSD], obj(ch), obj(perp_vk), obj(registry), obj(pfs_btc),
                     f"some({source_id}u16)", f"some({u64(10_000)})")
        cmds += call(f"{PERP}::clearing_house::set_collateral_oracle_params", [TUSD, ADMIN], obj(ch), obj(perp_pkg_admin), obj(registry), obj(pfs_tusd),
                     f"some({source_id}u16)", f"some({u64(10_000)})")
        ptb("feeds of the signed source, and the market switched to it", cmds)
        price[0] = fx(45_000)

        plan["G"] = 4_950
        cmds = call("0x2::coin::mint", [TUSD], obj(tusd_treasury), u64(10_000 * TUSD_UNIT), assign="coin")
        cmds += call(f"{PERP}::account::create_account", [TUSD], obj(registry), assign="acc")
        cmds += call(f"{PERP}::account::deposit_collateral", [TUSD, ADMIN], "acc.0", "acc.2", obj(registry), "coin")
        cmds += call(f"{PERP}::account::consume_policy_and_share_account", [TUSD], "acc.0", "acc.1")
        cmds += ["--transfer-objects", "[acc.2]", obj(me)]
        j = ptb("an account for G", cmds)
        g = events(j, "::events::CreatedAccount")[0]
        acct["G"] = dict(id=int(g["account_id"]), obj=g["account_obj_id"], cap=lib.owned_created(j, f"AuthorityCap<{PERP}::authority::ACCOUNT, {ADMIN}>"))
        a = acct["G"]
        cmds = call(f"{PERP}::clearing_house::create_market_position", [TUSD, ADMIN], obj(ch), obj(a["cap"]), obj(a["obj"]))
        cmds += call(f"{PERP}::clearing_house::allocate_collateral", [TUSD, ADMIN], obj(ch), obj(a["cap"]), obj(a["obj"]), u64(4_950 * TUSD_UNIT))
        cmds += call(f"{PERP}::clearing_house::set_position_initial_margin_ratio", [TUSD, ADMIN], obj(ch), obj(a["cap"]), obj(a["obj"]), u256(lib.IMR))
        ptb("G's position at 10x", cmds)
        session("M2 offers 1.0 at 45,000", "M2", [limit(ASK, B9, 45_000)])
        session("G buys 1.0", "G", [market_order(BID, B9)])
        session("M2 bids 1.0 just under 42,000", "M2", [limit(BID, B9, 41_990)])
        last_on_chain = time.time()
        wait_indexed()
        check("G long 1 on the signed source's price", float(base("G")) == 1, base("G"))
        check("the market reads the signed source", sql(f"SELECT base_source_id FROM markets WHERE market = '{ch}'") == str(source_id))

        service = {"btc": fx(45_000)}

        class Updates(BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802
                ts = int(time.time() * 1000) - 300
                body = {
                    "packageId": SIGNED,
                    "sourceId": signed_source,
                    "aggregatorConfigId": oracle_config,
                    "updates": [
                        {"symbol": symbol, "storageId": storage[pfs], "priceFeedStorageId": pfs, "price": str(value), "confidence": "0",
                         "timestampMs": str(ts), "publicKey": signing.public_key(seed).hex(), "signature": sign(pfs, value, ts).hex()}
                        for symbol, pfs, value in (("BTC/USD", pfs_btc, service["btc"]), ("TUSD/USD", pfs_tusd, lib.ONE))
                    ],
                }
                data = json.dumps(body).encode()
                self.send_response(200)
                self.send_header("content-type", "application/json")
                self.end_headers()
                self.wfile.write(data)

            def log_message(self, *_):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 18788), Updates)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        try:
            p = start("oracle", ["--oracle-updates-url", "http://127.0.0.1:18788/v1/updates"])
            check("starts with the oracle service", wait_for("health", healthy, 60) is not None, (work / "liquidator-oracle.log").read_text()[-800:])
            check("knows which source the service signs for", f"source_id={source_id}" in (work / "liquidator-oracle.log").read_text().replace("\x1b[3m", "").replace("\x1b[0m", "").replace("\x1b[2m", ""))
            # Nobody relays: let the chain's price go stale.
            time.sleep(max(0, 11 - (time.time() - last_on_chain)))
            before = sql(f"SELECT price, updated_checkpoint FROM oracle_prices WHERE source_id = {source_id} AND storage_id = {storage[pfs_btc]}")
            check("the chain's price is 45,000 and stale", before.startswith("45000"), before)
            check("G untouched while the service says 45,000", liquidated("G") == 0)
            service["btc"] = fx(42_000)
            got = wait_for("G liquidated", lambda: liquidated("G") or None, 40)
            wait_indexed()
            check("liquidates G on the service's price", got is not None, (work / "liquidator-oracle.log").read_text()[-1500:])
            liq_checkpoint = sql(f"SELECT checkpoint FROM fills WHERE account_id = {acct['G']['id']} AND kind = 'liquidated' LIMIT 1")
            after = sql(f"SELECT price, updated_checkpoint FROM oracle_prices WHERE source_id = {source_id} AND storage_id = {storage[pfs_btc]}")
            check("with the signed price in the same transaction", after == f"{after.split('|')[0]}|{liq_checkpoint}" and after.startswith("42000"), f"{after} liquidated at {liq_checkpoint}")
            mark = sql(f"SELECT mark_price FROM fills WHERE account_id = {acct['G']['id']} AND kind = 'liquidated' LIMIT 1")
            check("at the new mark price", abs(float(mark) - 42_000) < 420, mark)
            relayed = [line for line in http_text("/metrics").splitlines() if line.startswith("perp_liquidator_oracle_updates_included_total")]
            check("counts the relayed updates", relayed and float(relayed[0].split()[-1]) >= 2, str(relayed))
            check("and unwinds into the bids under it", sql(
                f"SELECT count(*) FROM fills l JOIN fills t ON t.tx_digest = l.tx_digest WHERE l.account_id = {acct['G']['id']} AND l.kind = 'liquidated' "
                f"AND t.account_id = {k} AND t.kind = 'trade' AND t.liquidity = 'taker'") != "0")
            _, s = http_json("/status")
            stale = [a for a in s["recent"] if "stale_oracle" in a["outcome"]]
            check("never refused for a stale price", not stale, str(stale[:1]))

            # -------------------------------------------------------------- 8. shutdown
            section("8. Shutdown")
            check("stops cleanly on SIGTERM", stop(p) == 0)
        finally:
            server.shutdown()
    finally:
        for p in procs:
            if p.poll() is None:
                p.kill()

    failed = [n for n, ok in lib.RESULTS if not ok]
    print(f"\n{len(lib.RESULTS) - len(failed)}/{len(lib.RESULTS)} checks passed")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
