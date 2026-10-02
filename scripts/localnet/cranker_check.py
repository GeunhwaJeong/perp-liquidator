#!/usr/bin/env python3
# Copyright (c) 2026 Geunhwa Jeong
# SPDX-License-Identifier: Apache-2.0
"""Run the funding cranker against a local network and check what it does.

    scripts/localnet/cranker_check.py --perp-dex <engine copy> --work <localnet work dir> \\
        --cranker <perp-cranker binary>

Expects the stack the indexer's `scripts/localnet/run_all.sh` leaves up when run with HOLD, as
`check.py` does, on a fresh network: its BTC-USD market funds every minute and samples its TWAPs
every second. Nobody trades during the check, so only the cranker updates the market, while the
check keeps the market's prices fresh as a relayer would. The cranker signs with a key of its
own. Then:

1. startup checks pass, and another chain's ID is refused;
2. live on the quiet market, it samples the TWAPs every --twap-min-interval-ms and settles
   funding at each minute boundary, never cranking when nothing is due;
3. in a dry run it simulates and sends nothing;
4. it leaves a paused market alone and picks it up again once resumed;
5. stopped for more than three funding intervals (what the engine catches up), it starts by
   raising a critical alert that funding is being lost, settles, and clears it;
6. with the market on a signed source whose price goes stale after ten seconds and nothing
   relaying, its cranks are refused for the stale price and the market is backed off;
7. given the oracle service, it puts the signed base price in front of each crank and the
   market is current again;
8. it exits with code 3 when the deployment file changes, and cleanly on SIGTERM.

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
STATUS_URL = "http://127.0.0.1:9189"
TWAP_MIN_INTERVAL_MS = 5_000


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


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--perp-dex", required=True)
    parser.add_argument("--work", required=True)
    parser.add_argument("--cranker", required=True)
    parser.add_argument("--grpc", default="127.0.0.1:9000")
    parser.add_argument(
        "--engine-head",
        default="~/perp-dex",
        help="an engine checkout with the oracle_haneul package and e2e/oracle_signing.py, read only",
    )
    args = parser.parse_args()

    work = Path(args.work).resolve()
    binary = str(Path(args.cranker).resolve())
    lib = load_lib(args.perp_dex)
    os.chdir(Path(args.perp_dex).expanduser())
    lib.safety_check()

    call, obj, u64, fx = lib.call, lib.obj, lib.u64, lib.fx
    # The price pusher and the check send from the same address: one transaction at a time, or
    # two of them pick the same gas coin and one is refused.
    admin = threading.Lock()

    def ptb(*a, **k):
        with admin:
            return lib.ptb(*a, **k)

    events, section, check, CLOCK = lib.events, lib.section, lib.check, lib.CLOCK

    state = json.loads(STATE_FILE.read_text())
    P, ids, oracle = state["packages"], state["publish_tx"], state["oracle"]
    AUTH, PERP, E2E = P["authority_cap"], P["perpetuals"], P["perp_e2e"]
    ADMIN = f"{AUTH}::authority::ADMIN"
    TUSD = f"{E2E}::tusd::TUSD"
    registry = lib.shared_created(ids["perpetuals"], "::registry::Registry")
    perp_pkg_admin = lib.owned_created(ids["perpetuals"], "::authority::AuthorityCap<")
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

    def schedule():
        j = object_json(ch)
        state = j["market_state"]
        return {
            "paused": int(j["paused"]),
            "funding": int(state["funding_last_upd_ms"]),
            "premium": int(state["premium_twap_last_upd_ms"]),
            "spread": int(state["spread_twap_last_upd_ms"]),
        }

    chain_id = grpc("haneul.rpc.v2.LedgerService/GetServiceInfo", {})["chainId"]

    # The cranker's own key: it needs gas and nothing else.
    created = json.loads(lib.cli("client", "new-address", "ed25519", "--json").stdout)
    cranker = created["address"]
    lib.cli("client", "faucet", "--address", cranker)
    keystore = Path(os.environ["HANEUL_CONFIG_DIR"]) / "haneul.keystore"

    # The market's prices, kept fresh while the check wants them fresh: the scenario's mock
    # source until phase 6, then a signed source.
    price = fx(30_000)
    pushing = threading.Event()
    pushing.set()
    signed = {}

    def push_prices():
        cmds = (call(f"{E2E}::mock_source::set_price", [], obj(source), obj(oracle_config), obj(pfs_btc), lib.u128(price), CLOCK)
                + call(f"{E2E}::mock_source::set_price", [], obj(source), obj(oracle_config), obj(pfs_tusd), lib.u128(lib.ONE), CLOCK))
        ptb("refresh prices", cmds)

    def pusher():
        while True:
            if pushing.is_set() and not signed:
                try:
                    push_prices()
                except Exception as e:  # noqa: BLE001
                    print(f"   price push failed: {str(e)[:200]}")
            time.sleep(2)

    push_prices()
    threading.Thread(target=pusher, daemon=True).start()

    cranker_deployment = work / "cranker.json"
    cranker_deployment.write_text(json.dumps({
        "network": "localnet",
        "packages": {"perpetuals": PERP},
        "registry": registry,
        "collateral": {"coinType": TUSD, "decimals": 6, "priceFeedStorage": pfs_tusd},
        "markets": {"BTC-USD": {"marketId": "BTC-USD", "clearingHouse": ch, "basePriceFeedStorage": pfs_btc}},
    }, indent=2))
    base_args = [
        binary, "--deployment", str(cranker_deployment), "--rpc-url", f"http://{args.grpc}",
        "--chain-id", chain_id, "--key-file", str(keystore), "--address", cranker,
        "--poll-interval-ms", "500", "--twap-min-interval-ms", str(TWAP_MIN_INTERVAL_MS),
        "--min-gas-balance", "1",
    ]
    procs = []

    def start(name, extra=(), deployment_file=None):
        log = open(work / f"cranker-{name}.log", "w")
        argv = list(base_args)
        if deployment_file is not None:
            argv[argv.index("--deployment") + 1] = str(deployment_file)
        p = subprocess.Popen(argv + list(extra), stdout=log, stderr=subprocess.STDOUT,
                             env={**os.environ, "RUST_LOG": "info,perp_cranker=debug", "NO_COLOR": "1"})
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

    def log_of(name):
        return (work / f"cranker-{name}.log").read_text()

    try:
        # -------------------------------------------------------------- 1. startup
        section("1. Startup checks")
        out = subprocess.run(base_args + ["--check-only"], capture_output=True, text=True, env={**os.environ, "RUST_LOG": "info"})
        check("the startup checks pass", out.returncode == 0, (out.stdout + out.stderr)[-400:])
        at = base_args.index("--chain-id") + 1
        wrong = subprocess.run([*base_args[:at], "wrong-chain", *base_args[at + 1:], "--check-only"], capture_output=True, text=True)
        check("refuses another chain's ID", wrong.returncode == 1 and "not wrong-chain" in wrong.stdout + wrong.stderr)

        # -------------------------------------------------------------- 2. live on a quiet market
        section("2. A quiet market, live")
        before = schedule()
        p = start("live")
        check("becomes healthy", wait_for(healthy, 60) is not None, log_of("live")[-500:])
        samples, last = [], before["premium"]
        started = time.time()
        while time.time() - started < 75:
            s = schedule()
            if s["premium"] != last:
                samples.append(s["premium"])
                last = s["premium"]
            time.sleep(0.5)
        after = schedule()
        m = metrics()
        cranks = metric(m, "perp_cranker_cranks_total", market="BTC-USD", result="executed")
        gaps = [b - a for a, b in zip(samples, samples[1:])]
        boundary = (before["funding"] // 60_000 + 1) * 60_000
        check("settled funding at the minute boundary", after["funding"] >= boundary, f"{before['funding']} -> {after['funding']}")
        check("sampled the premium TWAP throughout", len(samples) >= 10, f"{len(samples)} samples")
        # A funding crank also samples the TWAPs that are due by the market's own interval, so the
        # sample at a minute boundary may come sooner than the minimum; every other one waits it.
        early = [g for g in gaps if g < TWAP_MIN_INTERVAL_MS]
        check("about every --twap-min-interval-ms", gaps and max(gaps) <= TWAP_MIN_INTERVAL_MS + 6_000,
              f"gaps {min(gaps or [0])}..{max(gaps or [0])} ms")
        check("sooner only when funding fell due", len(early) <= 2, f"{len(early)} early gaps: {early}")
        check("one crank per sample, nothing sent when nothing was due", len(samples) <= cranks <= len(samples) + 2, f"{cranks:.0f} cranks")
        failed = sum(metric(m, "perp_cranker_cranks_total", market="BTC-USD", result=r) for r in ("refused", "rejected", "unknown", "failed"))
        check("no crank failed", failed == 0, f"{failed:.0f}")
        check("spent gas on them", metric(m, "perp_cranker_gas_spent_total") > 0)
        check("stops cleanly", stop(p) == 0)

        # -------------------------------------------------------------- 3. dry run
        section("3. Dry run")
        before = schedule()
        p = start("dry", ["--dry-run"])
        wait_for(healthy, 60)
        simulated = wait_for(lambda: metric(metrics(), "perp_cranker_cranks_total", market="BTC-USD", result="simulated") or None, 30)
        check("simulates the due cranks", simulated is not None)
        time.sleep(3)
        check("and sends none", schedule()["premium"] == before["premium"], f"{before['premium']} -> {schedule()['premium']}")
        check("stops cleanly", stop(p) == 0)

        # -------------------------------------------------------------- 4. paused market
        section("4. A paused market")
        cmds = call(f"{PERP}::registry::create_package_pause_guardian_cap", [], obj(registry), obj(perp_pkg_admin), assign="guardian")
        cmds += call(f"{PERP}::clearing_house::admin_pause_market", [TUSD], obj(ch), "guardian", obj(registry), "1u8")
        cmds += ["--transfer-objects", "[guardian]", obj(me)]
        ptb("pause the market", cmds)
        before = schedule()
        check("the market is paused", before["paused"] == 1)
        p = start("paused")
        wait_for(healthy, 60)
        skipped = wait_for(lambda: metric(metrics(), "perp_cranker_skipped_total", market="BTC-USD", reason="paused") or None, 20)
        check("leaves it alone", skipped is not None and schedule()["premium"] == before["premium"])
        check("and sends nothing", metric(metrics(), "perp_cranker_cranks_total", market="BTC-USD", result="executed") == 0)
        ptb("resume the market", call(f"{PERP}::clearing_house::admin_resume_market", [TUSD, ADMIN], obj(ch), obj(perp_pkg_admin), obj(registry)))
        resumed = wait_for(lambda: schedule()["premium"] != before["premium"], 30)
        check("picks it up once resumed", resumed is not None)
        check("stops cleanly", stop(p) == 0)

        # -------------------------------------------------------------- 5. funding being lost
        section("5. Funding missed for more than the engine catches up")
        s = schedule()
        # Three intervals have ended without an update once the clock is two intervals past the
        # next boundary.
        lost_at = (s["funding"] // 60_000 + 1) * 60_000 + 2 * 60_000
        wait_s = max(0, lost_at / 1000 - time.time()) + 3
        print(f"   waiting {wait_s:.0f} s with nobody cranking")
        time.sleep(wait_s)
        p = start("lost")
        current = wait_for(lambda: schedule()["funding"] >= int(time.time() * 1000) // 60_000 * 60_000 or None, 60)
        check("settles the missed funding", current is not None, str(schedule()))
        log = log_of("lost")
        check("raised a critical alert that funding was being lost", "funding is being lost" in log, log[-600:])
        check("and counted it", metric(metrics(), "perp_cranker_alerts_total", level="critical") >= 1)
        check("cleared it once current", wait_for(lambda: "funding is current again" in log_of("lost"), 20) is not None)
        check("is healthy", wait_for(healthy, 20) is not None, str(http_json("/health")))
        check("stops cleanly", stop(p) == 0)

        # -------------------------------------------------------------- 6. stale prices, nothing relaying
        section("6. A signed source nobody relays")
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

        def vec_u8(data):
            return "vector[" + ",".join(f"{x}u8" for x in data) + "]"

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
        for pfs, value in ((pfs_btc, price), (pfs_tusd, lib.ONE)):
            cmds += call(f"{SIGNED}::price_feed_storage::new_price_feed", [VK, ADMIN], obj(signed_source), obj(oracle_vk), obj(oracle_config), obj(pfs),
                         lib.u128(value), lib.u128(0), u64(ts), vec_u8(signing.public_key(seed)), vec_u8(sign(pfs, value, ts)), u64(1), CLOCK)
        cmds += call(f"{PERP}::clearing_house::set_base_oracle_params", [VK, ADMIN, TUSD], obj(ch), obj(perp_vk), obj(registry), obj(pfs_btc),
                     f"some({source_id}u16)", f"some({u64(10_000)})")
        cmds += call(f"{PERP}::clearing_house::set_collateral_oracle_params", [TUSD, ADMIN], obj(ch), obj(perp_pkg_admin), obj(registry), obj(pfs_tusd),
                     f"some({source_id}u16)", f"some({u64(30_000)})")
        ptb("feeds of the signed source, and the market switched to it", cmds)
        signed.update(source=signed_source)
        print("   waiting 12 s so the base price is older than the market's 10 s tolerance")
        time.sleep(12)
        p = start("stale")
        backoff = wait_for(lambda: metric(metrics(), "perp_cranker_skipped_total", market="BTC-USD", reason="backoff") or None, 40)
        check("its cranks are refused and the market is backed off", backoff is not None, log_of("stale")[-600:])
        _, status = http_json("/status")
        problem = (status or {}).get("problems", {}).get("failing:BTC-USD", {})
        check("and says why", "stale" in problem.get("message", ""), str(problem))
        check("nothing was sent", metric(metrics(), "perp_cranker_cranks_total", market="BTC-USD", result="executed") == 0)
        check("stops cleanly", stop(p) == 0)

        # -------------------------------------------------------------- 7. with the oracle service
        section("7. The signed base price in front of each crank")

        class Updates(BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802
                ts = int(time.time() * 1000) - 300
                body = {
                    "packageId": SIGNED, "sourceId": signed_source, "aggregatorConfigId": oracle_config,
                    "updates": [
                        {"symbol": symbol, "storageId": storage[pfs], "priceFeedStorageId": pfs, "price": str(value), "confidence": "0",
                         "timestampMs": str(ts), "publicKey": signing.public_key(seed).hex(), "signature": sign(pfs, value, ts).hex()}
                        for symbol, pfs, value in (("BTC/USD", pfs_btc, price), ("TUSD/USD", pfs_tusd, lib.ONE))
                    ],
                }
                data = json.dumps(body).encode()
                self.send_response(200)
                self.send_header("content-type", "application/json")
                self.end_headers()
                self.wfile.write(data)

            def log_message(self, *_):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 18789), Updates)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        before = schedule()
        p = start("oracle", ["--oracle-updates-url", "http://127.0.0.1:18789/v1/updates"])
        cranked = wait_for(lambda: metric(metrics(), "perp_cranker_cranks_total", market="BTC-USD", result="executed") or None, 40)
        check("cranks go through again", cranked is not None, log_of("oracle")[-600:])
        check("with the signed price relayed in them", metric(metrics(), "perp_cranker_oracle_updates_included_total") >= 1)
        feed = next(f for f in object_json(pfs_btc)["feeds"] if int(f["source_id"]) == source_id)
        check("the base feed's price is fresh on chain", int(time.time() * 1000) - int(feed["timestamp_ms"]) < 15_000, str(feed))
        check("the market is sampled again", schedule()["premium"] > before["premium"])
        check("stops cleanly", stop(p) == 0)
        server.shutdown()

        # -------------------------------------------------------------- 8. deployment change, shutdown
        section("8. A changed deployment file, and shutdown")
        copy = work / "cranker-changing.json"
        shutil.copy(cranker_deployment, copy)
        p = start("changed", ["--config-check-secs", "1", "--oracle-updates-url", "http://127.0.0.1:18789/v1/updates"], deployment_file=copy)
        wait_for(healthy, 60)
        copy.write_text(copy.read_text().replace('"network": "localnet"', '"network": "localnet", "note": "changed"'))
        try:
            code = p.wait(timeout=20)
        except subprocess.TimeoutExpired:
            code = None
        check("exits with code 3 so its supervisor restarts it", code == 3, f"exit {code}")
        p = start("term")
        wait_for(healthy, 60)
        check("stops cleanly on SIGTERM", stop(p) == 0)
    finally:
        pushing.clear()
        for p in procs:
            if p.poll() is None:
                p.kill()

    failed = [n for n, ok in lib.RESULTS if not ok]
    print(f"\n{len(lib.RESULTS) - len(failed)}/{len(lib.RESULTS)} checks passed")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
