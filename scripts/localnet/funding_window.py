#!/usr/bin/env python3
# Copyright (c) 2026 Geunhwa Jeong
# SPDX-License-Identifier: Apache-2.0

"""Measure on a local network what the premium TWAP period does to funding.

    scripts/localnet/mark_drag.py      up      --work <new scratch directory>
    scripts/localnet/funding_window.py run     --work <the same directory>
    scripts/localnet/funding_window.py funding --work <the same directory>
    scripts/localnet/mark_drag.py      down    --work <the same directory>

Funding is settled at the funding time as the premium TWAP times the funding frequency over the
funding period, one eighth on mainnet. A premium sample is the book's mid price less the index,
clipped to `max_funding_rate` of the index, and the TWAP weighs a sample by the time since the
last one over `premium_twap_period_ms`. The mid price is the mean of the best bid and the best
ask whatever their sizes. With a period of 60 seconds, the hour's funding is the mid price of
the minute before the hour, and with the makers gone two one-lot orders set it.

The stack is the one `mark_drag.py up` builds: the mainnet funding schedule, a BTC/USD market at
100,000 with an empty book, and a relay that keeps the index there. Each `up` takes one `run`
and one `funding`, in that order or either alone.

`run` sets `max_book_index_spread` to mainnet's 1%, then:

A. period 60 s: rests a one-lot bid 6% and a one-lot ask 8% above the index, leaves the market
   alone for 65 seconds, and samples once;
T. sets the period to one hour with the TWAP where A left it, cancels the orders, and samples
   an empty book after 65 seconds: the call does not reset the TWAP;
R. sets the period back to 60 s and samples an empty book after 65 seconds;
B. period 60 s: the same orders, sampled every few seconds for 65 seconds, as a market that
   trades is;
C. sets the period to one hour with the TWAP at zero, then the same as A;
D. period one hour: the orders stay 65 seconds more, sampled every few seconds.

It leaves the period at one hour, the TWAP at zero and the book empty.

`funding` opens a short and a long of 0.1 BTC, waits for the next funding time with the period
at one hour, rests the same two orders 70 seconds before it, calls `update_funding`, and reads
what the cumulative funding rate moved and what `settle_position_funding` paid each position.
The funding times are multiples of the funding frequency, so with the mainnet schedule it ends
just after the next hour of the clock.

On 2026-11-24, with the engine at 2d353bc: A read 499.99 with the cap at 500, which settles as
0.0625% of the index an hour; T read 490.01 and R 0.01; B read 332.04; C read 9.12 and D 17.77.
At the funding time the TWAP was 10.07, the cumulative rate moved 1.2592 per BTC (0.00126% of
the index), and the short of 0.1 BTC was paid 0.1259 by the long. Mainnet's period was set from
60 seconds to one hour that day.

The fixture's relay pays from the administrator's gas coin every three seconds. The calls that
take no capability are paid by an address of their own, and a transaction that loses the gas
coin to the relay, or finds the index older than the market allows because the relay lost its
own round, is sent again after a random wait.

Needs grpcurl and Node 22. Refuses to run anywhere but a local chain.
"""

import argparse
import json
import random
import subprocess
import sys
import time

sys.dont_write_bytecode = True
import mark_drag as md  # noqa: E402

ONE = md.ONE
HOUR_MS = 3_600_000
MINUTE_MS = 60_000


class FundingWindow(md.Run):
    def __init__(self, s):
        super().__init__(s)
        self.out.close()
        self.out = open(s.logs / "funding-window-results.txt", "a")
        self._payer = None

    def say(self, *a):
        line = time.strftime("%H:%M:%SZ ", time.gmtime()) + " ".join(str(x) for x in a)
        print(line, flush=True)
        self.out.write(line + "\n")
        self.out.flush()

    # ------------------------------------------------------------ transactions

    def _ptb(self, args, tries=14):
        """A retry on a fixed interval loses the gas coin to the relay again and again: the wait
        is random. An index older than the market's ten seconds is the relay's lost round."""
        p = None
        for _ in range(tries):
            p = subprocess.run([self.s.haneul, "client", "ptb", *args], capture_output=True, text=True, env=self.s.env, cwd=self.s.work)
            text = p.stdout + p.stderr
            stale = "code 1000" in text and "oracle_price" in text
            if p.returncode == 0 or (("MoveAbort" in text or "aborted within function" in text) and not stale):
                return p
            time.sleep(0.7 + random.random() * 2.5)
        return p

    def payer(self):
        """An address with gas from the faucet, for the calls that take no capability."""
        if self._payer is None:
            out = self.s.cli("new-address", "ed25519", f"payer{int(time.time())}", "--json")
            address = json.loads(out[out.find("{"):])["address"]
            self.s.cli("faucet", "--address", address)
            time.sleep(5)
            self._payer = address
            self.say(f"calls that take no capability are paid by {address[:12]}, an address of its own")
        return self._payer

    def set_period(self, period_ms):
        j, _ = self.ptb("set_twap_params", md.call(
            f"{self.perp}::clearing_house::set_twap_params", [self.vk, self.admin, self.tusd],
            md.obj(self.ch), md.obj(self.perp_vk), md.obj(self.registry),
            "none", "none", "none", f"some({period_ms}u64)", "none", "none", "@0x6"))
        ev = self.events(j, "::events::SetTwapParams")[0]
        self.say(
            f"set_twap_params: funding {ev['funding_frequency_ms']}/{ev['funding_period_ms']}, "
            f"premium {ev['premium_twap_frequency_ms']}/{ev['premium_twap_period_ms']}, "
            f"spread {ev['spread_twap_frequency_ms']}/{ev['spread_twap_period_ms']}"
        )

    def sample(self):
        j, _ = self.ptb("update_twaps", md.call(
            f"{self.perp}::clearing_house::update_twaps", [self.tusd], md.obj(self.ch), md.obj(self.pfs_btc), "@0x6"), sender=self.payer())
        ev = self.events(j, "::events::UpdatedPremiumTwap")
        return ev[0] if ev else None

    def sampled_for(self, seconds):
        began, rounds = time.time(), 0
        while time.time() - began < seconds:
            self.sample()
            rounds += 1
            time.sleep(1.5)
        return rounds, time.time() - began

    def empty_book_window(self, label):
        self.quiet("an empty book")
        self.sample()
        return self.show(label)

    # ------------------------------------------------------------ reads

    def market(self):
        return self.get_object(self.ch)["json"]

    def index(self):
        return int(self.get_object(self.pfs_btc)["json"]["feeds"][0]["price"])

    def cap(self, market, index):
        return index * int(market["market_params"]["limits_params"]["max_funding_rate"]) // ONE

    def show(self, label):
        j, index = self.market(), self.index()
        premium = md.signed(j["market_state"]["premium_twap"])
        cap = self.cap(j, index)
        twap = j["market_params"]["twap_params"]
        share = int(twap["funding_frequency_ms"]) / int(twap["funding_period_ms"])
        self.say(
            f"{label}: premium_twap {premium / ONE:+,.2f} ({premium / cap * 100:.1f}% of the cap {cap / ONE:,.0f}), "
            f"period {int(twap['premium_twap_period_ms']) // 1000} s -> the next settlement moves "
            f"{premium * share / ONE:+,.3f} per BTC ({premium * share / index * 100:+.5f}% of the index)"
        )
        return premium

    def start(self, period_ms):
        """An empty book, and the period the step begins with."""
        self.s.assert_local()
        j = self.market()
        if j["orderbook"]["best_bid_price"] is not None or j["orderbook"]["best_ask_price"] is not None:
            sys.exit("the book is not empty: this market has been run on; take it down and up again")
        if int(j["market_params"]["twap_params"]["premium_twap_period_ms"]) != period_ms:
            self.set_period(period_ms)
        return j

    # ------------------------------------------------------------ run

    def run(self):
        self.start(MINUTE_MS)
        self.say("=== the premium TWAP period and funding ===")
        self.payer()
        self.set_limit(100)
        self.say(f"twap_params: {self.market()['market_params']['twap_params']}")
        self.show("start")

        self.say("--- A: period 60 s, two one-lot orders, the market left alone for 65 s, one sample")
        ids = self.far_orders()
        self.quiet("the premium TWAP period is 60 s")
        ev = self.sample()
        self.say(f"A sample: book {md.usd(ev['actual_book_price'])}, clipped {md.usd(ev['clipped_book_price'])}, index {md.usd(ev['index_price'])}")
        self.show("A")

        self.say("--- T: the period set to one hour with the TWAP where A left it, then an empty book for 65 s")
        self.set_period(HOUR_MS)
        self.cancel(ids)
        self.empty_book_window("T")

        self.say("--- R: the period set back to 60 s, then an empty book for 65 s")
        self.set_period(MINUTE_MS)
        self.empty_book_window("R")

        self.say("--- B: period 60 s, the same orders, sampled every few seconds for 65 s")
        ids = self.far_orders()
        rounds, took = self.sampled_for(65)
        self.say(f"B: {rounds} samples in {took:.1f} s")
        self.show("B")
        self.cancel(ids)
        self.empty_book_window("B reset")

        self.say("--- C: the period set to one hour with the TWAP at zero, then the same as A")
        self.set_period(HOUR_MS)
        ids = self.far_orders()
        self.quiet("the same 65 s as A")
        self.sample()
        self.show("C")

        self.say("--- D: period 1 h, the orders stay, sampled every few seconds for 65 s more")
        rounds, took = self.sampled_for(65)
        self.say(f"D: {rounds} samples in {took:.1f} s")
        self.show("D")

        self.say("--- back to zero: the orders canceled, 60 s, an empty book, one hour again")
        self.cancel(ids)
        self.set_period(MINUTE_MS)
        self.empty_book_window("zero")
        self.set_period(HOUR_MS)
        self.show("end")
        self.say("=== done ===")

    # ------------------------------------------------------------ funding

    def funding_now(self):
        j, _ = self.ptb("update_funding", md.call(
            f"{self.perp}::clearing_house::update_funding", [self.tusd], md.obj(self.ch), md.obj(self.pfs_btc), "@0x6"), sender=self.payer())
        return self.events(j, "::events::UpdatedPremiumTwap"), self.events(j, "::events::UpdatedFunding")

    def settle(self, who, account_id):
        j, _ = self.ptb("settle_position_funding", md.call(
            f"{self.perp}::clearing_house::settle_position_funding", [self.tusd], md.obj(self.ch), md.obj(self.pfs_tusd),
            f"{account_id}u64", "@0x6"), sender=self.payer())
        settled = self.events(j, "::events::SettledFunding")
        for e in settled:
            self.say(f"{who} (account {e['account_id']}): funding settled {md.signed(e['collateral_change_usd']) / ONE:+,.4f}")
        if not settled:
            self.say(f"{who} (account {account_id}): nothing to settle")

    def funding(self, lead_s):
        twap = self.start(HOUR_MS)["market_params"]["twap_params"]
        frequency, period = int(twap["funding_frequency_ms"]), int(twap["funding_period_ms"])
        self.say("=== the premium TWAP period and funding, the funding time ===")
        self.say(f"twap_params: {self.market()['market_params']['twap_params']}")
        victim = self.open_victim()
        self.payer()

        funding_time = (int(time.time() * 1000) // frequency + 1) * frequency / 1000
        wait = funding_time - lead_s - time.time()
        if wait < 0:
            sys.exit(f"the next funding time is {funding_time - time.time():.0f} s away: too close for a lead of {lead_s} s")
        self.say(f"next funding time {time.strftime('%H:%M:%SZ', time.gmtime(funding_time))}; waiting {wait:.0f} s, nobody touches the market")
        time.sleep(wait)

        before = md.signed(self.market()["market_state"]["cum_funding_rate_long"])
        self.show(f"{lead_s} s before the funding time, before the orders")
        ids = self.far_orders()
        time.sleep(max(0.0, funding_time + 2.5 - time.time()))

        # The chain's clock runs a little behind: funding is settled once it has passed the time.
        funded = None
        for _ in range(30):
            premium_events, funding_events = self.funding_now()
            for e in premium_events:
                self.say(f"update_funding sampled: book {md.usd(e['actual_book_price'])}, clipped {md.usd(e['clipped_book_price'])}, "
                         f"premium_twap {md.usd(e['premium_twap'])} at {e['premium_twap_last_upd_ms']}")
            if funding_events:
                funded = funding_events[0]
                break
            time.sleep(1)
        if funded is None:
            sys.exit("no funding was settled in 30 calls")

        j, index = self.market(), self.index()
        delta = md.signed(funded["cum_funding_rate_long"]) - before
        premium = md.signed(j["market_state"]["premium_twap"])
        cap = self.cap(j, index)
        self.say(f"FUNDING SETTLED at {funded['funding_last_upd_ms']}: the cumulative rate moved {delta / ONE:+,.4f} per BTC "
                 f"({delta / index * 100:+.5f}% of the index)")
        self.say(f"premium_twap {premium / ONE:+,.4f} times {frequency}/{period} is {premium * frequency / period / ONE:+,.4f} per BTC")
        self.say(f"with the TWAP at the cap of {cap / ONE:,.0f}, which A reads with the period at 60 s, the same settlement moves "
                 f"{cap * frequency / period / ONE:+,.4f} per BTC ({cap * frequency / period / index * 100:+.5f}%)")
        self.settle("the short of 0.1 BTC", victim["id"])
        self.settle("the long of 0.1 BTC, the maker account", self.maker["id"])
        self.cancel(ids)
        self.show("end")
        self.say("=== done ===")


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("step", choices=["run", "funding"])
    parser.add_argument("--work", required=True, help="the directory mark_drag.py brought up")
    parser.add_argument("--perp-dex", default="~/perp-dex", help="the engine checkout, read only")
    parser.add_argument("--web", default="~/perp-dex-web", help="the front end checkout, read only")
    parser.add_argument("--haneul", help="the CLI binary; <perp-dex>/deps/bin/haneul by default")
    parser.add_argument("--node", default="node", help="a Node 22 binary")
    parser.add_argument("--lead", type=int, default=70, help="funding: seconds before the funding time at which the orders are rested")
    args = parser.parse_args()
    stack = md.Stack(args)
    if not stack.pids_file.exists():
        sys.exit(f"no {stack.pids_file}: bring the stack up with mark_drag.py first")
    measurement = FundingWindow(stack)
    if args.step == "run":
        measurement.run()
    else:
        measurement.funding(args.lead)


if __name__ == "__main__":
    main()
