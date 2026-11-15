#!/usr/bin/env python3
# Copyright (c) 2026 Geunhwa Jeong
# SPDX-License-Identifier: Apache-2.0

"""Check on a local network what the front end does with an account that was sent to a wallet.

    scripts/localnet/first_cap.py up   --work <new scratch directory>
    scripts/localnet/first_cap.py run  --work <the same directory> [--web <another checkout>]
    scripts/localnet/first_cap.py down --work <the same directory>

An account's admin capability is an object, and anyone can send one to a wallet. Its sender can
still act on the account it names in two ways: with an assistant capability minted before the
transfer, which allocates the account's balance and trades with it, and with a stop order ticket
created before it, which the ticket's executor runs with no capability and which allocates the
balance to the order it commits to. A front end that takes such an account for the wallet's own
deposits into it, and the sender trades the deposit away: its own account rests one lot far from
the index, the sent account takes it and gives it back at the index.

`run` prepares one account of each kind, sends each to a new wallet with 10,000 TUSD, and has the
wallet deposit through the front end's own code (`src/haneul/perp` of the checkout, run by a
small script this one writes into the work directory):

1. an account with an assistant capability;
2. an account with two tickets that name the sender as their executor;
3. a wallet nobody sent anything to.

The front end is expected to refuse the deposits of 1 and 2 and leave the wallets as they were,
to let the wallet of 2 cancel the tickets and deposit after that, and to open the wallet of 3 an
account of its own. Where a deposit goes through instead, the sender takes what it can, and the
run reports the amount and fails. `--web` names the checkout whose code deposits, for a look at
an older front end on the same stack.

On 2026-11-15, with the engine at 2d353bc: the front end at 7d7e29af deposited into both accounts
and the sender took 8,898.18 of the 10,000 TUSD each time; the front end at 617a4865 refused
both.

`up` and `down` are those of mark_drag.py: a stack of its own inside the work directory, which
has to be a new directory each time. Needs grpcurl and Node 22. Refuses to run anywhere but a
local chain.
"""

import argparse
import hashlib
import json
import subprocess
import sys
import time
from pathlib import Path

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent))
import mark_drag as md  # noqa: E402
from mark_drag import B9, LOT, ONE, TUSD_UNIT, call, obj  # noqa: E402

DEPOSIT = 10_000  # TUSD each wallet holds and deposits
STAKE = 2_000  # TUSD each sender starts with, all of it in an account of its own
FAR = 9_000_000  # the price the sent account is made to buy one lot at, with the index at 100,000

# The wallet's side. It does what AccountTransactionSupervisor does on a deposit and on the
# cancellation of a stop ticket, with the functions the supervisor calls. Node has no storage,
# so no account is recorded as the wallet's own: each run is a device that has not seen it.
WALLET_SCRIPT = r"""/* eslint-disable no-console */
import { HaneulGrpcClient } from '@haneullabs/haneul/grpc';
import { Ed25519Keypair } from '@haneullabs/haneul/keypairs/ed25519';
import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';

const perp: any = await import(join(process.env.WEB_SRC!, 'src/haneul/perp/index.ts'));
const ROOT = join(import.meta.dirname, '..', '..');
const deployment = JSON.parse(
  readFileSync(join(ROOT, 'public/configs/haneul/perp.localnet.json'), 'utf8')
);
const client = new HaneulGrpcClient({ network: 'localnet', baseUrl: 'http://127.0.0.1:9000' });

const [mode, keyFile, amount] = process.argv.slice(2);
if (!existsSync(keyFile!)) {
  writeFileSync(keyFile!, new Ed25519Keypair().getSecretKey() + '\n', { mode: 0o600 });
}
const signer = Ed25519Keypair.fromSecretKey(readFileSync(keyFile!, 'utf8').trim());
const me = signer.getPublicKey().toHaneulAddress();
// A front end from before the record of known accounts has neither the function nor `exposure`.
const known = (): string[] => perp.loadKnownAccounts?.(deployment.network, me) ?? [];
const account = () => perp.findPerpAccount(client, deployment, me, known());
const out = (o: unknown) =>
  console.log(JSON.stringify(o, (_, v) => (typeof v === 'bigint' ? v.toString() : v)));

if (mode === 'address') {
  out({ address: me });
} else if (mode === 'status') {
  const coins = await perp.listCollateralCoins(client, deployment, me);
  out({
    caps: (await perp.listAccountCaps(client, deployment, me)).length,
    chosen: (await account()) ?? null,
    walletUnits: coins.reduce((sum: bigint, c: { balance: bigint }) => sum + c.balance, 0n),
  });
} else if (mode === 'deposit') {
  const builder = new perp.PerpTransactionBuilder(deployment);
  const units = perp.collateralToUnits(Number(amount), deployment.collateral.decimals);
  const coins = await perp.listCollateralCoins(client, deployment, me);
  const found = await account();
  if (found?.exposure != null) {
    out({ refused: found.exposure });
  } else {
    const ids = coins.map((c: { objectId: string }) => c.objectId);
    const tx = found
      ? builder.deposit({ ref: { account: found.account, cap: found.cap }, coins: ids, amount: units })
      : builder.createAccount({ sender: me, coins: ids, amount: units });
    const result = await perp.signAndExecuteWith(signer, tx, client);
    out({ digest: result.digest, createdAccount: !found });
  }
} else if (mode === 'cancel-tickets') {
  const builder = new perp.PerpTransactionBuilder(deployment);
  const found = await account();
  const ref = { account: found.account, cap: found.cap };
  let canceled = 0;
  for (const ticketId of process.argv.slice(4)) {
    const tx = builder.cancelStopTicket({ ref, ticketId, recipient: me });
    await perp.signAndExecuteWith(signer, tx, client);
    canceled += 1;
  }
  out({ canceled });
} else {
  throw new Error(`unknown mode ${mode}`);
}
"""


def tusd(units):
    return f"{int(units) / TUSD_UNIT:,.2f}"


def vec_u8(data):
    return "[" + ", ".join(f"{x}u8" for x in data) + "]"


class Check(md.Run):
    """mark_drag's transactions and reads, with the two senders and the wallets of this check."""

    def __init__(self, s):
        super().__init__(s)
        self.out.close()
        unused = s.logs / "mark-drag-results.txt"
        if unused.exists() and unused.stat().st_size == 0:
            unused.unlink()
        self.out = open(s.logs / "first-cap-results.txt", "a")
        state = json.loads(s.state_file.read_text())
        config = json.loads((s.fixture_dir / "public/configs/haneul/perp.localnet.json").read_text())
        self.orders = state["P"]["perpetuals_orders"]
        self.assistant_role = self.admin.rsplit("::", 1)[0] + "::ASSISTANT"
        self.ticket_gas = int(config["ticketGas"])
        self.account_mod = f"{self.perp}::account"
        self.ch_mod = f"{self.perp}::clearing_house"
        self.stamp = int(time.time())
        self.failed = []
        self.wallet_script = s.fixture_dir / "scripts/haneul-localnet/first-cap-wallet.mts"
        self.wallet_script.write_text(WALLET_SCRIPT)

    def expect(self, what, ok):
        self.say(f"  {'ok  ' if ok else 'FAIL'} {what}")
        if not ok:
            self.failed.append(what)

    # ------------------------------------------------------------ the wallet, through the front end

    def app(self, mode, key, *args):
        web = self.s.web
        cmd = [str(web / "node_modules/.bin/tsx"), "--tsconfig", str(web / "tsconfig.json"),
               str(self.wallet_script), mode, str(key), *map(str, args)]
        p = subprocess.run(cmd, capture_output=True, text=True, env=dict(self.s.env, WEB_SRC=str(web)), cwd=self.s.fixture_dir)
        if p.returncode != 0:
            sys.exit(f"the front end's {mode} failed:\n{(p.stdout + p.stderr)[-2000:]}")
        return json.loads(p.stdout.strip().splitlines()[-1])

    def send_and_deposit(self, name, sent, sender):
        """A new wallet is sent the account's admin capability, is funded, and deposits."""
        key = self.s.work / f"wallet-{name}-{self.stamp}.key"
        address = self.app("address", key)["address"]
        self.ptb("send the admin capability", ["--transfer-objects", f"[{obj(sent['cap'])}]", obj(address)], sender=sender)
        self.fund(address, DEPOSIT)
        chosen = self.app("status", key)["chosen"]
        self.say(f"  wallet {address[:10]}: the front end finds account #{chosen['accountId']}, exposure {chosen.get('exposure')}")
        deposit = self.app("deposit", key, DEPOSIT)
        answer = f"REFUSED for the {deposit['refused']}" if deposit.get("refused") else "went through"
        self.say(f"  its deposit of {DEPOSIT:,} TUSD {answer}: wallet {tusd(self.wallet(address))}, account #{sent['id']} {tusd(self.balance(sent))}")
        return key, address, deposit

    def fund(self, address, amount):
        """Gas and TUSD for an address, from the fixture.

        The fixture mints with the admin's gas coin, and so does its price relay every three
        seconds. When the two meet, the coin stays locked until the epoch ends, a minute at
        most, and the relay stands still with it: the mint is tried again until it goes through,
        and the relay is given the time to sign a price again.
        """
        cmd = [self.s.node, "scripts/haneul-localnet/fixture.mjs", "fund", address, str(amount)]
        text = ""
        for attempt in range(10):
            p = subprocess.run(cmd, capture_output=True, text=True, env=self.s.env, cwd=self.s.fixture_dir)
            if p.returncode == 0:
                if attempt:
                    time.sleep(6)
                return
            text = p.stdout + p.stderr
            if "locked by a different transaction" not in text and "unavailable for consumption" not in text:
                break
            if attempt == 0:
                self.say("  the admin's gas coin is locked until the epoch ends; waiting for it")
            time.sleep(10)
        sys.exit(f"fixture fund {address} {amount} failed:\n{text[-1500:]}")

    # ------------------------------------------------------------ reads

    def tusd_coins(self, address):
        """This CLI lists raw objects: the ID is the first 32 bytes, a coin's balance the next 8."""
        out = self.s.cli("objects", address, "--json")
        coins = []
        for o in json.loads(out[out.find("["):]) if "[" in out else []:
            move = o["data"].get("Move", {})
            kind = move.get("type_")
            if isinstance(kind, dict) and kind.get("Coin", {}).get("struct", {}).get("name") == "TUSD":
                raw = bytes(move["contents"])
                coins.append(("0x" + raw[:32].hex(), int.from_bytes(raw[32:40], "little")))
        return coins

    def wallet(self, address):
        return sum(units for _, units in self.tusd_coins(address))

    def balance(self, account):
        return int(self.get_object(account["obj"])["json"]["collateral"])

    # ------------------------------------------------------------ accounts and sessions

    def new_address(self, alias):
        out = self.s.cli("new-address", "ed25519", f"{alias}-{self.stamp}", "--json")
        return json.loads(out[out.find("{"):])["address"]

    def create_account(self, owner, deposit_coin=None):
        cmds = call(f"{self.account_mod}::create_account", [self.tusd], obj(self.registry)) + ["--assign", "acc"]
        if deposit_coin:
            cmds += call(f"{self.account_mod}::deposit_collateral", [self.tusd, self.admin], "acc.0", "acc.2", obj(self.registry), obj(deposit_coin))
        cmds += call(f"{self.account_mod}::consume_policy_and_share_account", [self.tusd], "acc.0", "acc.1")
        cmds += ["--transfer-objects", "[acc.2]", obj(owner)]
        j, _ = self.ptb("create an account", cmds, sender=owner)
        created = self.events(j, "::events::CreatedAccount")[0]
        cap = next(c["objectId"] for c in j["objectChanges"]
                   if c["type"] == "created" and "AuthorityCap<" in c["objectType"] and "::authority::ACCOUNT" in c["objectType"])
        return {"obj": created["account_obj_id"], "cap": cap, "id": int(created["account_id"])}

    def open_position(self, account, sender, ten_times=True, allocate=0):
        args = (obj(self.ch), obj(account["cap"]), obj(account["obj"]))
        cmds = call(f"{self.ch_mod}::create_market_position", [self.tusd, self.admin], *args)
        if ten_times:
            cmds += call(f"{self.ch_mod}::set_position_initial_margin_ratio", [self.tusd, self.admin], *args, f"{ONE // 10}u256")
        if allocate:
            cmds += call(f"{self.ch_mod}::allocate_collateral", [self.tusd, self.admin], *args, f"{allocate * TUSD_UNIT}u64")
        self.ptb("open a position", cmds, sender=sender)

    def session_as(self, label, account, cap, role, actions, sender):
        """mark_drag's session with the capability and its role chosen: an assistant's, here."""
        cmds = call("0x1::option::none", [f"{self.account_mod}::IntegratorInfo"]) + ["--assign", "no_integrator"]
        cmds += call(f"{self.ch_mod}::start_session", [self.tusd, role], obj(self.ch), obj(cap), obj(account["obj"]),
                     obj(self.pfs_btc), obj(self.pfs_tusd), "no_integrator", "@0x6") + ["--assign", "hp"]
        for action in actions:
            cmds += action
        cmds += call(f"{self.ch_mod}::end_session", [self.tusd, role], "hp", obj(cap), obj(account["obj"]), "false", "false") + ["--assign", "res"]
        cmds += call(f"{self.ch_mod}::share", [self.tusd], "res.0")
        j, _ = self.ptb(label, cmds, sender=sender)
        return j

    def rest(self, own, sender, is_ask, price_usd):
        """One lot of the sender's own account, post-only. Returns the order's ID."""
        j = self.session_as("the sender's order", own, own["cap"], self.admin, [self.order(is_ask, 1, price_usd, 2)], sender)
        return self.events(j, "::events::PostedOrder")[0]["order_id"]

    def cancel_order(self, own, sender, order_id):
        self.ptb("cancel", ["--make-move-vec", "<u128>", f"[{order_id}u128]", "--assign", "ids"] + call(
            f"{self.ch_mod}::cancel_orders", [self.tusd, self.admin], obj(self.ch), obj(own["cap"]), obj(own["obj"]), "ids"), sender=sender)

    def sender(self, alias):
        """An address with its stake in an account of its own, at ten times."""
        address = self.new_address(alias)
        self.fund(address, STAKE)
        own = self.create_account(address, deposit_coin=self.tusd_coins(address)[0][0])
        self.open_position(own, address, allocate=STAKE)
        return address, own

    def taken(self, own, sender):
        """The sender's flat position gives its collateral back and the account pays it out."""
        args = (obj(self.ch), obj(own["cap"]), obj(own["obj"]))
        cmds = call(f"{self.ch_mod}::set_position_initial_margin_ratio", [self.tusd, self.admin], *args, f"{ONE}u256")
        cmds += call(f"{self.ch_mod}::deallocate_free_collateral", [self.tusd, self.admin], *args, obj(self.pfs_btc), obj(self.pfs_tusd), "@0x6") + ["--assign", "freed"]
        cmds += call(f"{self.account_mod}::withdraw_collateral", [self.tusd], obj(own["obj"]), obj(own["cap"]), obj(self.registry), "freed") + ["--assign", "coin"]
        cmds += ["--transfer-objects", "[coin]", obj(sender)]
        self.ptb("pay out", cmds, sender=sender)
        gained = self.wallet(sender) - STAKE * TUSD_UNIT
        self.expect(f"the sender takes nothing of the deposit (it took {tusd(gained)} of the {DEPOSIT:,} TUSD)", gained <= 0)

    # ------------------------------------------------------------ 1. an assistant capability

    def with_assistant(self):
        self.say("--- 1. an account whose sender keeps an assistant capability")
        sender, own = self.sender("sender-a")
        sent = self.create_account(sender)
        j, _ = self.ptb("mint an assistant capability", call(f"{self.account_mod}::new_assistant_account_cap", [self.tusd], obj(sent["obj"]), obj(sent["cap"]), obj(self.registry))
                        + ["--assign", "assistant", "--transfer-objects", "[assistant]", obj(sender)], sender=sender)
        assistant = next(c["objectId"] for c in j["objectChanges"]
                         if c["type"] == "created" and "::authority::AuthorityCap<" in c["objectType"] and "::authority::ASSISTANT" in c["objectType"])
        self.open_position(sent, sender)
        _, address, deposit = self.send_and_deposit("a", sent, sender)
        allocation = call(f"{self.ch_mod}::allocate_collateral", [self.tusd, self.assistant_role], obj(self.ch), obj(assistant), obj(sent["obj"]),
                          f"{(DEPOSIT - 100) * TUSD_UNIT}u64")
        if deposit.get("refused"):
            self.expect("the deposit is refused for the assistant capability", deposit["refused"] == "assistants")
            self.expect(f"the wallet keeps its {DEPOSIT:,} TUSD", self.wallet(address) == DEPOSIT * TUSD_UNIT)
            self.expect("the account stays empty", self.balance(sent) == 0)
            j, _ = self.ptb("the assistant allocates", allocation, sender=sender, may_abort=True)
            self.expect("the sender's allocation fails: there is nothing on the account", j is None)
            return
        # The front end deposited. The assistant capability allocates the deposit, buys one lot
        # of the sender's own account far above the index and sells it back at the index.
        self.rest(own, sender, True, FAR)
        self.ptb("the assistant allocates", allocation, sender=sender)
        self.session_as("the assistant buys", sent, assistant, self.assistant_role, [self.order(False, 1, FAR, 3)], sender)
        self.rest(own, sender, False, self.index_usd)
        self.session_as("the assistant sells", sent, assistant, self.assistant_role, [self.order(True, 1, self.index_usd, 3)], sender)
        self.taken(own, sender)

    # ------------------------------------------------------------ 2. tickets

    def commitment(self, is_ask, price_usd, salt):
        """blake2b256(bcs(order details) || salt), as `place_stop_order_standalone` rebuilds it."""
        b = bytes.fromhex(self.ch[2:])                    # the market
        b += b"\x00"                                      # no expiry
        b += b"\x01"                                      # a limit order
        b += b"\x00"                                      # triggers on the index price
        b += ONE.to_bytes(32, "little")                   # ... at or above 1 USD, which always holds
        b += b"\x01"
        b += b"\x01" if is_ask else b"\x00"
        b += LOT.to_bytes(8, "little")
        b += (price_usd * B9).to_bytes(8, "little")
        b += (3).to_bytes(8, "little")                    # immediate or cancel
        b += b"\x00"                                      # not reduce-only: the order may allocate margin
        b += b"\x00"                                      # no integrator
        return hashlib.blake2b(b + salt, digest_size=32).digest()

    def leave_ticket(self, sent, sender, is_ask, price_usd, salt):
        cmds = ["--split-coins", "gas", f"[{self.ticket_gas}]", "--assign", "g"]
        cmds += ["--make-move-vec", "<address>", f"[{obj(sender)}]", "--assign", "executors"]
        cmds += call("0x1::option::none", ["address"]) + ["--assign", "no_domain"]
        cmds += ["--make-move-vec", "<u8>", vec_u8(self.commitment(is_ask, price_usd, salt)), "--assign", "commitment"]
        cmds += call(f"{self.orders}::stop_orders::create_stop_order_ticket", [self.tusd, self.admin], obj(sent["obj"]), obj(sent["cap"]), obj(self.registry),
                     "executors", "no_domain", "g.0", "1u64", "commitment")
        j, _ = self.ptb("leave a ticket", cmds, sender=sender)
        return self.events(j, "::events::CreatedStopOrderTicket")[0]["ticket_id"]

    def run_ticket(self, sent, sender, ticket, is_ask, price_usd, salt, may_abort=False):
        """The executor's transaction: no capability of the account is among its inputs."""
        cmds = call(f"{self.ch_mod}::no_domain_executor", []) + ["--assign", "executor"]
        cmds += call("0x1::option::none", ["u64"]) + ["--assign", "no_expiry"]
        cmds += call("0x1::option::none", [f"{self.account_mod}::IntegratorInfo"]) + ["--assign", "no_integrator"]
        cmds += call("0x2::object::id_from_address", [], obj(ticket)) + ["--assign", "ticket"]
        cmds += ["--make-move-vec", "<u8>", vec_u8(salt), "--assign", "salt"]
        cmds += call(f"{self.orders}::stop_orders::place_stop_order_standalone", [self.tusd], obj(self.ch), obj(self.pfs_btc), obj(self.pfs_tusd), "@0x6", obj(self.registry),
                     "ticket", obj(sent["obj"]), "no_expiry", "true", "0u8", f"{ONE}u256", "true", "true" if is_ask else "false",
                     f"{LOT}u64", f"{price_usd * B9}u64", "3u64", "false", "salt", "no_integrator", "executor") + ["--assign", "res"]
        cmds += call(f"{self.ch_mod}::share", [self.tusd], "res.2")
        cmds += ["--transfer-objects", "[res.1]", obj(sender)]
        j, _ = self.ptb("execute the ticket", cmds, sender=sender, may_abort=may_abort)
        return j

    def with_tickets(self):
        self.say("--- 2. an account that holds two tickets naming its sender as executor, and no assistant capability")
        sender, own = self.sender("sender-t")
        sent = self.create_account(sender)
        self.open_position(sent, sender, ten_times=False)
        salt_buy, salt_sell = bytes([7] * 32), bytes([9] * 32)
        buy = self.leave_ticket(sent, sender, False, FAR, salt_buy)
        sell = self.leave_ticket(sent, sender, True, self.index_usd, salt_sell)
        key, address, deposit = self.send_and_deposit("t", sent, sender)
        if deposit.get("refused"):
            self.expect("the deposit is refused for the tickets", deposit["refused"] == "tickets")
            self.expect(f"the wallet keeps its {DEPOSIT:,} TUSD", self.wallet(address) == DEPOSIT * TUSD_UNIT)
            canceled = self.app("cancel-tickets", key, buy, sell)["canceled"]
            exposure = self.app("status", key)["chosen"].get("exposure")
            self.say(f"  the wallet cancels the {canceled} tickets through the front end; exposure is now {exposure}")
            self.expect("without the tickets the account has no exposure", exposure is None)
            deposit = self.app("deposit", key, DEPOSIT)
            self.expect("the deposit now goes through", not deposit.get("refused") and self.balance(sent) == DEPOSIT * TUSD_UNIT)
            ask = self.rest(own, sender, True, FAR)
            j = self.run_ticket(sent, sender, buy, False, FAR, salt_buy, may_abort=True)
            self.expect("the sender's execution of the canceled ticket fails", j is None)
            self.expect(f"the {DEPOSIT:,} TUSD are still on the account", self.balance(sent) == DEPOSIT * TUSD_UNIT)
            self.cancel_order(own, sender, ask)
            return
        # The front end deposited. The sender executes its two tickets against its own account.
        self.rest(own, sender, True, FAR)
        self.run_ticket(sent, sender, buy, False, FAR, salt_buy)
        self.rest(own, sender, False, self.index_usd)
        self.run_ticket(sent, sender, sell, True, self.index_usd, salt_sell)
        self.taken(own, sender)

    # ------------------------------------------------------------ 3. nothing sent

    def plain_wallet(self):
        self.say("--- 3. a wallet nobody sent anything to")
        key = self.s.work / f"wallet-p-{self.stamp}.key"
        address = self.app("address", key)["address"]
        self.fund(address, DEPOSIT)
        deposit = self.app("deposit", key, DEPOSIT)
        chosen = self.app("status", key)["chosen"]
        self.say(f"  wallet {address[:10]}: account #{chosen['accountId']}, balance {tusd(chosen['collateral'])}, exposure {chosen.get('exposure')}")
        self.expect("the deposit opens the wallet an account of its own", deposit.get("createdAccount") is True)
        self.expect(f"the account holds the {DEPOSIT:,} TUSD and has no exposure",
                    int(chosen["collateral"]) == DEPOSIT * TUSD_UNIT and chosen.get("exposure") is None)

    def run(self):
        self.s.assert_local()
        self.say(f"=== an account sent to a wallet, and the deposit of the front end at {self.s.web} ===")
        self.with_assistant()
        self.with_tickets()
        self.plain_wallet()
        self.say("=== " + ("all as expected" if not self.failed else f"{len(self.failed)} FAILED: " + "; ".join(self.failed)) + " ===")
        return not self.failed


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("step", choices=["up", "run", "down"])
    parser.add_argument("--work", required=True, help="a scratch directory; everything this script writes is inside it")
    parser.add_argument("--perp-dex", default="~/perp-dex", help="the engine checkout, read only")
    parser.add_argument("--web", default="~/perp-dex-web", help="the front end checkout, read only: its fixture and node modules for `up`, its code for `run`")
    parser.add_argument("--haneul", help="the CLI binary; <perp-dex>/deps/bin/haneul by default")
    parser.add_argument("--node", default="node", help="a Node 22 binary")
    args = parser.parse_args()
    stack = md.Stack(args)
    if args.step == "up":
        md.up(stack)
    elif args.step == "down":
        md.down(stack)
    else:
        if not stack.pids_file.exists():
            sys.exit(f"no {stack.pids_file}: bring the stack up first")
        sys.exit(0 if Check(stack).run() else 1)


if __name__ == "__main__":
    main()
