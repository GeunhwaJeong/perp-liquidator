// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! The keeper's transactions. Executing a stop ticket is one transaction: relay the market's
//! signed prices, reveal the ticket's details to `stop_orders::place_stop_order_sltp` or
//! `place_stop_order_standalone`, which hash them against the ticket's commitment, check the
//! trigger and run the order in a session of its own, then merge the gas the ticket held into
//! the keeper's gas coin and share the clearing house again. The engine takes the clearing house
//! by value for the session and hands it back; it has no `store`, so only
//! `clearing_house::share` can put it back.
//!
//! Object inputs are given by ID only; the full node fills in versions and mutability when it
//! resolves the transaction.

use haneul_sdk_types::{Address, Identifier, TypeTag};
use haneul_transaction_builder::{Argument, Function, ObjectInput, TransactionBuilder};
use perp_bot_common::oracle::{Relay, SignedUpdate};
use perp_bot_common::tickets::{IntegratorInfo, StandaloneStop, StopLossTakeProfit};

const CLOCK: Address = Address::from_static("0x6");
const MOVE_STDLIB: Address = Address::from_static("0x1");

/// The packages and shared objects every execution names.
#[derive(Clone, Debug)]
pub struct Engine {
    /// The engine package calls go to.
    pub package: Address,
    /// The package the engine's types were first published at.
    pub types_package: Address,
    /// The `perpetuals_orders` package, which holds the tickets.
    pub orders_package: Address,
    pub registry: Address,
    pub collateral: TypeTag,
}

/// The feeds a ticket's market reads. The market itself is the clearing house its details name,
/// so the details cannot be sent to another one.
#[derive(Clone, Copy, Debug)]
pub struct Feeds {
    pub base: Address,
    pub collateral: Address,
}

/// A ticket on chain: its ID, and the shared account that holds it.
#[derive(Clone, Copy, Debug)]
pub struct Ticket {
    pub id: Address,
    pub account: Address,
}

/// The details revealed to the engine.
#[derive(Clone, Copy, Debug)]
pub enum Details<'a> {
    StopLossTakeProfit(&'a StopLossTakeProfit),
    StandaloneStop(&'a StandaloneStop),
}

impl Details<'_> {
    pub fn clearing_house(&self) -> Address {
        match self {
            Self::StopLossTakeProfit(d) => d.clearing_house,
            Self::StandaloneStop(d) => d.clearing_house,
        }
    }

    fn integrator_info(&self) -> Option<IntegratorInfo> {
        match self {
            Self::StopLossTakeProfit(d) => d.integrator_info,
            Self::StandaloneStop(d) => d.integrator_info,
        }
    }
}

/// Prices to relay in front of the execution.
pub type Refresh<'a> = Option<(&'a Relay, &'a [SignedUpdate])>;

/// Turns an object ID into a transaction input. Production leaves resolution to the full node
/// (`ObjectInput::new`); tests resolve offline.
pub type Resolver = dyn Fn(Address) -> ObjectInput + Sync;

pub struct Builder<'a> {
    engine: &'a Engine,
    object: &'a Resolver,
    tx: TransactionBuilder,
    /// Every command so far, in order: what an abort's command index names.
    commands: Vec<String>,
}

impl<'a> Builder<'a> {
    pub fn new(engine: &'a Engine, object: &'a Resolver) -> Self {
        Self {
            engine,
            object,
            tx: TransactionBuilder::new(),
            commands: Vec::new(),
        }
    }

    pub fn finish(self) -> TransactionBuilder {
        self.tx
    }

    /// The commands so far, indexed, for failure messages.
    pub fn commands(&self) -> String {
        self.commands
            .iter()
            .enumerate()
            .map(|(i, c)| format!("{i}:{c}"))
            .collect::<Vec<_>>()
            .join(" ")
    }

    fn obj(&mut self, id: Address) -> Argument {
        let input = (self.object)(id);
        self.tx.object(input)
    }

    fn call(
        &mut self,
        package: Address,
        module: &str,
        function: &str,
        type_args: Vec<TypeTag>,
        args: Vec<Argument>,
    ) -> Argument {
        self.commands.push(format!("{module}::{function}"));
        let function =
            Function::new(package, ident(module), ident(function)).with_type_args(type_args);
        self.tx.move_call(function, args)
    }

    /// Relays signed prices. The market's feeds are among them, so their inputs are the same
    /// objects the execution reads.
    pub fn refresh(&mut self, refresh: Refresh<'_>) -> &mut Self {
        let Some((relay, updates)) = refresh else {
            return self;
        };
        for update in updates {
            let args = vec![
                self.obj(relay.source),
                self.obj(relay.config),
                self.obj(update.price_feed_storage),
                self.tx.pure(&update.price),
                self.tx.pure(&update.confidence),
                self.tx.pure(&update.timestamp_ms),
                self.tx.pure(&update.public_key),
                self.tx.pure(&update.signature),
                self.obj(CLOCK),
            ];
            self.call(
                relay.package,
                "price_feed_storage",
                "update_price_feed",
                vec![],
                args,
            );
        }
        self
    }

    /// Executes a ticket with its details. The arguments follow the Move signatures; the engine
    /// hashes the clearing house's ID, the detail fields, the integrator and the salt, so any
    /// difference from what the ticket committed to aborts with `EWrongOrderDetails`.
    pub fn execute(&mut self, ticket: &Ticket, details: Details<'_>, feeds: &Feeds) -> &mut Self {
        let executor = self.call(
            self.engine.package,
            "clearing_house",
            "no_domain_executor",
            vec![],
            vec![],
        );
        let integrator = self.integrator(details.integrator_info());
        let mut args = vec![
            self.obj(details.clearing_house()),
            self.obj(feeds.base),
            self.obj(feeds.collateral),
            self.obj(CLOCK),
            self.obj(self.engine.registry),
            self.tx.pure(&ticket.id),
            self.obj(ticket.account),
        ];
        let function = match details {
            Details::StopLossTakeProfit(d) => {
                args.extend([
                    self.tx.pure(&d.expire_timestamp_ms),
                    self.tx.pure(&d.is_limit_order),
                    self.tx.pure(&d.trigger_price_type),
                    self.tx.pure(&d.stop_loss_price),
                    self.tx.pure(&d.take_profit_price),
                    self.tx.pure(&d.position_is_ask),
                    self.tx.pure(&d.size),
                    self.tx.pure(&d.price),
                    self.tx.pure(&d.order_type),
                    self.tx.pure(&d.salt),
                ]);
                "place_stop_order_sltp"
            }
            Details::StandaloneStop(d) => {
                args.extend([
                    self.tx.pure(&d.expire_timestamp_ms),
                    self.tx.pure(&d.is_limit_order),
                    self.tx.pure(&d.trigger_price_type),
                    self.tx.pure(&d.stop_index_price),
                    self.tx.pure(&d.ge_stop_index_price),
                    self.tx.pure(&d.side),
                    self.tx.pure(&d.size),
                    self.tx.pure(&d.price),
                    self.tx.pure(&d.order_type),
                    self.tx.pure(&d.reduce_only),
                    self.tx.pure(&d.salt),
                ]);
                "place_stop_order_standalone"
            }
        };
        args.extend([integrator, executor]);
        // (SessionSummary, Coin<HANEUL>, ClearingHouse<T>)
        let result = self
            .call(
                self.engine.orders_package,
                "stop_orders",
                function,
                vec![self.engine.collateral.clone()],
                args,
            )
            .to_nested(3);
        // The gas the ticket held pays the keeper back. Merged into the gas coin, it leaves no
        // small coins behind.
        let gas = self.tx.gas();
        self.tx.merge_coins(gas, vec![result[1]]);
        self.commands.push("MergeCoins".to_owned());
        self.call(
            self.engine.package,
            "clearing_house",
            "share",
            vec![self.engine.collateral.clone()],
            vec![result[2]],
        );
        self
    }

    /// `Option<IntegratorInfo>` cannot be a pure input, so it is made on chain.
    fn integrator(&mut self, info: Option<IntegratorInfo>) -> Argument {
        match info {
            None => {
                let integrator_info: TypeTag =
                    format!("{}::account::IntegratorInfo", self.engine.types_package)
                        .parse()
                        .expect("a well-formed type");
                self.call(MOVE_STDLIB, "option", "none", vec![integrator_info], vec![])
            }
            Some(info) => {
                let args = vec![
                    self.tx.pure(&info.integrator_id),
                    self.tx.pure(&info.integrator_fee),
                ];
                self.call(
                    self.engine.package,
                    "account",
                    "create_integrator_info",
                    vec![],
                    args,
                )
            }
        }
    }
}

fn ident(name: &str) -> Identifier {
    Identifier::new(name).expect("a valid Move identifier")
}

#[cfg(test)]
mod tests {
    use haneul_sdk_types::hash::Hasher;
    use haneul_sdk_types::{
        Argument as Arg, Command, Input, MoveCall, ProgrammableTransaction, TransactionKind,
    };

    use super::*;

    // The mainnet BTC-USD clearing house, as in the commitments `perp_bot_common::tickets` and
    // perp-web's `tickets.spec.ts` pin.
    const CLEARING_HOUSE: &str =
        "0xf77dda417d4003c017c96697809d3a25b2767aca586b5b7579e975e47508df77";

    fn engine() -> Engine {
        Engine {
            package: Address::from_static("0xe2"),
            types_package: Address::from_static("0xe1"),
            orders_package: Address::from_static("0x0d"),
            registry: Address::from_static("0x5"),
            collateral: "0x7f::tusd::TUSD".parse().unwrap(),
        }
    }

    fn feeds() -> Feeds {
        Feeds {
            base: Address::from_static("0xfa"),
            collateral: Address::from_static("0x1d"),
        }
    }

    fn ticket() -> Ticket {
        Ticket {
            id: Address::from_static("0x71c"),
            account: Address::from_static("0xacc"),
        }
    }

    fn offline(id: Address) -> ObjectInput {
        ObjectInput::shared(id, 1, true)
    }

    fn usd(dollars: u128) -> [u8; 32] {
        let mut bytes = [0; 32];
        bytes[..16].copy_from_slice(&(dollars * 10u128.pow(18)).to_le_bytes());
        bytes
    }

    fn salt() -> Vec<u8> {
        (0..32).collect()
    }

    fn stop_loss_take_profit_on_a_long() -> StopLossTakeProfit {
        StopLossTakeProfit {
            clearing_house: Address::from_static(CLEARING_HOUSE),
            expire_timestamp_ms: None,
            is_limit_order: false,
            trigger_price_type: 0,
            stop_loss_price: Some(usd(95_000)),
            take_profit_price: Some(usd(105_000)),
            position_is_ask: false,
            size: 250_000_000,
            price: 0,
            order_type: 0,
            integrator_info: None,
            salt: salt(),
        }
    }

    fn standalone_stop_on_the_book_price() -> StandaloneStop {
        StandaloneStop {
            clearing_house: Address::from_static(CLEARING_HOUSE),
            expire_timestamp_ms: Some(1_800_000_000_000),
            is_limit_order: true,
            trigger_price_type: 1,
            stop_index_price: usd(101_000),
            ge_stop_index_price: true,
            side: false,
            size: 100_000_000,
            price: 101_500_000_000,
            order_type: 0,
            reduce_only: true,
            integrator_info: None,
            salt: salt(),
        }
    }

    fn relay() -> (Relay, Vec<SignedUpdate>) {
        let relay = Relay {
            package: Address::from_static("0x0a"),
            source: Address::from_static("0x5c"),
            config: Address::from_static("0xcf"),
        };
        let update = |feed: Address, price: u128| SignedUpdate {
            price_feed_storage: feed,
            storage_id: 0,
            price,
            confidence: 0,
            timestamp_ms: 1_790_000_000_000,
            public_key: vec![1; 32],
            signature: vec![2; 64],
        };
        let updates = vec![
            update(feeds().base, 100_000 * 10u128.pow(18)),
            update(feeds().collateral, 10u128.pow(18)),
        ];
        (relay, updates)
    }

    fn build(b: Builder<'_>) -> ProgrammableTransaction {
        let mut tx = b.finish();
        tx.set_sender(Address::from_static("0x5e"));
        tx.set_gas_budget(1);
        tx.set_gas_price(1);
        tx.add_gas_objects([ObjectInput::owned(
            Address::from_static("0x9a5"),
            1,
            haneul_sdk_types::Digest::ZERO,
        )]);
        let tx = tx.try_build().unwrap();
        let TransactionKind::ProgrammableTransaction(ptb) = tx.kind else {
            panic!("a programmable transaction")
        };
        ptb
    }

    /// A command as `module::function<type args>`, or its kind.
    fn name(command: &Command) -> String {
        match command {
            Command::MoveCall(call) => {
                // Addresses print in full; the fixtures use their short forms.
                let types: Vec<String> = call
                    .type_arguments
                    .iter()
                    .map(|t| {
                        t.to_string()
                            .replace(&format!("0x{}", "0".repeat(62)), "0x")
                    })
                    .collect();
                format!("{}::{}<{}>", call.module, call.function, types.join(","))
            }
            Command::MergeCoins(_) => "MergeCoins".to_owned(),
            other => format!("{other:?}"),
        }
    }

    fn names(ptb: &ProgrammableTransaction) -> Vec<String> {
        ptb.commands.iter().map(name).collect()
    }

    fn stop_call(ptb: &ProgrammableTransaction) -> (usize, &MoveCall) {
        ptb.commands
            .iter()
            .enumerate()
            .find_map(|(i, c)| match c {
                Command::MoveCall(call) if call.module.to_string() == "stop_orders" => {
                    Some((i, call))
                }
                _ => None,
            })
            .expect("a stop_orders call")
    }

    fn pure_of(ptb: &ProgrammableTransaction, arg: &Arg) -> Vec<u8> {
        match arg {
            Arg::Input(i) => match &ptb.inputs[*i as usize] {
                Input::Pure(bytes) => bytes.clone(),
                other => panic!("input {i} is not pure: {other:?}"),
            },
            other => panic!("{other:?} is not an input"),
        }
    }

    fn object_of(ptb: &ProgrammableTransaction, arg: &Arg) -> Address {
        match arg {
            Arg::Input(i) => match &ptb.inputs[*i as usize] {
                Input::Shared(shared) => shared.object_id(),
                other => panic!("input {i} is not a shared object: {other:?}"),
            },
            other => panic!("{other:?} is not an input"),
        }
    }

    #[test]
    fn a_stop_loss_take_profit_relays_reveals_keeps_the_gas_and_shares_the_market() {
        let engine = engine();
        let details = stop_loss_take_profit_on_a_long();
        let (relay, updates) = relay();
        let mut b = Builder::new(&engine, &offline);
        b.refresh(Some((&relay, &updates))).execute(
            &ticket(),
            Details::StopLossTakeProfit(&details),
            &feeds(),
        );
        assert_eq!(
            b.commands(),
            "0:price_feed_storage::update_price_feed 1:price_feed_storage::update_price_feed \
             2:clearing_house::no_domain_executor 3:option::none \
             4:stop_orders::place_stop_order_sltp 5:MergeCoins 6:clearing_house::share"
        );
        let ptb = build(b);
        assert_eq!(
            names(&ptb),
            vec![
                "price_feed_storage::update_price_feed<>",
                "price_feed_storage::update_price_feed<>",
                "clearing_house::no_domain_executor<>",
                "option::none<0xe1::account::IntegratorInfo>",
                "stop_orders::place_stop_order_sltp<0x7f::tusd::TUSD>",
                "MergeCoins",
                "clearing_house::share<0x7f::tusd::TUSD>",
            ]
        );

        let (index, call) = stop_call(&ptb);
        assert_eq!(call.package, engine.orders_package);
        assert_eq!(call.arguments.len(), 19);
        // The objects, where the Move signature has them.
        let objects: Vec<Address> = [0, 1, 2, 3, 4, 6]
            .iter()
            .map(|&i| object_of(&ptb, &call.arguments[i]))
            .collect();
        assert_eq!(
            objects,
            vec![
                details.clearing_house,
                feeds().base,
                feeds().collateral,
                CLOCK,
                engine.registry,
                ticket().account,
            ]
        );
        assert_eq!(
            pure_of(&ptb, &call.arguments[5]),
            ticket().id.as_bytes().to_vec()
        );
        // The integrator and the executor come from the two calls in front.
        assert_eq!(call.arguments[17], Arg::Result(3));
        assert_eq!(call.arguments[18], Arg::Result(2));

        let Command::MergeCoins(merge) = &ptb.commands[index + 1] else {
            panic!("a merge after the execution")
        };
        assert_eq!(merge.coin, Arg::Gas);
        assert_eq!(
            merge.coins_to_merge,
            vec![Arg::NestedResult(index as u16, 1)]
        );
        let Command::MoveCall(share) = &ptb.commands[index + 2] else {
            panic!("a call after the merge")
        };
        assert_eq!(share.arguments, vec![Arg::NestedResult(index as u16, 2)]);
    }

    #[test]
    fn the_revealed_details_hash_to_the_commitments_the_front_end_pins() {
        let engine = engine();
        let sltp = stop_loss_take_profit_on_a_long();
        let standalone = standalone_stop_on_the_book_price();
        // The detail fields' arguments, then the salt's right after them.
        for (details, fields, expected) in [
            (
                Details::StopLossTakeProfit(&sltp),
                7..16,
                "330487179a9f27ca0f9adf7fca5bb677b5484ded8aaecabf9cc5ca5fe1e372b8",
            ),
            (
                Details::StandaloneStop(&standalone),
                7..17,
                "17c0fe7d24464e1d3df5dcf1cf54c9534e5044e6284a9ee22272a5e6c3f229ec",
            ),
        ] {
            let mut b = Builder::new(&engine, &offline);
            b.execute(&ticket(), details, &feeds());
            let ptb = build(b);
            let (_, call) = stop_call(&ptb);
            // What the engine hashes: the clearing house's ID, the fields in signature order,
            // no integrator, then the salt's bare bytes.
            let mut preimage = details.clearing_house().as_bytes().to_vec();
            for arg in &call.arguments[fields.clone()] {
                preimage.extend(pure_of(&ptb, arg));
            }
            preimage.push(0);
            let salt: Vec<u8> =
                bcs::from_bytes(&pure_of(&ptb, &call.arguments[fields.end])).unwrap();
            preimage.extend(salt);
            assert_eq!(hex::encode(Hasher::digest(preimage).into_inner()), expected);
        }
    }

    #[test]
    fn an_integrator_is_made_on_chain_and_no_prices_means_no_relay() {
        let engine = engine();
        let mut details = standalone_stop_on_the_book_price();
        details.integrator_info = Some(IntegratorInfo {
            integrator_id: 7,
            integrator_fee: 1_000_000,
        });
        let mut b = Builder::new(&engine, &offline);
        b.refresh(None)
            .execute(&ticket(), Details::StandaloneStop(&details), &feeds());
        let ptb = build(b);
        assert_eq!(
            names(&ptb),
            vec![
                "clearing_house::no_domain_executor<>",
                "account::create_integrator_info<>",
                "stop_orders::place_stop_order_standalone<0x7f::tusd::TUSD>",
                "MergeCoins",
                "clearing_house::share<0x7f::tusd::TUSD>",
            ]
        );
        let Command::MoveCall(create) = &ptb.commands[1] else {
            panic!("a call")
        };
        assert_eq!(create.package, engine.package);
        let args: Vec<Vec<u8>> = create.arguments.iter().map(|a| pure_of(&ptb, a)).collect();
        assert_eq!(
            args,
            vec![
                7u32.to_le_bytes().to_vec(),
                1_000_000u32.to_le_bytes().to_vec()
            ]
        );
        let (_, call) = stop_call(&ptb);
        assert_eq!(call.arguments.len(), 20);
        assert_eq!(call.arguments[18], Arg::Result(1));
        assert_eq!(call.arguments[19], Arg::Result(0));
    }
}
