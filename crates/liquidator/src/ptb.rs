// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! The liquidator's transactions, one decision per transaction.
//!
//! A liquidation is a session of the liquidator's account: refresh the prices, open the
//! session, liquidate (which must be its first action), sell what was taken over back into the
//! book with an immediate-or-cancel reduce-only order at most the slippage away from the mark
//! price, and close the session, allocating the margin the remainder needs and returning the
//! collateral it does not. Everything happens or nothing does: the liquidator never holds a
//! position it did not also try to unwind, and a failed unwind does not leave a liquidation
//! half done.
//!
//! Object inputs are given by ID only; the full node fills in versions and mutability when it
//! resolves the transaction.

use haneul_sdk_types::{Address, Identifier, TypeTag};
use haneul_transaction_builder::{Argument, Function, ObjectInput, TransactionBuilder};

use crate::adl::Plan;
use crate::oracle::{Relay, SignedUpdate};

const CLOCK: Address = Address::from_static("0x6");
/// `place_limit_order`'s order type for immediate-or-cancel.
const IMMEDIATE_OR_CANCEL: u64 = 3;

/// What every call needs to know about the engine and the liquidator's account.
#[derive(Clone, Debug)]
pub struct Engine {
    /// The package calls go to.
    pub package: Address,
    /// The package the engine's types were first published at.
    pub types_package: Address,
    pub collateral: TypeTag,
    /// The role of the liquidator's capability: `authority::ADMIN` or `authority::ASSISTANT`.
    pub role: TypeTag,
    pub account: Address,
    pub cap: Address,
}

#[derive(Clone, Copy, Debug)]
pub struct MarketObjects {
    pub clearing_house: Address,
    pub base_feed: Address,
    pub collateral_feed: Address,
}

/// An order that unwinds a position taken over.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Unwind {
    /// Sell when true: what was taken over is long.
    pub is_ask: bool,
    /// At least the size to unwind, in 9-decimal units. The engine trims a reduce-only order
    /// to the position it reduces.
    pub size: u64,
    pub price: u64,
}

/// Prices to relay in front of the calls.
pub type Refresh<'a> = Option<(&'a Relay, &'a [SignedUpdate])>;

pub struct Builder<'a> {
    engine: &'a Engine,
    /// Turns an object ID into a transaction input. Production leaves resolution to the full
    /// node; tests resolve offline.
    object: &'a dyn Fn(Address) -> ObjectInput,
    tx: TransactionBuilder,
    /// Commands added so far: the index the next one gets.
    commands: u64,
}

impl<'a> Builder<'a> {
    pub fn new(engine: &'a Engine, object: &'a dyn Fn(Address) -> ObjectInput) -> Self {
        Self {
            engine,
            object,
            tx: TransactionBuilder::new(),
            commands: 0,
        }
    }

    pub fn finish(self) -> TransactionBuilder {
        self.tx
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
        let function =
            Function::new(package, ident(module), ident(function)).with_type_args(type_args);
        self.commands += 1;
        self.tx.move_call(function, args)
    }

    fn perp(
        &mut self,
        module: &str,
        function: &str,
        type_args: Vec<TypeTag>,
        args: Vec<Argument>,
    ) -> Argument {
        self.call(self.engine.package, module, function, type_args, args)
    }

    fn collateral(&self) -> TypeTag {
        self.engine.collateral.clone()
    }

    fn session_types(&self) -> Vec<TypeTag> {
        vec![self.engine.collateral.clone(), self.engine.role.clone()]
    }

    /// Relays signed prices. The feeds of the market are among them, so their inputs are the
    /// same objects the session reads.
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

    fn start_session(&mut self, market: &MarketObjects) -> Argument {
        let integrator_info: TypeTag =
            format!("{}::account::IntegratorInfo", self.engine.types_package)
                .parse()
                .expect("a well-formed type");
        let none = self.call(
            Address::from_static("0x1"),
            "option",
            "none",
            vec![integrator_info],
            vec![],
        );
        let args = vec![
            self.obj(market.clearing_house),
            self.obj(self.engine.cap),
            self.obj(self.engine.account),
            self.obj(market.base_feed),
            self.obj(market.collateral_feed),
            none,
            self.obj(CLOCK),
        ];
        self.perp(
            "clearing_house",
            "start_session",
            self.session_types(),
            args,
        )
    }

    /// Returns the index of the order's command.
    fn unwind_order(&mut self, session: Argument, unwind: &Unwind) -> u64 {
        let args = vec![
            session,
            self.tx.pure(&unwind.is_ask),
            self.tx.pure(&unwind.size),
            self.tx.pure(&unwind.price),
            self.tx.pure(&IMMEDIATE_OR_CANCEL),
            self.tx.pure(&None::<u64>),
            self.tx.pure(&true),
            self.tx.pure(&None::<u64>),
        ];
        let index = self.commands;
        self.perp(
            "clearing_house",
            "place_limit_order",
            vec![self.collateral()],
            args,
        );
        index
    }

    /// Closes the session, allocating any margin the position now needs and returning the
    /// collateral it does not, and shares the clearing house again.
    fn end_session(&mut self, session: Argument) {
        let args = vec![
            session,
            self.obj(self.engine.cap),
            self.obj(self.engine.account),
            self.tx.pure(&true),
            self.tx.pure(&true),
        ];
        let result = self.perp("clearing_house", "end_session", self.session_types(), args);
        let [clearing_house, _summary] = result.to_nested(2)[..] else {
            unreachable!("to_nested returns as many as asked for")
        };
        self.perp(
            "clearing_house",
            "share",
            vec![self.collateral()],
            vec![clearing_house],
        );
    }

    /// Liquidates `liqee`, force-canceling its resting orders `cancel`, and unwinds what is
    /// taken over when `unwind` is given. Returns the index of the unwinding order's command,
    /// to tell its failures from the liquidation's.
    pub fn liquidation(
        &mut self,
        market: &MarketObjects,
        liqee: u64,
        cancel: &[u128],
        unwind: Option<&Unwind>,
    ) -> Option<u64> {
        let session = self.start_session(market);
        let liqee = self.tx.pure(&liqee);
        let cancel = self.tx.pure(&cancel.to_vec());
        self.perp(
            "clearing_house",
            "liquidate",
            vec![self.collateral()],
            vec![session, liqee, cancel],
        );
        let unwind_command = unwind.map(|unwind| self.unwind_order(session, unwind));
        self.end_session(session);
        unwind_command
    }

    /// Unwinds what is left of positions taken over earlier.
    pub fn unwind(&mut self, market: &MarketObjects, unwind: &Unwind) -> &mut Self {
        let session = self.start_session(market);
        self.unwind_order(session, unwind);
        self.end_session(session);
        self
    }

    /// Closes a bad-debt position against counterparties chosen by `plan`.
    pub fn adl(
        &mut self,
        market: &MarketObjects,
        adl_cap: Address,
        registry: Address,
        bad_debt_account: u64,
        open_orders: &[u128],
        plan: &Plan,
    ) -> &mut Self {
        let counterparties: Vec<u64> = plan.shares.iter().map(|s| s.account_id as u64).collect();
        let sizes: Vec<u64> = plan.shares.iter().map(|s| s.size).collect();
        let weights: Vec<u64> = plan.shares.iter().map(|s| s.weight).collect();
        let args = vec![
            self.obj(market.clearing_house),
            self.obj(adl_cap),
            self.obj(registry),
            self.tx.pure(&bad_debt_account),
            self.tx.pure(&open_orders.to_vec()),
            self.tx.pure(&counterparties),
            self.tx.pure(&sizes),
            self.tx.pure(&weights),
            self.obj(market.base_feed),
            self.obj(market.collateral_feed),
            self.obj(CLOCK),
        ];
        self.perp("adl", "execute_adl", vec![self.collateral()], args);
        self
    }

    /// Opens the liquidator's position object in a market.
    pub fn create_position(&mut self, clearing_house: Address) -> &mut Self {
        let args = vec![
            self.obj(clearing_house),
            self.obj(self.engine.cap),
            self.obj(self.engine.account),
        ];
        self.perp(
            "clearing_house",
            "create_market_position",
            self.session_types(),
            args,
        );
        self
    }
}

fn ident(name: &str) -> Identifier {
    Identifier::new(name).expect("a valid Move identifier")
}

#[cfg(test)]
mod tests {
    use haneul_sdk_types::{Command, Input, TransactionKind};

    use super::*;
    use crate::adl::Share;

    fn engine() -> Engine {
        Engine {
            package: Address::from_static("0xe2"),
            types_package: Address::from_static("0xe1"),
            collateral: "0x7f::tusd::TUSD".parse().unwrap(),
            role: "0xa7::authority::ASSISTANT".parse().unwrap(),
            account: Address::from_static("0xac"),
            cap: Address::from_static("0xca"),
        }
    }

    fn market() -> MarketObjects {
        MarketObjects {
            clearing_house: Address::from_static("0xc0"),
            base_feed: Address::from_static("0xb1"),
            collateral_feed: Address::from_static("0xb2"),
        }
    }

    fn offline(id: Address) -> ObjectInput {
        ObjectInput::shared(id, 1, true)
    }

    /// The calls of a built transaction as `module::function<type args>`.
    fn calls(tx: TransactionBuilder) -> (Vec<String>, Vec<Input>) {
        let mut tx = tx;
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
        let calls = ptb
            .commands
            .iter()
            .map(|c| match c {
                Command::MoveCall(call) => {
                    let types: Vec<String> =
                        call.type_arguments.iter().map(|t| t.to_string()).collect();
                    format!("{}::{}<{}>", call.module, call.function, types.join(","))
                }
                other => format!("{other:?}"),
            })
            .collect();
        (calls, ptb.inputs)
    }

    fn pure(inputs: &[Input]) -> Vec<Vec<u8>> {
        inputs
            .iter()
            .filter_map(|i| match i {
                Input::Pure(bytes) => Some(bytes.clone()),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn a_liquidation_refreshes_liquidates_unwinds_and_settles_in_one_go() {
        let engine = engine();
        let relay = Relay {
            package: Address::from_static("0x0a"),
            source: Address::from_static("0x5c"),
            config: Address::from_static("0xcf"),
        };
        let update = SignedUpdate {
            price_feed_storage: Address::from_static("0xb1"),
            storage_id: 4,
            price: 94_000 * 10u128.pow(18),
            confidence: 0,
            timestamp_ms: 1_790_000_000_000,
            public_key: vec![1; 32],
            signature: vec![2; 64],
        };
        let updates = [update];
        let mut b = Builder::new(&engine, &offline);
        b.refresh(Some((&relay, &updates)));
        let unwind_command = b.liquidation(
            &market(),
            42,
            &[7, 9],
            Some(&Unwind {
                is_ask: true,
                size: 300_000_000,
                price: 93_530_000_000_000,
            }),
        );
        assert_eq!(unwind_command, Some(4));
        let (calls, inputs) = calls(b.finish());
        assert_eq!(
            calls,
            vec![
                "price_feed_storage::update_price_feed<>".to_string(),
                format!(
                    "option::none<{}::account::IntegratorInfo>",
                    Address::from_static("0xe1")
                ),
                format!(
                    "clearing_house::start_session<{},{}>",
                    engine.collateral, engine.role
                ),
                format!("clearing_house::liquidate<{}>", engine.collateral),
                format!("clearing_house::place_limit_order<{}>", engine.collateral),
                format!(
                    "clearing_house::end_session<{},{}>",
                    engine.collateral, engine.role
                ),
                format!("clearing_house::share<{}>", engine.collateral),
            ]
        );
        let pure = pure(&inputs);
        // The liqee's account ID and the orders to cancel go in as BCS.
        assert!(pure.contains(&bcs_of(&42u64)));
        assert!(pure.contains(&bcs_of(&vec![7u128, 9u128])));
        // Immediate-or-cancel, reduce-only, at the unwind price.
        assert!(pure.contains(&bcs_of(&3u64)));
        assert!(pure.contains(&bcs_of(&93_530_000_000_000u64)));
        assert!(pure.contains(&bcs_of(&None::<u64>)));
    }

    #[test]
    fn a_liquidation_without_unwinding_takes_the_position_as_is() {
        let engine = engine();
        let mut b = Builder::new(&engine, &offline);
        assert_eq!(b.liquidation(&market(), 42, &[], None), None);
        let (calls, _) = calls(b.finish());
        assert!(
            !calls
                .iter()
                .any(|c| c.starts_with("clearing_house::place_limit_order"))
        );
        assert_eq!(calls.len(), 5);
    }

    #[test]
    fn adl_passes_the_plan_in_order() {
        let engine = engine();
        let plan = Plan {
            size: 300_000_000,
            shares: vec![
                Share {
                    account_id: 2,
                    size: 200_000_000,
                    weight: 666_666_666_666_666_667,
                },
                Share {
                    account_id: 1,
                    size: 100_000_000,
                    weight: 333_333_333_333_333_333,
                },
            ],
        };
        let mut b = Builder::new(&engine, &offline);
        b.adl(
            &market(),
            Address::from_static("0xad"),
            Address::from_static("0x4e"),
            5,
            &[11],
            &plan,
        );
        let (calls, inputs) = calls(b.finish());
        assert_eq!(
            calls,
            vec![format!("adl::execute_adl<{}>", engine.collateral)]
        );
        let pure = pure(&inputs);
        assert!(pure.contains(&bcs_of(&vec![2u64, 1u64])));
        assert!(pure.contains(&bcs_of(&vec![200_000_000u64, 100_000_000u64])));
        assert!(pure.contains(&bcs_of(&vec![
            666_666_666_666_666_667u64,
            333_333_333_333_333_333u64
        ])));
    }

    fn bcs_of<T: serde::Serialize>(value: &T) -> Vec<u8> {
        bcs::to_bytes(value).unwrap()
    }
}
