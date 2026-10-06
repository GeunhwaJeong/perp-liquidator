// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! The maker's transactions. A round is one atomic session of the maker's account: relay the
//! prices, cancel what rests, open the session, post the ladder (post-only, with an expiry),
//! close the session through the fee extension when the deployment has one. Either the whole
//! new book lands or nothing changes.
//!
//! Object inputs are given by ID only; the full node fills in versions and mutability when it
//! resolves the transaction.

use haneul_sdk_types::{Address, Identifier, TypeTag};
use haneul_transaction_builder::{Argument, Function, ObjectInput, TransactionBuilder};
use perp_bot_common::deployment::FeeObjects;
use perp_bot_common::oracle::{Relay, SignedUpdate};

use crate::model::{Flatten, Level};

const CLOCK: Address = Address::from_static("0x6");
/// `place_limit_order` order types.
const POST_ONLY: u64 = 2;
const IMMEDIATE_OR_CANCEL: u64 = 3;

/// What every call needs to know about the engine and the account it trades.
#[derive(Clone, Debug)]
pub struct Engine {
    /// The package calls go to.
    pub package: Address,
    /// The package the engine's types were first published at.
    pub types_package: Address,
    pub collateral: TypeTag,
    /// The role of the capability: `authority::ADMIN` or `authority::ASSISTANT`.
    pub role: TypeTag,
    pub account: Address,
    pub cap: Address,
    /// The fee-tier extension, with the registry it needs; sessions end through it when set.
    pub fees: Option<(FeeObjects, Address)>,
}

#[derive(Clone, Copy, Debug)]
pub struct MarketObjects {
    pub clearing_house: Address,
    pub base_feed: Address,
    pub collateral_feed: Address,
}

/// Prices to relay in front of the calls.
pub type Refresh<'a> = Option<(&'a Relay, &'a [SignedUpdate])>;

/// What a round posts.
#[derive(Clone, Debug, Default)]
pub struct Round {
    /// Resting orders to cancel first, by ID. Unknown IDs are skipped, not failed on.
    pub cancel: Vec<u128>,
    pub levels: Vec<Level>,
    pub flatten: Option<Flatten>,
    /// Every level expires at this time, in milliseconds.
    pub expires_at_ms: u64,
    /// A client order ID per level, so that posted orders can be told apart in the events.
    pub client_id_base: u64,
}

/// Turns an object ID into a transaction input. Production leaves resolution to the full node
/// (`ObjectInput::new`); tests resolve offline.
pub type Resolver = dyn Fn(Address) -> ObjectInput + Sync;

pub struct Builder<'a> {
    engine: &'a Engine,
    object: &'a Resolver,
    tx: TransactionBuilder,
    /// `module::function` of every call so far, in order: what an abort's command index names.
    calls: Vec<String>,
}

impl<'a> Builder<'a> {
    pub fn new(engine: &'a Engine, object: &'a Resolver) -> Self {
        Self {
            engine,
            object,
            tx: TransactionBuilder::new(),
            calls: Vec::new(),
        }
    }

    pub fn finish(self) -> TransactionBuilder {
        self.tx
    }

    /// The calls so far, indexed, for failure messages.
    pub fn commands(&self) -> String {
        self.calls
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
        self.calls.push(format!("{module}::{function}"));
        let function =
            Function::new(package, ident(module), ident(function)).with_type_args(type_args);
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

    /// Cancels resting orders by ID on the shared clearing house, before any session takes it.
    /// IDs the engine no longer knows (filled or expired) are skipped.
    fn cancel(&mut self, market: &MarketObjects, ids: &[u128]) {
        if ids.is_empty() {
            return;
        }
        let args = vec![
            self.obj(market.clearing_house),
            self.obj(self.engine.cap),
            self.obj(self.engine.account),
            self.tx.pure(&ids.to_vec()),
        ];
        self.perp(
            "clearing_house",
            "try_cancel_orders",
            self.session_types(),
            args,
        );
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

    #[allow(clippy::too_many_arguments)]
    fn limit_order(
        &mut self,
        session: Argument,
        is_bid: bool,
        size: u64,
        price: u64,
        order_type: u64,
        client_id: Option<u64>,
        reduce_only: bool,
        expires_at_ms: Option<u64>,
    ) {
        // The engine's side flag is true for an ask.
        let args = vec![
            session,
            self.tx.pure(&!is_bid),
            self.tx.pure(&size),
            self.tx.pure(&price),
            self.tx.pure(&order_type),
            self.tx.pure(&client_id),
            self.tx.pure(&reduce_only),
            self.tx.pure(&expires_at_ms),
        ];
        self.perp(
            "clearing_house",
            "place_limit_order",
            vec![self.collateral()],
            args,
        );
    }

    /// Closes the session, allocating the margin the ladder needs and keeping the rest where
    /// it is, and shares the clearing house again. Through the fee extension when configured,
    /// so that maker volume and rebates are credited.
    fn end_session(&mut self, session: Argument) {
        let result = match self.engine.fees.clone() {
            Some((fees, registry)) => {
                let args = vec![
                    session,
                    self.obj(self.engine.cap),
                    self.obj(self.engine.account),
                    self.obj(registry),
                    self.obj(fees.schedule),
                    self.obj(fees.tier_registry),
                    self.tx.pure(&true),
                    self.tx.pure(&false),
                    self.obj(CLOCK),
                ];
                self.call(
                    fees.package,
                    "fees",
                    "end_session",
                    self.session_types(),
                    args,
                )
            }
            None => {
                let args = vec![
                    session,
                    self.obj(self.engine.cap),
                    self.obj(self.engine.account),
                    self.tx.pure(&true),
                    self.tx.pure(&false),
                ];
                self.perp("clearing_house", "end_session", self.session_types(), args)
            }
        };
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

    /// A full round: cancel, then a session that posts the ladder and the flattening order.
    /// With nothing to post and nothing to cancel, nothing is added.
    pub fn round(&mut self, market: &MarketObjects, round: &Round) -> &mut Self {
        self.cancel(market, &round.cancel);
        if round.levels.is_empty() && round.flatten.is_none() {
            return self;
        }
        let session = self.start_session(market);
        if let Some(flatten) = &round.flatten {
            self.limit_order(
                session,
                flatten.is_bid,
                flatten.size,
                flatten.price,
                IMMEDIATE_OR_CANCEL,
                None,
                true,
                None,
            );
        }
        for (i, level) in round.levels.iter().enumerate() {
            self.limit_order(
                session,
                level.is_bid,
                level.size,
                level.price,
                POST_ONLY,
                Some(round.client_id_base + i as u64),
                level.reduce_only,
                Some(round.expires_at_ms),
            );
        }
        self.end_session(session);
        self
    }

    /// Cancels every resting order and nothing else: the last round before stopping.
    pub fn pull(&mut self, market: &MarketObjects, ids: &[u128]) -> &mut Self {
        self.cancel(market, ids);
        self
    }

    /// One immediate-or-cancel order in a session of its own: the flow simulator's trade.
    pub fn taker(
        &mut self,
        market: &MarketObjects,
        is_bid: bool,
        size: u64,
        price: u64,
        reduce_only: bool,
    ) -> &mut Self {
        let session = self.start_session(market);
        self.limit_order(
            session,
            is_bid,
            size,
            price,
            IMMEDIATE_OR_CANCEL,
            None,
            reduce_only,
            None,
        );
        self.end_session(session);
        self
    }

    /// Opens the account's position object in a market.
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

    /// Sets the account's own initial margin ratio on the market (1 / leverage, ifixed).
    pub fn set_leverage(
        &mut self,
        clearing_house: Address,
        initial_margin_ratio: &[u8; 32],
    ) -> &mut Self {
        let args = vec![
            self.obj(clearing_house),
            self.obj(self.engine.cap),
            self.obj(self.engine.account),
            self.tx.pure(initial_margin_ratio),
        ];
        self.perp(
            "clearing_house",
            "set_position_initial_margin_ratio",
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

    fn engine(fees: bool) -> Engine {
        Engine {
            package: Address::from_static("0xe2"),
            types_package: Address::from_static("0xe1"),
            collateral: "0x7f::tusd::TUSD".parse().unwrap(),
            role: "0xca::authority::ASSISTANT".parse().unwrap(),
            account: Address::from_static("0xacc"),
            cap: Address::from_static("0xca9"),
            fees: fees.then(|| {
                (
                    FeeObjects {
                        package: Address::from_static("0xfe"),
                        schedule: Address::from_static("0x5c"),
                        tier_registry: Address::from_static("0x7e"),
                    },
                    Address::from_static("0x5"),
                )
            }),
        }
    }

    fn market() -> MarketObjects {
        MarketObjects {
            clearing_house: Address::from_static("0xc0b6"),
            base_feed: Address::from_static("0xfa"),
            collateral_feed: Address::from_static("0x1d"),
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
            .filter_map(|c| match c {
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
                    Some(format!(
                        "{}::{}<{}>",
                        call.module,
                        call.function,
                        types.join(",")
                    ))
                }
                _ => None,
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
    fn a_round_cancels_posts_the_ladder_post_only_with_expiry_and_ends_through_fees() {
        let engine = engine(true);
        let mut b = Builder::new(&engine, &offline);
        let round = Round {
            cancel: vec![7, 9],
            levels: vec![
                Level {
                    is_bid: true,
                    price: 84_932_000_000_000,
                    size: 10_000_000,
                    reduce_only: false,
                },
                Level {
                    is_bid: false,
                    price: 85_068_000_000_000,
                    size: 10_000_000,
                    reduce_only: false,
                },
            ],
            flatten: None,
            expires_at_ms: 1_791_000_000_000,
            client_id_base: 100,
        };
        b.round(&market(), &round);
        let (calls, inputs) = calls(b.finish());
        assert_eq!(
            calls,
            vec![
                "clearing_house::try_cancel_orders<0x7f::tusd::TUSD,0xca::authority::ASSISTANT>",
                "option::none<0xe1::account::IntegratorInfo>",
                "clearing_house::start_session<0x7f::tusd::TUSD,0xca::authority::ASSISTANT>",
                "clearing_house::place_limit_order<0x7f::tusd::TUSD>",
                "clearing_house::place_limit_order<0x7f::tusd::TUSD>",
                "fees::end_session<0x7f::tusd::TUSD,0xca::authority::ASSISTANT>",
                "clearing_house::share<0x7f::tusd::TUSD>",
            ]
        );
        // Equal pure values share one input, so arguments are looked for, not indexed: the IDs
        // to cancel as a BCS vector<u128>; the bid's side flag (false: the engine's flag is true
        // for an ask), size, price, post-only type, client ids 100 and 101, and the expiry.
        let pure = pure(&inputs);
        assert_eq!(pure[0], bcs::to_bytes(&vec![7u128, 9]).unwrap());
        for wanted in [
            bcs::to_bytes(&false).unwrap(),
            bcs::to_bytes(&true).unwrap(),
            bcs::to_bytes(&10_000_000u64).unwrap(),
            bcs::to_bytes(&84_932_000_000_000u64).unwrap(),
            bcs::to_bytes(&85_068_000_000_000u64).unwrap(),
            bcs::to_bytes(&POST_ONLY).unwrap(),
            bcs::to_bytes(&Some(100u64)).unwrap(),
            bcs::to_bytes(&Some(101u64)).unwrap(),
            bcs::to_bytes(&Some(1_791_000_000_000u64)).unwrap(),
        ] {
            assert!(pure.contains(&wanted), "missing input {wanted:?}");
        }
    }

    #[test]
    fn without_the_fee_extension_the_core_ends_the_session_and_a_flatten_goes_first() {
        let engine = engine(false);
        let mut b = Builder::new(&engine, &offline);
        let round = Round {
            cancel: vec![],
            levels: vec![Level {
                is_bid: false,
                price: 85_000_000_000_000,
                size: 10_000_000,
                reduce_only: true,
            }],
            flatten: Some(Flatten {
                is_bid: false,
                price: 84_830_000_000_000,
                size: 350_000_000,
            }),
            expires_at_ms: 1,
            client_id_base: 0,
        };
        b.round(&market(), &round);
        let (calls, inputs) = calls(b.finish());
        assert_eq!(
            calls,
            vec![
                "option::none<0xe1::account::IntegratorInfo>",
                "clearing_house::start_session<0x7f::tusd::TUSD,0xca::authority::ASSISTANT>",
                "clearing_house::place_limit_order<0x7f::tusd::TUSD>",
                "clearing_house::place_limit_order<0x7f::tusd::TUSD>",
                "clearing_house::end_session<0x7f::tusd::TUSD,0xca::authority::ASSISTANT>",
                "clearing_house::share<0x7f::tusd::TUSD>",
            ]
        );
        // Equal pure values share one input, so the flatten's arguments are looked for, not
        // indexed: immediate-or-cancel, reduce-only, no expiry, and the post-only level with one.
        let pure = pure(&inputs);
        for wanted in [
            bcs::to_bytes(&IMMEDIATE_OR_CANCEL).unwrap(),
            bcs::to_bytes(&POST_ONLY).unwrap(),
            bcs::to_bytes(&true).unwrap(),
            bcs::to_bytes(&None::<u64>).unwrap(),
            bcs::to_bytes(&Some(1u64)).unwrap(),
            bcs::to_bytes(&350_000_000u64).unwrap(),
        ] {
            assert!(pure.contains(&wanted), "missing input {wanted:?}");
        }
    }

    #[test]
    fn an_empty_round_only_cancels_and_a_pull_cancels_nothing_else() {
        let engine = engine(true);
        let mut b = Builder::new(&engine, &offline);
        b.round(
            &market(),
            &Round {
                cancel: vec![1],
                ..Round::default()
            },
        );
        let (made, _) = calls(b.finish());
        assert_eq!(made.len(), 1);
        let mut b = Builder::new(&engine, &offline);
        b.pull(&market(), &[]);
        let (made, _) = calls(b.finish());
        assert!(made.is_empty());
    }
}
