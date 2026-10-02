// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! The crank: relay the market's signed base price when the oracle service has one, then
//! `clearing_house::update_funding`, which samples the due TWAPs and settles the due funding.
//! Both read the base feed, which aborts when its price is older than the market's tolerance:
//! the relay is what lets a crank through while the relayer is behind.

use haneul_sdk_types::{Address, Identifier, TypeTag};
use haneul_transaction_builder::{Function, ObjectInput, TransactionBuilder};
use perp_bot_common::oracle::{Relay, SignedUpdate};

const CLOCK: Address = Address::from_static("0x6");

/// What a crank of one market names.
#[derive(Clone, Debug)]
pub struct Crank {
    /// The engine package calls go to.
    pub package: Address,
    pub collateral: TypeTag,
    pub clearing_house: Address,
    pub base_feed: Address,
}

/// Builds one crank. `object` turns an object ID into an input: production leaves resolution to
/// the full node, tests resolve offline.
pub fn crank(
    crank: &Crank,
    relay: Option<(&Relay, &[SignedUpdate])>,
    object: &dyn Fn(Address) -> ObjectInput,
) -> TransactionBuilder {
    let mut tx = TransactionBuilder::new();
    if let Some((relay, updates)) = relay {
        for update in updates {
            let args = vec![
                tx.object(object(relay.source)),
                tx.object(object(relay.config)),
                tx.object(object(update.price_feed_storage)),
                tx.pure(&update.price),
                tx.pure(&update.confidence),
                tx.pure(&update.timestamp_ms),
                tx.pure(&update.public_key),
                tx.pure(&update.signature),
                tx.object(object(CLOCK)),
            ];
            tx.move_call(
                Function::new(
                    relay.package,
                    ident("price_feed_storage"),
                    ident("update_price_feed"),
                ),
                args,
            );
        }
    }
    let args = vec![
        tx.object(object(crank.clearing_house)),
        tx.object(object(crank.base_feed)),
        tx.object(object(CLOCK)),
    ];
    tx.move_call(
        Function::new(
            crank.package,
            ident("clearing_house"),
            ident("update_funding"),
        )
        .with_type_args(vec![crank.collateral.clone()]),
        args,
    );
    tx
}

fn ident(name: &str) -> Identifier {
    Identifier::new(name).expect("a valid Move identifier")
}

#[cfg(test)]
mod tests {
    use haneul_sdk_types::{Command, Input, TransactionKind};

    use super::*;

    fn offline(id: Address) -> ObjectInput {
        ObjectInput::shared(id, 1, true)
    }

    fn calls(mut tx: TransactionBuilder) -> (Vec<String>, Vec<Input>) {
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

    fn market() -> Crank {
        Crank {
            package: Address::from_static("0xe2"),
            collateral: "0x7f::tusd::TUSD".parse().unwrap(),
            clearing_house: Address::from_static("0xc0"),
            base_feed: Address::from_static("0xb1"),
        }
    }

    #[test]
    fn a_crank_without_signed_prices_is_one_call() {
        let (calls, inputs) = calls(crank(&market(), None, &offline));
        assert_eq!(
            calls,
            vec![format!(
                "clearing_house::update_funding<{}>",
                market().collateral
            )]
        );
        // The clearing house, its base feed and the clock.
        assert_eq!(inputs.len(), 3);
    }

    #[test]
    fn signed_prices_go_in_front_of_the_crank() {
        let relay = Relay {
            package: Address::from_static("0x0a"),
            source: Address::from_static("0x5c"),
            config: Address::from_static("0xcf"),
        };
        let update = SignedUpdate {
            price_feed_storage: Address::from_static("0xb1"),
            storage_id: 0,
            price: 100_000 * 10u128.pow(18),
            confidence: 0,
            timestamp_ms: 1_790_000_000_000,
            public_key: vec![1; 32],
            signature: vec![2; 64],
        };
        let updates = [update];
        let (calls, inputs) = calls(crank(&market(), Some((&relay, &updates)), &offline));
        assert_eq!(
            calls,
            vec![
                "price_feed_storage::update_price_feed<>".to_string(),
                format!("clearing_house::update_funding<{}>", market().collateral),
            ]
        );
        let pure: Vec<Vec<u8>> = inputs
            .iter()
            .filter_map(|i| match i {
                Input::Pure(bytes) => Some(bytes.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(
            pure[0],
            bcs::to_bytes(&(100_000u128 * 10u128.pow(18))).unwrap()
        );
        assert_eq!(pure[2], bcs::to_bytes(&1_790_000_000_000u64).unwrap());
    }
}
