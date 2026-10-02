// Copyright (c) 2026 Geunhwa Jeong
// SPDX-License-Identifier: Apache-2.0

//! The full node: reading objects, resolving and simulating transactions, and executing them.

use std::time::Duration;

use anyhow::Context;
use haneul_rpc::field::{FieldMask, FieldMaskUtil};
use haneul_rpc::proto::haneul::rpc::v2 as proto;
use haneul_sdk_types::{Address, Transaction, UserSignature};
use haneul_transaction_builder::TransactionBuilder;
use serde::Serialize;
use url::Url;

#[derive(Clone)]
pub struct Chain {
    client: haneul_rpc::Client,
    execute_timeout: Duration,
}

/// An object as JSON, with what startup checks look at.
#[derive(Clone, Debug)]
pub struct ObjectInfo {
    pub object_type: String,
    /// The owning address, for address-owned objects.
    pub owner: Option<String>,
    pub shared: bool,
    pub json: serde_json::Value,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Abort {
    pub package: String,
    pub module: String,
    pub function: String,
    pub code: u64,
    /// The index of the command that aborted.
    pub command: Option<u64>,
}

/// What became of a transaction, simulated or executed.
#[derive(Clone, Debug)]
pub struct Outcome {
    pub digest: String,
    pub success: bool,
    pub abort: Option<Abort>,
    /// The failure, described, when it was not a Move abort.
    pub error: Option<String>,
    /// `(type, BCS)` of every event.
    pub events: Vec<(String, Vec<u8>)>,
    /// Computation and storage less the storage rebate, in the smallest unit of HANEUL.
    pub gas_used: i64,
    pub checkpoint: Option<u64>,
}

#[derive(Debug, thiserror::Error)]
pub enum ExecuteError {
    /// Refused before execution, for instance because an input was consumed by an earlier
    /// transaction: it did not run, and can be built again at once.
    #[error("rejected: {0}")]
    Rejected(String),
    /// No answer: it may still have run, so look it up before trying again.
    #[error("no answer: {0}")]
    Unknown(String),
}

/// Whether the validators turned the transaction down, so that it certainly did not run.
fn rejected(status: &tonic::Status) -> bool {
    matches!(
        status.code(),
        tonic::Code::InvalidArgument | tonic::Code::FailedPrecondition
    )
}

#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    /// The transaction would abort.
    #[error("aborts in {}::{} with {}", .0.module, .0.function, .0.code)]
    Aborted(Abort),
    /// The transaction would fail other than by an abort.
    #[error("would fail: {0}")]
    Failed(String),
    /// The full node could not resolve or simulate it.
    #[error("{0}")]
    Node(String),
}

/// Fields of an executed transaction the liquidator reads.
const EXECUTED: [&str; 6] = [
    "digest",
    "effects.status",
    "effects.gas_used",
    "events.events.event_type",
    "events.events.contents",
    "checkpoint",
];

impl Chain {
    pub fn new(url: &Url, execute_timeout: Duration) -> anyhow::Result<Self> {
        let client = haneul_rpc::Client::new(url.as_str()).context("Invalid gRPC URL")?;
        Ok(Self {
            client,
            execute_timeout,
        })
    }

    pub async fn chain_id(&self) -> anyhow::Result<String> {
        let response = self
            .client
            .clone()
            .ledger_client()
            .get_service_info(proto::GetServiceInfoRequest::default())
            .await?
            .into_inner();
        Ok(response.chain_id().to_owned())
    }

    pub async fn object(&self, id: Address) -> anyhow::Result<ObjectInfo> {
        let request = proto::GetObjectRequest::new(&id).with_read_mask(FieldMask::from_paths([
            "object_type",
            "owner",
            "json",
        ]));
        let response = self
            .client
            .clone()
            .ledger_client()
            .get_object(request)
            .await
            .with_context(|| format!("Failed to read object {id}"))?
            .into_inner();
        let object = response.object();
        let owner = object.owner();
        let kind = owner.kind();
        Ok(ObjectInfo {
            object_type: object.object_type().to_owned(),
            owner: (kind == proto::owner::OwnerKind::Address).then(|| owner.address().to_owned()),
            shared: matches!(
                kind,
                proto::owner::OwnerKind::Shared | proto::owner::OwnerKind::ConsensusAddress
            ),
            json: object
                .json_opt()
                .map(to_json)
                .unwrap_or(serde_json::Value::Null),
        })
    }

    /// An address's balance of a coin type, in raw units.
    pub async fn balance(&self, owner: Address, coin_type: &str) -> anyhow::Result<u64> {
        let request = proto::GetBalanceRequest::default()
            .with_owner(owner.to_string())
            .with_coin_type(coin_type);
        let response = self
            .client
            .clone()
            .state_client()
            .get_balance(request)
            .await?
            .into_inner();
        Ok(response.balance().balance())
    }

    pub async fn reference_gas_price(&self) -> anyhow::Result<u64> {
        let request = proto::GetEpochRequest::default()
            .with_read_mask(FieldMask::from_paths(["reference_gas_price"]));
        let response = self
            .client
            .clone()
            .ledger_client()
            .get_epoch(request)
            .await?
            .into_inner();
        Ok(response.epoch().reference_gas_price())
    }

    /// Resolves the inputs, picks the gas and estimates the budget. The full node simulates the
    /// transaction to do so, so a transaction that would fail is refused here.
    pub async fn build(&self, tx: TransactionBuilder) -> Result<Transaction, BuildError> {
        let mut client = self.client.clone();
        tx.build(&mut client).await.map_err(|e| match e {
            haneul_transaction_builder::Error::SimulationFailure(failure) => {
                let error = failure.execution_error();
                match abort_of(error) {
                    Some(abort) => BuildError::Aborted(abort),
                    None => BuildError::Failed(describe(error)),
                }
            }
            other => BuildError::Node(other.to_string()),
        })
    }

    /// Simulates a built transaction for what it would do.
    pub async fn simulate(&self, tx: &Transaction) -> anyhow::Result<Outcome> {
        let request = proto::SimulateTransactionRequest::default()
            .with_transaction(proto::Transaction::from(tx.clone()))
            .with_read_mask(FieldMask::from_paths(
                EXECUTED.map(|p| format!("transaction.{p}")),
            ))
            .with_checks(proto::simulate_transaction_request::TransactionChecks::Enabled)
            .with_do_gas_selection(false);
        let response = self
            .client
            .clone()
            .execution_client()
            .simulate_transaction(request)
            .await?
            .into_inner();
        let mut outcome = outcome(response.transaction());
        outcome.digest = tx.digest().to_string();
        Ok(outcome)
    }

    /// Executes a signed transaction and waits until the full node has it in a checkpoint, so
    /// that the next transaction is built and simulated on top of it: built any earlier, it
    /// would pick gas coins and object versions this one already consumed.
    pub async fn execute(
        &self,
        tx: &Transaction,
        signature: UserSignature,
    ) -> Result<Outcome, ExecuteError> {
        use haneul_rpc::client::ExecuteAndWaitError;

        let request = proto::ExecuteTransactionRequest::default()
            .with_transaction(proto::Transaction::from(tx.clone()))
            .with_signatures(vec![proto::UserSignature::from(signature)])
            .with_read_mask(FieldMask::from_paths(EXECUTED));
        let mut client = self.client.clone();
        let response = match client
            .execute_transaction_and_wait_for_checkpoint(request, self.execute_timeout)
            .await
        {
            Ok(response) => response,
            // Executed, but not seen in a checkpoint in time: the effects are known.
            Err(ExecuteAndWaitError::CheckpointTimeout(response))
            | Err(ExecuteAndWaitError::CheckpointStreamError { response, .. }) => {
                tracing::warn!(digest = %tx.digest(), "Executed but not yet in a checkpoint");
                response
            }
            Err(ExecuteAndWaitError::RpcError(status)) if rejected(&status) => {
                return Err(ExecuteError::Rejected(status.message().to_owned()));
            }
            Err(e) => return Err(ExecuteError::Unknown(e.to_string())),
        };
        let mut outcome = outcome(response.get_ref().transaction());
        if outcome.digest.is_empty() {
            outcome.digest = tx.digest().to_string();
        }
        Ok(outcome)
    }

    /// A transaction's outcome, or None while the node does not have it.
    pub async fn transaction(&self, digest: &str) -> anyhow::Result<Option<Outcome>> {
        let request = proto::GetTransactionRequest::default()
            .with_digest(digest)
            .with_read_mask(FieldMask::from_paths(EXECUTED));
        match self
            .client
            .clone()
            .ledger_client()
            .get_transaction(request)
            .await
        {
            Ok(response) => Ok(Some(outcome(response.into_inner().transaction()))),
            Err(status) if status.code() == tonic::Code::NotFound => Ok(None),
            Err(status) => Err(status.into()),
        }
    }
}

fn outcome(tx: &proto::ExecutedTransaction) -> Outcome {
    let effects = tx.effects();
    let status = effects.status();
    let success = status.success();
    let (abort, error) = if success {
        (None, None)
    } else {
        let error = status.error();
        (abort_of(error), Some(describe(error)))
    };
    let gas = effects.gas_used();
    let gas_used =
        gas.computation_cost() as i64 + gas.storage_cost() as i64 - gas.storage_rebate() as i64;
    let events = tx
        .events()
        .events()
        .iter()
        .map(|event| {
            (
                event.event_type().to_owned(),
                event.contents().value().to_vec(),
            )
        })
        .collect();
    Outcome {
        digest: tx.digest().to_owned(),
        success,
        abort,
        error,
        events,
        gas_used,
        checkpoint: tx.checkpoint_opt(),
    }
}

fn abort_of(error: &proto::ExecutionError) -> Option<Abort> {
    let abort = error.abort_opt()?;
    let location = abort.location();
    Some(Abort {
        package: location.package().to_owned(),
        module: location.module().to_owned(),
        function: location.function_name().to_owned(),
        code: abort.abort_code(),
        command: error.command_opt(),
    })
}

fn describe(error: &proto::ExecutionError) -> String {
    match error.description_opt() {
        Some(description) => description.to_owned(),
        None => format!("{error:?}"),
    }
}

fn to_json(value: &prost_types::Value) -> serde_json::Value {
    use prost_types::value::Kind;
    use serde_json::Value;
    match &value.kind {
        None | Some(Kind::NullValue(_)) => Value::Null,
        // Protobuf carries every number as a double. Move integers small enough to be exact in
        // one (u8 to u32; larger ones come as strings) are read back as integers.
        Some(Kind::NumberValue(n)) if n.fract() == 0.0 && n.abs() < 9_007_199_254_740_992.0 => {
            Value::Number((*n as i64).into())
        }
        Some(Kind::NumberValue(n)) => {
            serde_json::Number::from_f64(*n).map_or(Value::Null, Value::Number)
        }
        Some(Kind::StringValue(s)) => Value::String(s.clone()),
        Some(Kind::BoolValue(b)) => Value::Bool(*b),
        Some(Kind::StructValue(s)) => Value::Object(
            s.fields
                .iter()
                .map(|(k, v)| (k.clone(), to_json(v)))
                .collect(),
        ),
        Some(Kind::ListValue(l)) => Value::Array(l.values.iter().map(to_json).collect()),
    }
}

#[cfg(test)]
mod tests {
    use prost_types::value::Kind;
    use prost_types::{Struct, Value};

    use super::*;

    fn number(n: f64) -> Value {
        Value {
            kind: Some(Kind::NumberValue(n)),
        }
    }

    #[test]
    fn whole_numbers_come_back_as_integers() {
        let source = Value {
            kind: Some(Kind::StructValue(Struct {
                fields: [
                    ("source_id".to_owned(), number(1.0)),
                    ("ratio".to_owned(), number(0.5)),
                    (
                        "version".to_owned(),
                        Value {
                            kind: Some(Kind::StringValue("1".into())),
                        },
                    ),
                ]
                .into_iter()
                .collect(),
            })),
        };
        let json = to_json(&source);
        assert_eq!(json["source_id"].as_u64(), Some(1));
        assert_eq!(json["ratio"].as_f64(), Some(0.5));
        assert_eq!(json["version"], "1");
    }
}
