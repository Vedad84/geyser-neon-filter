use anyhow::anyhow;
use anyhow::Result;
use kafka_common::kafka_structs::KafkaLegacyMessage;
use kafka_common::kafka_structs::KafkaLoadedMessage;
use kafka_common::kafka_structs::KafkaReplicaBlockInfo;
use kafka_common::kafka_structs::KafkaSanitizedMessage;
use kafka_common::kafka_structs::KafkaTransactionStatusMeta;
use kafka_common::kafka_structs::KafkaTransactionTokenBalance;
use kafka_common::kafka_structs::NotifyTransaction;
use kafka_common::kafka_structs::UpdateAccount;
use postgres_types::FromSql;
use solana_runtime::bank::RewardType;
use solana_sdk::{
    instruction::CompiledInstruction,
    message::{
        v0::{self, LoadedAddresses, MessageAddressTableLookup},
        MessageHeader,
    },
    transaction::TransactionError,
};
use solana_transaction_status::InnerInstructions;
use solana_transaction_status::Reward;
use std::fmt;
use tokio_postgres::types::ToSql;

const MAX_TRANSACTION_STATUS_LEN: usize = 256;

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct DbAccountInfo {
    pub pubkey: Vec<u8>,
    pub lamports: i64,
    pub owner: Vec<u8>,
    pub executable: bool,
    pub rent_epoch: i64,
    pub data: Vec<u8>,
    pub slot: i64,
    pub write_version: i64,
    pub txn_signature: Option<Vec<u8>>,
}

#[derive(Clone, Debug, FromSql, ToSql)]
#[postgres(name = "CompiledInstruction")]
pub struct DbCompiledInstruction {
    pub program_id_index: i16,
    pub accounts: Vec<i16>,
    pub data: Vec<u8>,
}
#[derive(Clone, Debug, FromSql, ToSql)]
#[postgres(name = "TransactionTokenBalance")]
pub struct DbTransactionTokenBalance {
    pub account_index: i16,
    pub mint: String,
    pub ui_token_amount: Option<f64>,
    pub owner: String,
}

#[derive(Clone, Debug, FromSql, ToSql, PartialEq)]
#[postgres(name = "TransactionErrorCode")]
pub enum DbTransactionErrorCode {
    AccountInUse,
    AccountLoadedTwice,
    AccountNotFound,
    ProgramAccountNotFound,
    InsufficientFundsForFee,
    InvalidAccountForFee,
    AlreadyProcessed,
    BlockhashNotFound,
    InstructionError,
    CallChainTooDeep,
    MissingSignatureForFee,
    InvalidAccountIndex,
    SignatureFailure,
    InvalidProgramForExecution,
    SanitizeFailure,
    ClusterMaintenance,
    AccountBorrowOutstanding,
    WouldExceedMaxAccountCostLimit,
    WouldExceedMaxBlockCostLimit,
    UnsupportedVersion,
    InvalidWritableAccount,
    WouldExceedMaxAccountDataCostLimit,
    TooManyAccountLocks,
    AddressLookupTableNotFound,
    InvalidAddressLookupTableOwner,
    InvalidAddressLookupTableData,
    InvalidAddressLookupTableIndex,
    InvalidRentPayingAccount,
    WouldExceedMaxVoteCostLimit,
    WouldExceedAccountDataBlockLimit,
    WouldExceedAccountDataTotalLimit,
    DuplicateInstruction,
    InsufficientFundsForRent,
}

#[derive(Clone, Debug, FromSql, ToSql, PartialEq)]
#[postgres(name = "TransactionError")]
pub struct DbTransactionError {
    error_code: DbTransactionErrorCode,
    error_detail: Option<String>,
}

#[derive(Clone, Debug, FromSql, ToSql)]
#[postgres(name = "InnerInstructions")]
pub struct DbInnerInstructions {
    pub index: i16,
    pub instructions: Vec<DbCompiledInstruction>,
}

#[derive(Clone, Debug, FromSql, ToSql)]
#[postgres(name = "TransactionStatusMeta")]
pub struct DbTransactionStatusMeta {
    pub error: Option<DbTransactionError>,
    pub fee: i64,
    pub pre_balances: Vec<i64>,
    pub post_balances: Vec<i64>,
    pub inner_instructions: Option<Vec<DbInnerInstructions>>,
    pub log_messages: Option<Vec<String>>,
    pub pre_token_balances: Option<Vec<DbTransactionTokenBalance>>,
    pub post_token_balances: Option<Vec<DbTransactionTokenBalance>>,
    pub rewards: Option<Vec<DbReward>>,
}

#[derive(Clone, Debug, FromSql, ToSql)]
#[postgres(name = "TransactionMessageHeader")]
pub struct DbTransactionMessageHeader {
    pub num_required_signatures: i16,
    pub num_readonly_signed_accounts: i16,
    pub num_readonly_unsigned_accounts: i16,
}

#[derive(Clone, Debug, FromSql, ToSql)]
#[postgres(name = "TransactionMessage")]
pub struct DbTransactionMessage {
    pub header: DbTransactionMessageHeader,
    pub account_keys: Vec<Vec<u8>>,
    pub recent_blockhash: Vec<u8>,
    pub instructions: Vec<DbCompiledInstruction>,
}

#[derive(Clone, Debug, FromSql, ToSql)]
#[postgres(name = "TransactionMessageAddressTableLookup")]
pub struct DbTransactionMessageAddressTableLookup {
    pub account_key: Vec<u8>,
    pub writable_indexes: Vec<i16>,
    pub readonly_indexes: Vec<i16>,
}

#[derive(Clone, Debug, FromSql, ToSql)]
#[postgres(name = "TransactionMessageV0")]
pub struct DbTransactionMessageV0 {
    pub header: DbTransactionMessageHeader,
    pub account_keys: Vec<Vec<u8>>,
    pub recent_blockhash: Vec<u8>,
    pub instructions: Vec<DbCompiledInstruction>,
    pub address_table_lookups: Vec<DbTransactionMessageAddressTableLookup>,
}

#[derive(Clone, Debug, FromSql, ToSql)]
#[postgres(name = "LoadedAddresses")]
pub struct DbLoadedAddresses {
    pub writable: Vec<Vec<u8>>,
    pub readonly: Vec<Vec<u8>>,
}

#[derive(Clone, Debug, FromSql, ToSql)]
#[postgres(name = "LoadedMessageV0")]
pub struct DbLoadedMessageV0 {
    pub message: DbTransactionMessageV0,
    pub loaded_addresses: DbLoadedAddresses,
}

#[derive(Clone)]
pub struct DbTransaction {
    pub signature: Vec<u8>,
    pub is_vote: bool,
    pub slot: i64,
    pub message_type: i16,
    pub legacy_message: Option<DbTransactionMessage>,
    pub v0_loaded_message: Option<DbLoadedMessageV0>,
    pub message_hash: Vec<u8>,
    pub meta: DbTransactionStatusMeta,
    pub signatures: Vec<Vec<u8>>,
    pub index: i64,
}

#[derive(Clone, Debug)]
pub struct DbBlockInfo {
    pub slot: i64,
    pub blockhash: String,
    pub rewards: Vec<DbReward>,
    pub block_time: Option<i64>,
    pub block_height: Option<i64>,
}

#[derive(Clone, Debug, FromSql, ToSql, Eq, PartialEq)]
#[postgres(name = "RewardType")]
pub enum DbRewardType {
    Fee,
    Rent,
    Staking,
    Voting,
}

#[derive(Clone, Debug, FromSql, ToSql)]
#[postgres(name = "Reward")]
pub struct DbReward {
    pub pubkey: String,
    pub lamports: i64,
    pub post_balance: i64,
    pub reward_type: Option<DbRewardType>,
    pub commission: Option<i16>,
}

impl fmt::Display for DbAccountInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

fn range_check(lamports: u64, rent_epoch: u64, write_version: u64) -> Result<()> {
    if lamports > std::i64::MAX as u64 {
        return Err(anyhow!("account_info.lamports greater than std::i64::MAX!"));
    }
    if rent_epoch > std::i64::MAX as u64 {
        return Err(anyhow!(
            "account_info.rent_epoch greater than std::i64::MAX!"
        ));
    }
    if write_version > std::i64::MAX as u64 {
        return Err(anyhow!(
            "account_info.write_version greater than std::i64::MAX!"
        ));
    }
    Ok(())
}

impl TryFrom<&UpdateAccount> for DbAccountInfo {
    type Error = anyhow::Error;

    fn try_from(update_account: &UpdateAccount) -> Result<Self> {
        match &update_account.account {
            kafka_common::kafka_structs::KafkaReplicaAccountInfoVersions::V0_0_1(account_info) => {
                range_check(
                    account_info.lamports,
                    account_info.rent_epoch,
                    account_info.write_version,
                )?;

                Ok(DbAccountInfo {
                    pubkey: account_info.pubkey.clone(),
                    lamports: account_info.lamports as i64,
                    owner: account_info.owner.clone(),
                    executable: account_info.executable,
                    rent_epoch: account_info.rent_epoch as i64,
                    data: account_info.data.clone(),
                    slot: update_account.slot as i64,
                    write_version: account_info.write_version as i64,
                    txn_signature: None,
                })
            }
            kafka_common::kafka_structs::KafkaReplicaAccountInfoVersions::V0_0_2(account_info) => {
                range_check(
                    account_info.lamports,
                    account_info.rent_epoch,
                    account_info.write_version,
                )?;

                Ok(DbAccountInfo {
                    pubkey: account_info.pubkey.clone(),
                    lamports: account_info.lamports as i64,
                    owner: account_info.owner.clone(),
                    executable: account_info.executable,
                    rent_epoch: account_info.rent_epoch as i64,
                    data: account_info.data.clone(),
                    slot: update_account.slot as i64,
                    write_version: account_info.write_version as i64,
                    txn_signature: account_info.txn_signature.map(|v| v.as_ref().to_vec()),
                })
            }
        }
    }
}

impl From<&MessageHeader> for DbTransactionMessageHeader {
    fn from(header: &MessageHeader) -> Self {
        Self {
            num_required_signatures: header.num_required_signatures as i16,
            num_readonly_signed_accounts: header.num_readonly_signed_accounts as i16,
            num_readonly_unsigned_accounts: header.num_readonly_unsigned_accounts as i16,
        }
    }
}

impl From<&CompiledInstruction> for DbCompiledInstruction {
    fn from(instruction: &CompiledInstruction) -> Self {
        Self {
            program_id_index: instruction.program_id_index as i16,
            accounts: instruction
                .accounts
                .iter()
                .map(|account_idx| *account_idx as i16)
                .collect(),
            data: instruction.data.clone(),
        }
    }
}

impl From<&MessageAddressTableLookup> for DbTransactionMessageAddressTableLookup {
    fn from(address_table_lookup: &MessageAddressTableLookup) -> Self {
        Self {
            account_key: address_table_lookup.account_key.as_ref().to_vec(),
            writable_indexes: address_table_lookup
                .writable_indexes
                .iter()
                .map(|idx| *idx as i16)
                .collect(),
            readonly_indexes: address_table_lookup
                .readonly_indexes
                .iter()
                .map(|idx| *idx as i16)
                .collect(),
        }
    }
}

impl From<&v0::Message> for DbTransactionMessageV0 {
    fn from(message: &v0::Message) -> Self {
        Self {
            header: DbTransactionMessageHeader::from(&message.header),
            account_keys: message
                .account_keys
                .iter()
                .map(|key| key.as_ref().to_vec())
                .collect(),
            recent_blockhash: message.recent_blockhash.as_ref().to_vec(),
            instructions: message
                .instructions
                .iter()
                .map(DbCompiledInstruction::from)
                .collect(),
            address_table_lookups: message
                .address_table_lookups
                .iter()
                .map(DbTransactionMessageAddressTableLookup::from)
                .collect(),
        }
    }
}

impl From<&LoadedAddresses> for DbLoadedAddresses {
    fn from(loaded_addresses: &LoadedAddresses) -> Self {
        Self {
            writable: loaded_addresses
                .writable
                .iter()
                .map(|pubkey| pubkey.as_ref().to_vec())
                .collect(),
            readonly: loaded_addresses
                .readonly
                .iter()
                .map(|pubkey| pubkey.as_ref().to_vec())
                .collect(),
        }
    }
}

impl From<&KafkaLoadedMessage> for DbLoadedMessageV0 {
    fn from(message: &KafkaLoadedMessage) -> Self {
        Self {
            message: DbTransactionMessageV0::from(&message.message as &v0::Message),
            loaded_addresses: DbLoadedAddresses::from(
                &message.loaded_addresses as &LoadedAddresses,
            ),
        }
    }
}

impl From<&TransactionError> for DbTransactionErrorCode {
    fn from(err: &TransactionError) -> Self {
        match err {
            TransactionError::AccountInUse => Self::AccountInUse,
            TransactionError::AccountLoadedTwice => Self::AccountLoadedTwice,
            TransactionError::AccountNotFound => Self::AccountNotFound,
            TransactionError::ProgramAccountNotFound => Self::ProgramAccountNotFound,
            TransactionError::InsufficientFundsForFee => Self::InsufficientFundsForFee,
            TransactionError::InvalidAccountForFee => Self::InvalidAccountForFee,
            TransactionError::AlreadyProcessed => Self::AlreadyProcessed,
            TransactionError::BlockhashNotFound => Self::BlockhashNotFound,
            TransactionError::InstructionError(_idx, _error) => Self::InstructionError,
            TransactionError::CallChainTooDeep => Self::CallChainTooDeep,
            TransactionError::MissingSignatureForFee => Self::MissingSignatureForFee,
            TransactionError::InvalidAccountIndex => Self::InvalidAccountIndex,
            TransactionError::SignatureFailure => Self::SignatureFailure,
            TransactionError::InvalidProgramForExecution => Self::InvalidProgramForExecution,
            TransactionError::SanitizeFailure => Self::SanitizeFailure,
            TransactionError::ClusterMaintenance => Self::ClusterMaintenance,
            TransactionError::AccountBorrowOutstanding => Self::AccountBorrowOutstanding,
            TransactionError::WouldExceedMaxAccountCostLimit => {
                Self::WouldExceedMaxAccountCostLimit
            }
            TransactionError::WouldExceedMaxBlockCostLimit => Self::WouldExceedMaxBlockCostLimit,
            TransactionError::UnsupportedVersion => Self::UnsupportedVersion,
            TransactionError::InvalidWritableAccount => Self::InvalidWritableAccount,
            TransactionError::WouldExceedAccountDataBlockLimit => {
                Self::WouldExceedAccountDataBlockLimit
            }
            TransactionError::WouldExceedAccountDataTotalLimit => {
                Self::WouldExceedAccountDataTotalLimit
            }
            TransactionError::TooManyAccountLocks => Self::TooManyAccountLocks,
            TransactionError::AddressLookupTableNotFound => Self::AddressLookupTableNotFound,
            TransactionError::InvalidAddressLookupTableOwner => {
                Self::InvalidAddressLookupTableOwner
            }
            TransactionError::InvalidAddressLookupTableData => Self::InvalidAddressLookupTableData,
            TransactionError::InvalidAddressLookupTableIndex => {
                Self::InvalidAddressLookupTableIndex
            }
            TransactionError::InvalidRentPayingAccount => Self::InvalidRentPayingAccount,
            TransactionError::WouldExceedMaxVoteCostLimit => Self::WouldExceedMaxVoteCostLimit,
            TransactionError::DuplicateInstruction(_) => Self::DuplicateInstruction,
            TransactionError::InsufficientFundsForRent { account_index: _ } => {
                Self::InsufficientFundsForRent
            }
        }
    }
}

fn get_transaction_error(result: &Result<(), TransactionError>) -> Option<DbTransactionError> {
    if result.is_ok() {
        return None;
    }

    let error = result.as_ref().err().unwrap();
    Some(DbTransactionError {
        error_code: DbTransactionErrorCode::from(error),
        error_detail: {
            if let TransactionError::InstructionError(idx, instruction_error) = error {
                let mut error_detail =
                    format!("InstructionError: idx ({idx}), error: ({instruction_error})",);
                if error_detail.len() > MAX_TRANSACTION_STATUS_LEN {
                    error_detail = error_detail
                        .to_string()
                        .split_off(MAX_TRANSACTION_STATUS_LEN);
                }
                Some(error_detail)
            } else {
                None
            }
        },
    })
}

impl From<&InnerInstructions> for DbInnerInstructions {
    fn from(instructions: &InnerInstructions) -> Self {
        Self {
            index: instructions.index as i16,
            instructions: instructions
                .instructions
                .iter()
                .map(DbCompiledInstruction::from)
                .collect(),
        }
    }
}

impl From<&KafkaTransactionTokenBalance> for DbTransactionTokenBalance {
    fn from(token_balance: &KafkaTransactionTokenBalance) -> Self {
        Self {
            account_index: token_balance.account_index as i16,
            mint: token_balance.mint.clone(),
            ui_token_amount: token_balance.ui_token_amount.ui_amount,
            owner: token_balance.owner.clone(),
        }
    }
}

impl From<&KafkaTransactionStatusMeta> for DbTransactionStatusMeta {
    fn from(meta: &KafkaTransactionStatusMeta) -> Self {
        Self {
            error: get_transaction_error(&meta.status),
            fee: meta.fee as i64,
            pre_balances: meta
                .pre_balances
                .iter()
                .map(|balance| *balance as i64)
                .collect(),
            post_balances: meta
                .post_balances
                .iter()
                .map(|balance| *balance as i64)
                .collect(),
            inner_instructions: meta
                .inner_instructions
                .as_ref()
                .map(|instructions| instructions.iter().map(DbInnerInstructions::from).collect()),
            log_messages: meta.log_messages.clone(),
            pre_token_balances: meta.pre_token_balances.as_ref().map(|balances| {
                balances
                    .iter()
                    .map(DbTransactionTokenBalance::from)
                    .collect()
            }),
            post_token_balances: meta.post_token_balances.as_ref().map(|balances| {
                balances
                    .iter()
                    .map(DbTransactionTokenBalance::from)
                    .collect()
            }),
            rewards: meta
                .rewards
                .as_ref()
                .map(|rewards| rewards.iter().map(DbReward::from).collect()),
        }
    }
}

impl From<&KafkaLegacyMessage> for DbTransactionMessage {
    fn from(message: &KafkaLegacyMessage) -> Self {
        Self {
            header: DbTransactionMessageHeader::from(&message.message.header),
            account_keys: message
                .message
                .account_keys
                .iter()
                .map(|key| key.as_ref().to_vec())
                .collect(),
            recent_blockhash: message.message.recent_blockhash.as_ref().to_vec(),
            instructions: message
                .message
                .instructions
                .iter()
                .map(DbCompiledInstruction::from)
                .collect(),
        }
    }
}

fn get_message_hash(sanitized_message: &KafkaSanitizedMessage) -> Vec<u8> {
    match sanitized_message {
        KafkaSanitizedMessage::Legacy(m) => m.message.hash().to_bytes().to_vec(),
        KafkaSanitizedMessage::V0(_) => vec![],
    }
}

fn get_db_legacy_message(
    sanitized_message: &KafkaSanitizedMessage,
) -> Option<DbTransactionMessage> {
    match sanitized_message {
        KafkaSanitizedMessage::Legacy(message) => Some(message.into()),
        KafkaSanitizedMessage::V0(_) => None,
    }
}

impl From<NotifyTransaction> for DbTransaction {
    fn from(transaction: NotifyTransaction) -> Self {
        match transaction.transaction_info {
            kafka_common::kafka_structs::KafkaReplicaTransactionInfoVersions::V0_0_1(t) => {
                let message = &t.transaction.message;
                let message_type = match t.transaction.message {
                    KafkaSanitizedMessage::Legacy(_) => 0,
                    KafkaSanitizedMessage::V0(_) => 1,
                };
                let v0_loaded_message = match &message {
                    KafkaSanitizedMessage::V0(loaded_message) => {
                        Some(DbLoadedMessageV0::from(loaded_message))
                    }
                    _ => None,
                };
                let message_hash = get_message_hash(message);
                let meta = DbTransactionStatusMeta::from(&t.transaction_status_meta);
                let signature = t.signature.as_ref().to_vec();
                let signatures = t
                    .transaction
                    .signatures
                    .iter()
                    .map(|signature| signature.as_ref().to_vec())
                    .collect();
                let index = 0;
                let is_vote = t.is_vote;
                let slot = transaction.slot;

                DbTransaction {
                    signature,
                    is_vote,
                    slot: slot as i64,
                    message_type: message_type as i16,
                    legacy_message: get_db_legacy_message(message),
                    v0_loaded_message,
                    message_hash,
                    meta,
                    signatures,
                    index,
                }
            }
            kafka_common::kafka_structs::KafkaReplicaTransactionInfoVersions::V0_0_2(t) => {
                let message = &t.transaction.message;
                let message_type = match t.transaction.message {
                    KafkaSanitizedMessage::Legacy(_) => 0,
                    KafkaSanitizedMessage::V0(_) => 1,
                };
                let v0_loaded_message = match &message {
                    KafkaSanitizedMessage::V0(loaded_message) => {
                        Some(DbLoadedMessageV0::from(loaded_message))
                    }
                    _ => None,
                };
                let message_hash = get_message_hash(message);
                let meta = DbTransactionStatusMeta::from(&t.transaction_status_meta);
                let signature = t.signature.as_ref().to_vec();
                let signatures = t
                    .transaction
                    .signatures
                    .iter()
                    .map(|signature| signature.as_ref().to_vec())
                    .collect();
                let index = t.index;
                let is_vote = t.is_vote;
                let slot = transaction.slot;

                DbTransaction {
                    signature,
                    is_vote,
                    slot: slot as i64,
                    message_type: message_type as i16,
                    legacy_message: get_db_legacy_message(message),
                    v0_loaded_message,
                    message_hash,
                    meta,
                    signatures,
                    index: index as i64,
                }
            }
        }
    }
}

impl From<RewardType> for DbRewardType {
    fn from(reward_type: RewardType) -> Self {
        match reward_type {
            RewardType::Fee => Self::Fee,
            RewardType::Rent => Self::Rent,
            RewardType::Staking => Self::Staking,
            RewardType::Voting => Self::Voting,
        }
    }
}

impl From<&Reward> for DbReward {
    fn from(reward: &Reward) -> Self {
        DbReward {
            pubkey: reward.pubkey.clone(),
            lamports: reward.lamports,
            post_balance: reward.post_balance as i64,
            reward_type: reward.reward_type.map(|v| v.into()),
            commission: reward.commission.map(|v| v as i16),
        }
    }
}

impl From<KafkaReplicaBlockInfo> for DbBlockInfo {
    fn from(block_info: KafkaReplicaBlockInfo) -> Self {
        Self {
            slot: block_info.slot as i64,
            blockhash: block_info.blockhash.to_string(),
            rewards: block_info.rewards.iter().map(DbReward::from).collect(),
            block_time: block_info.block_time,
            block_height: block_info
                .block_height
                .map(|block_height| block_height as i64),
        }
    }
}
