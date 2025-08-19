#![allow(clippy::needless_borrow)]
use crate::store::SQLXPool;

use ckb_indexer_sync::Error;
use ckb_types::{
    H256,
    core::{BlockExt, BlockView},
    packed::{CellbaseWitnessReader, OutPoint},
    prelude::*,
};
use sql_builder::SqlBuilder;
use sqlx::{
    Row, Transaction,
    any::{Any, AnyArguments},
    query::Query,
};

const BATCH_SIZE_THRESHOLD: usize = 1_000;

enum FieldValue {
    Binary(Vec<u8>),
    BigInt(i64),
    Int(i32),
    NoneBigInt,
    SmallInt(i16),
}

impl FieldValue {
    fn bind<'a>(
        &'a self,
        query: Query<'a, Any, AnyArguments<'a>>,
    ) -> Query<'a, Any, AnyArguments<'a>> {
        match self {
            FieldValue::Binary(value) => query.bind(value),
            FieldValue::BigInt(value) => query.bind(value),
            FieldValue::Int(value) => query.bind(value),
            FieldValue::NoneBigInt => query.bind(Option::<i64>::None),
            FieldValue::SmallInt(value) => query.bind(value),
        }
    }
}

impl From<Vec<u8>> for FieldValue {
    fn from(value: Vec<u8>) -> Self {
        FieldValue::Binary(value)
    }
}

impl From<i64> for FieldValue {
    fn from(value: i64) -> Self {
        FieldValue::BigInt(value)
    }
}

impl From<usize> for FieldValue {
    fn from(value: usize) -> Self {
        FieldValue::BigInt(value as i64)
    }
}

impl From<u64> for FieldValue {
    fn from(value: u64) -> Self {
        FieldValue::BigInt(value as i64)
    }
}

impl From<i32> for FieldValue {
    fn from(value: i32) -> Self {
        FieldValue::Int(value)
    }
}

impl From<i16> for FieldValue {
    fn from(value: i16) -> Self {
        FieldValue::SmallInt(value)
    }
}

pub async fn init_block(
    init_tip_number: u64,
    init_tip_hash: &H256,
    tx: &mut Transaction<'_, Any>,
) -> Result<(), Error> {
    bulk_insert_and_return_ids(
        "block",
        &["hash", "number"],
        &[vec![
            init_tip_hash.as_bytes().to_vec().into(),
            init_tip_number.into(),
        ]],
        tx,
    )
    .await
    .map_err(|err| Error::DB(err.to_string()))
    .map(|_| ())
}

async fn spend_cell(
    output_id: i64,
    tx_hash: &[u8],
    input_index: usize,
    tx: &mut Transaction<'_, Any>,
) -> Result<bool, Error> {
    let updated_rows = sqlx::query(
        r#"
            UPDATE output
            SET is_spent = 1,
                consumed_tx_hash = $2,
                input_index = $3
            WHERE
                id = $1
        "#,
    )
    .bind(output_id)
    .bind(tx_hash)
    .bind(input_index as i32)
    .execute(tx.as_mut())
    .await
    .map_err(|err| Error::DB(err.to_string()))?
    .rows_affected();

    Ok(updated_rows > 0)
}

fn build_bulk_insert_sql(
    table: &str,
    fields: &[&str],
    bulk: &[Vec<FieldValue>],
) -> Result<String, Error> {
    let mut builder = SqlBuilder::insert_into(table);
    builder.fields(fields);
    bulk.iter().enumerate().for_each(|(row_index, row)| {
        let placeholders = (1..=row.len())
            .map(|i| format!("${}", i + row_index * row.len()))
            .collect::<Vec<String>>();
        builder.values(&placeholders);
    });
    let sql = builder
        .sql()
        .map_err(|err| Error::DB(err.to_string()))?
        .trim_end_matches(';')
        .to_string();
    Ok(sql)
}

async fn bulk_insert_and_return_ids(
    table: &str,
    fields: &[&str],
    rows: &[Vec<FieldValue>],
    tx: &mut Transaction<'_, Any>,
) -> Result<Vec<i64>, Error> {
    let mut id_list = Vec::new();
    for bulk in rows.chunks(BATCH_SIZE_THRESHOLD) {
        // build query str
        let sql = build_bulk_insert_sql(table, fields, bulk)?;
        let sql = format!("{} RETURNING id", sql);

        // bind
        let mut query = SQLXPool::new_query(&sql);
        for row in bulk {
            for field in row {
                query = field.bind(query);
            }
        }

        // execute
        let mut rows = query
            .fetch_all(tx.as_mut())
            .await
            .map_err(|err| Error::DB(err.to_string()))?;
        id_list.append(&mut rows);
    }
    let ret: Vec<_> = id_list.iter().map(|row| row.get::<i64, _>("id")).collect();
    Ok(ret)
}

// query function
// query output_id
pub(crate) async fn query_output_id(
    out_point: &OutPoint,
    tx: &mut Transaction<'_, Any>,
) -> Result<Option<i64>, Error> {
    let output_tx_hash = out_point.tx_hash().raw_data().to_vec();
    let output_index: u32 = out_point.index().unpack();

    sqlx::query(
        r#"
        SELECT output.id
        FROM
            output
        WHERE
            output.tx_hash = $1
            AND output.output_index = $2
        "#,
    )
    .bind(output_tx_hash)
    .bind(output_index as i32)
    .fetch_optional(tx.as_mut())
    .await
    .map_err(|err| Error::DB(err.to_string()))
    .map(|row| row.map(|row| row.get::<i64, _>("id")))
}

pub(crate) async fn query_block_id(
    block_hash: &[u8],
    tx: &mut Transaction<'_, Any>,
) -> Result<Option<i64>, Error> {
    sqlx::query(
        r#"
        SELECT id
        FROM
            block
        WHERE
            block_hash = $1
        "#,
    )
    .bind(block_hash)
    .fetch_optional(tx.as_mut())
    .await
    .map_err(|err| Error::DB(err.to_string()))
    .map(|row| row.map(|row| row.get::<i64, _>("id")))
}

pub(crate) async fn query_script_id(
    script_hash: &[u8],
    tx: &mut Transaction<'_, Any>,
) -> Result<Option<i64>, Error> {
    sqlx::query(
        r#"
        SELECT id
        FROM
            script
        WHERE
            script_hash = $1
        "#,
    )
    .bind(script_hash)
    .fetch_optional(tx.as_mut())
    .await
    .map_err(|err| Error::DB(err.to_string()))
    .map(|row| row.map(|row| row.get::<i64, _>("id")))
}

pub(crate) async fn update_block(
    block_view: &BlockView,
    block_ext: &BlockExt,
    db_tx: &mut Transaction<'_, Any>,
) -> Result<(), Error> {
    // prepare block
    let block_hash = block_view.hash().raw_data().to_vec();
    let block_number = block_view.number();
    let compact_target = block_view.compact_target().to_be_bytes().to_vec();
    let parent_hash = block_view.parent_hash().raw_data().to_vec();
    let nonce = block_view.nonce().to_be_bytes().to_vec();
    let difficulty = block_view.difficulty().to_be_bytes().to_vec();
    let timestamp = block_view.timestamp();
    let version = block_view.version().to_be_bytes().to_vec();
    let transactions_root = block_view.transactions_root().raw_data().to_vec();
    let transactions_count = block_view.transactions().len();

    // process epoch
    let epoch = block_view.epoch().full_value().to_be_bytes().to_vec();
    let epoch_unpack = block_view.epoch();
    let epoch_number = epoch_unpack.number();
    let epoch_index = epoch_unpack.index();
    let start_number = block_number - epoch_index;
    let epoch_length = epoch_unpack.length();

    let dao = block_view.dao().raw_data().to_vec();
    let proposals_hash = block_view.proposals_hash().raw_data().to_vec();
    let extra_hash = block_view.extra_hash().raw_data().to_vec();
    let extension = match block_view.data().extension() {
        Some(extension) => extension.raw_data().to_vec(),
        None => Vec::new(),
    };

    // process proposals
    let proposals_unpack = block_view.data().proposals();
    let proposals = proposals_unpack.as_bytes().to_vec();
    let proposals_count = proposals_unpack.len();

    let uncles_count = block_view.data().uncles().len();
    let uncle_block_hashes = block_view.uncle_hashes().as_bytes().to_vec();

    // process cellbase
    let cellbase_tx = &block_view.transactions()[0];
    // witness must be exist event 1-11 blocks
    let cellbase_witness = cellbase_tx.witnesses().get(0).unwrap();
    let cellbase_witness_raw_data = cellbase_witness.raw_data().to_vec();
    let cellbase_witness_reader =
        CellbaseWitnessReader::from_slice(&cellbase_witness_raw_data).unwrap();
    let miner_script = cellbase_witness_reader
        .lock()
        .to_entity()
        .as_bytes()
        .to_vec();
    let miner_message = cellbase_witness_reader
        .message()
        .as_utf8()
        .unwrap_or("")
        .as_bytes()
        .to_vec();
    // 1-11 blocks has no cellbase output
    let reward = if !cellbase_tx.outputs().is_empty() {
        cellbase_tx.outputs().get(0).unwrap().capacity().unpack()
    } else {
        0
    };

    // get from block ext
    let mut total_transaction_fee: u64 = 0;
    for tx_fee in &block_ext.txs_fees {
        total_transaction_fee += tx_fee.as_u64();
    }

    // cell_consumed means total output occupied capacity
    let mut cell_consumed = 0;
    for tx in block_view.transactions() {
        cell_consumed += tx.outputs().total_size();
        cell_consumed += tx.outputs_data().total_size();
    }

    // total_cell_capacity means total output capacity
    let mut total_cell_capacity = 0;
    for tx in block_view.transactions() {
        for output in tx.outputs() {
            let capacity: u64 = output.capacity().unpack();
            total_cell_capacity += capacity;
        }
    }

    // input - output = live cell changes
    let mut live_cell_changes = 0;
    for tx in block_view.transactions() {
        let input_count = tx.inputs().len();
        let output_count = tx.outputs().len();
        live_cell_changes += input_count as i64 - output_count as i64;
    }

    let block_size = block_view.data().total_size();
    // get from block ext
    let mut cycles_sum: u64 = 0;
    if let Some(cycles) = &block_ext.cycles {
        for cycle in cycles {
            cycles_sum += cycle;
        }
    }

    // insert to uncle_block table with transaction tx
    let block_id = bulk_insert_and_return_ids(
        "block",
        &[
            "block_hash",
            "block_number",
            "compact_target",
            "parent_hash",
            "nonce",
            "difficulty",
            "timestamp",
            "version",
            "transactions_root",
            "transactions_count",
            "epoch",
            "start_number",
            "epoch_length",
            "epoch_number",
            "dao",
            "proposals_hash",
            "extra_hash",
            "extension",
            "proposals",
            "proposals_count",
            "uncles_count",
            "uncle_block_hashes",
            "miner_script",
            "miner_message",
            "reward",
            "total_transaction_fee",
            "cell_consumed",
            "total_cell_capacity",
            "block_size",
            "cycles",
            "live_cell_changes",
        ],
        &[vec![
            block_hash.clone().into(),
            block_number.into(),
            compact_target.into(),
            parent_hash.into(),
            nonce.into(),
            difficulty.into(),
            timestamp.into(),
            version.into(),
            transactions_root.into(),
            transactions_count.into(),
            epoch.into(),
            start_number.into(),
            epoch_length.into(),
            epoch_number.into(),
            dao.into(),
            proposals_hash.into(),
            extra_hash.into(),
            extension.into(),
            proposals.into(),
            proposals_count.into(),
            uncles_count.into(),
            uncle_block_hashes.into(),
            miner_script.into(),
            miner_message.into(),
            reward.into(),
            total_transaction_fee.into(),
            cell_consumed.into(),
            total_cell_capacity.into(),
            block_size.into(),
            cycles_sum.into(),
            live_cell_changes.into(),
        ]],
        db_tx,
    )
    .await?[0];

    // process uncles
    let uncle_blocks = block_view
        .uncles()
        .into_iter()
        .map(|uncle| {
            let uncle_block_header = uncle.header();
            BlockView::new_advanced_builder()
                .header(uncle_block_header)
                .proposals(uncle.data().proposals())
                .build()
        })
        .collect::<Vec<_>>();

    let mut uncle_block_values = Vec::new();

    for (uncle_block_index, uncle_block) in uncle_blocks.iter().enumerate() {
        let uncle_block_hash = uncle_block.hash().raw_data().to_vec();
        let uncle_block_number = uncle_block.number();
        let uncle_block_compact_target = uncle_block.compact_target().to_be_bytes().to_vec();
        let uncle_block_parent_hash = uncle_block.parent_hash().raw_data().to_vec();
        let uncle_block_nonce = uncle_block.nonce().to_be_bytes().to_vec();
        let uncle_block_timestamp = uncle_block.timestamp();
        let uncle_block_version = uncle_block.version().to_be_bytes().to_vec();
        let uncle_block_transactions_root = uncle_block.transactions_root().raw_data().to_vec();
        let uncle_block_epoch = uncle_block.epoch().full_value().to_be_bytes().to_vec();
        let uncle_block_dao = uncle_block.dao().raw_data().to_vec();
        let uncle_block_proposals_hash = uncle_block.proposals_hash().raw_data().to_vec();
        let uncle_block_extra_hash = uncle_block.extra_hash().raw_data().to_vec();
        let uncle_block_extension = match uncle_block.data().extension() {
            Some(extension) => extension.raw_data().to_vec(),
            None => Vec::new(),
        };
        let uncle_block_proposals = uncle_block.data().proposals().as_bytes().to_vec();

        uncle_block_values.push(vec![
            uncle_block_index.into(),
            uncle_block_hash.into(),
            uncle_block_number.into(),
            uncle_block_compact_target.into(),
            uncle_block_parent_hash.into(),
            uncle_block_nonce.into(),
            uncle_block_timestamp.into(),
            uncle_block_version.into(),
            uncle_block_transactions_root.into(),
            uncle_block_epoch.into(),
            uncle_block_dao.into(),
            uncle_block_proposals_hash.into(),
            uncle_block_extra_hash.into(),
            uncle_block_extension.into(),
            uncle_block_proposals.into(),
        ]);
    }
    // insert to uncle_block table with transaction tx
    let _ = bulk_insert_and_return_ids(
        "uncle_block",
        &[
            "index",
            "block_hash",
            "block_number",
            "compact_target",
            "parent_hash",
            "nonce",
            "timestamp",
            "version",
            "transactions_root",
            "epoch",
            "dao",
            "proposals_hash",
            "extra_hash",
            "extension",
            "proposals",
        ],
        &uncle_block_values,
        db_tx,
    )
    .await;

    // process transactions
    for (tx_index, tx_view) in block_view.transactions().iter().enumerate() {
        let tx_hash = tx_view.hash().raw_data().to_vec();
        let tx_version = tx_view.version().to_be_bytes().to_vec();
        let tx_inputs_count = tx_view.inputs().len() as i32;
        let tx_outputs_count = tx_view.outputs().len() as i32;
        let tx_witnesses = tx_view.witnesses().as_bytes().to_vec();
        // block_id use outside
        // block_number use outside
        // block_hash use outside
        // tx_index use loop var
        let tx_header_deps = tx_view.header_deps().as_bytes().to_vec();
        // get cycles of tx from block ext
        // block_ext.cycles  except the cellbase tx
        // see util/types/src/core/extras.rs
        let cycles = if tx_index == 0 {
            0
        } else {
            block_ext
                .cycles
                .as_ref()
                .map_or(0, |cycles| cycles[tx_index - 1])
        };
        // get fee of tx from block ext
        // block_ext.txs_fees  except the cellbase tx
        // see util/types/src/core/extras.rs
        let transaction_fee = if tx_index == 0 {
            0
        } else {
            block_ext.txs_fees[tx_index - 1].as_u64()
        };
        // get tx_size from block ext has some bug
        let bytes = tx_view.data().total_size();

        // insert transaction
        let tx_id = bulk_insert_and_return_ids(
            "transaction",
            &[
                "tx_hash",
                "version",
                "input_count",
                "output_count",
                "witnesses",
                "block_id",
                "block_number",
                "block_hash",
                "tx_index",
                "header_deps",
                "cycles",
                "transaction_fee",
                "bytes",
            ],
            &[vec![
                tx_hash.clone().into(),
                tx_version.into(),
                tx_inputs_count.into(),
                tx_outputs_count.into(),
                tx_witnesses.into(),
                block_id.into(),
                block_number.into(),
                block_hash.clone().into(),
                tx_index.into(),
                tx_header_deps.into(),
                cycles.into(),
                transaction_fee.into(),
                bytes.into(),
            ]],
            db_tx,
        )
        .await?[0];

        // process header deps
        let mut tx_association_header_dep_rows = Vec::new();
        for header_dep in tx_view.header_deps_iter() {
            if let Some(block_id) = query_block_id(&header_dep.raw_data(), db_tx).await? {
                tx_association_header_dep_rows.push(vec![tx_id.into(), block_id.into()]);
            }
        }
        let _ = bulk_insert_and_return_ids(
            "tx_association_header_dep",
            &["tx_id", "block_id"],
            &tx_association_header_dep_rows,
            db_tx,
        )
        .await?;

        // process cell deps
        let mut tx_association_cell_dep_rows = Vec::new();
        for (cell_dep_index, cell_dep) in tx_view.cell_deps_iter().enumerate() {
            if let Some(output_id) = query_output_id(&cell_dep.out_point(), db_tx).await? {
                let outpoint_tx_hash = cell_dep.out_point().tx_hash().raw_data().to_vec();
                let outpoint_index: u32 = cell_dep.out_point().index().unpack();
                tx_association_cell_dep_rows.push(vec![
                    tx_id.into(),
                    cell_dep_index.into(),
                    outpoint_tx_hash.into(),
                    (outpoint_index as i32).into(),
                    output_id.into(),
                    (u8::from(cell_dep.dep_type()) as i16).into(),
                ]);
            }
        }
        let _ = bulk_insert_and_return_ids(
            "tx_association_cell_dep",
            &[
                "tx_id",
                "index",
                "outpoint_tx_hash",
                "outpoint_index",
                "output_id",
                "dep_type",
            ],
            &tx_association_cell_dep_rows,
            db_tx,
        )
        .await?;

        // process inputs
        for (input_index, input) in tx_view.inputs().into_iter().enumerate() {
            // skip input of cellbase tx
            if tx_index == 0 {
                continue;
            }
            if let Some(output_id) = query_output_id(&input.previous_output(), db_tx).await? {
                if !spend_cell(output_id, &tx_hash, input_index, db_tx).await? {
                    return Err(Error::DB("spend cell failed".to_string()));
                }
                let pre_outpoint_tx_hash = input.previous_output().tx_hash().raw_data().to_vec();
                let pre_outpoint_index: u32 = input.previous_output().index().unpack();
                let since = input.since().raw_data().to_vec();
                let consumed_tx_id = tx_id;
                let consumed_tx_hash = tx_hash.clone();
                let input_index = input_index as i32;

                // insert to input table
                let _ = bulk_insert_and_return_ids(
                    "input",
                    &[
                        "output_id",
                        "pre_outpoint_tx_hash",
                        "pre_outpoint_index",
                        "since",
                        "consumed_tx_id",
                        "consumed_tx_hash",
                        "input_index",
                    ],
                    &[vec![
                        output_id.into(),
                        pre_outpoint_tx_hash.into(),
                        (pre_outpoint_index as i32).into(),
                        since.into(),
                        consumed_tx_id.into(),
                        consumed_tx_hash.into(),
                        input_index.into(),
                    ]],
                    db_tx,
                )
                .await?;
            }
        }

        // process outputs
        for (output_index, output) in tx_view.outputs().into_iter().enumerate() {
            let output_capacity: u64 = output.capacity().unpack();

            // lock script
            let lock_script = output.lock();
            let lock_script_hash = lock_script.calc_script_hash().raw_data().to_vec();
            let lock_script_id = if let Some(id) = query_script_id(&lock_script_hash, db_tx).await?
            {
                id
            } else {
                bulk_insert_and_return_ids(
                    "script",
                    &["code_hash", "hash_type", "args", "script_hash"],
                    &[vec![
                        lock_script.code_hash().raw_data().to_vec().into(),
                        (u8::from(lock_script.hash_type()) as i16).into(),
                        lock_script.args().raw_data().to_vec().into(),
                        lock_script_hash.into(),
                    ]],
                    db_tx,
                )
                .await?[0]
            };

            // type script
            let output_type = output.type_().to_opt();
            let mut type_script_id = None;
            if let Some(output_type) = output_type {
                let output_type_hash = output_type.calc_script_hash().raw_data().to_vec();
                type_script_id = if let Some(id) = query_script_id(&output_type_hash, db_tx).await?
                {
                    Some(id)
                } else {
                    let id = bulk_insert_and_return_ids(
                        "script",
                        &["code_hash", "hash_type", "args", "script_hash"],
                        &[vec![
                            output_type.code_hash().raw_data().to_vec().into(),
                            (u8::from(output_type.hash_type()) as i16).into(),
                            output_type.args().raw_data().to_vec().into(),
                            output_type_hash.into(),
                        ]],
                        db_tx,
                    )
                    .await?[0];
                    Some(id)
                };
            };

            // data
            let output_data = tx_view
                .outputs_data()
                .get(output_index)
                .map(|data| data.raw_data().to_vec())
                .unwrap_or_default();
            let occupied_capacity = output.total_size() + output_data.len();

            // update when output is spent
            let is_spent = 0;
            let consumed_tx_hash = Vec::new();
            let input_index = -1;

            let _ = bulk_insert_and_return_ids(
                "output",
                &[
                    "tx_id",
                    "tx_hash",
                    "output_index",
                    "capacity",
                    "lock_script_id",
                    "type_script_id",
                    "data",
                    "occupied_capacity",
                    "is_spent",
                    "consumed_tx_hash",
                    "input_index",
                ],
                &[vec![
                    tx_id.into(),
                    tx_hash.clone().into(),
                    output_index.into(),
                    output_capacity.into(),
                    lock_script_id.into(),
                    type_script_id.map_or(FieldValue::NoneBigInt, FieldValue::BigInt),
                    output_data.into(),
                    occupied_capacity.into(),
                    is_spent.into(),
                    consumed_tx_hash.into(),
                    input_index.into(),
                ]],
                db_tx,
            )
            .await?;
        }
    }

    Ok(())
}
