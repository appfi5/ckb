//! An overlay to index the pending txs in the ckb tx pool

use ckb_async_runtime::{
    Handle,
    tokio::{self, task::JoinHandle},
};
use ckb_logger::info;
use ckb_notify::NotifyController;
use ckb_stop_handler::{CancellationToken, new_tokio_exit_rx};
use ckb_types::{core::TransactionView, packed::OutPoint};

use std::collections::HashSet;
use std::sync::{Arc, RwLock};

const SUBSCRIBER_NAME: &str = "Indexer";

/// An overlay to index the pending txs in the ckb tx pool,
/// currently only supports removals of dead cells from the pending txs
pub struct Pool {
    pub dead_cells: Arc<RwLock<HashSet<OutPoint>>>,
    pub store: PoolSQLXPool,
}

impl Pool {
    pub fn new(async_handle: Handle) -> Self {
        let mut store = PoolSQLXPool::default();
        async_handle.block_on(async {
            let ret = store.connect().await;
            if let Err(e) = ret {
                panic!("connect db failed: {}", e);
            }
        });

        Self {
            dead_cells: Arc::new(RwLock::new(HashSet::new())),
            store,
        }
    }
    /// the tx has been committed in a block, it should be removed from pending dead cells
    pub async fn transaction_committed(&self, tx: &TransactionView) {
        for input in tx.inputs() {
            self.dead_cells
                .write()
                .unwrap()
                .remove(&input.previous_output());
        }

        let tx_hash = tx.hash().raw_data().to_vec();
        log::trace!("transaction_committed: {}", hex::encode(&tx_hash));

        let pending_tx_data = {
            if let Ok(pool) = self.store.get_pg_pool() {
                query_pending_tx(&tx_hash, pool).await.ok().flatten()
            } else {
                None
            }
        };

        if let Some((
            version,
            input_count,
            output_count,
            witnesses,
            header_deps,
            bytes,
            created_at,
        )) = pending_tx_data
        {
            if let Ok(pool) = self.store.get_pg_pool() {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64;
                if let Err(e) = bulk_insert(
                    "ckb_transaction",
                    &[
                        "tx_hash",
                        "version",
                        "input_count",
                        "output_count",
                        "witnesses",
                        "header_deps",
                        "bytes",
                        "status",
                        "created_at",
                        "updated_at",
                    ],
                    &[vec![
                        tx_hash.clone().into(),
                        version.into(),
                        input_count.into(),
                        output_count.into(),
                        witnesses.into(),
                        header_deps.into(),
                        bytes.into(),
                        1.into(), // status: committed
                        created_at.into(),
                        now.into(),
                    ]],
                    pool,
                )
                .await
                {
                    log::error!("update committed tx failed: {:?}", e);
                }
            } else {
                log::error!("get pg pool failed");
            }
        } else {
            log::trace!("query pending tx failed: {}", hex::encode(&tx_hash));
        }
    }

    /// the tx has been rejected for some reason, it should be removed from pending dead cells
    pub async fn transaction_rejected(&self, tx: &TransactionView) {
        for input in tx.inputs() {
            self.dead_cells
                .write()
                .unwrap()
                .remove(&input.previous_output());
        }
        let tx_hash = tx.hash().raw_data().to_vec();
        log::trace!("transaction_rejected: {}", hex::encode(&tx_hash));

        let pending_tx_data = {
            if let Ok(pool) = self.store.get_pg_pool() {
                query_pending_tx(&tx_hash, pool).await.ok().flatten()
            } else {
                None
            }
        };

        if let Some((
            version,
            input_count,
            output_count,
            witnesses,
            header_deps,
            bytes,
            created_at,
        )) = pending_tx_data
        {
            if let Ok(pool) = self.store.get_pg_pool() {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64;
                if let Err(e) = bulk_insert(
                    "ckb_transaction",
                    &[
                        "tx_hash",
                        "version",
                        "input_count",
                        "output_count",
                        "witnesses",
                        "header_deps",
                        "bytes",
                        "status",
                        "created_at",
                        "updated_at",
                    ],
                    &[vec![
                        tx_hash.clone().into(),
                        version.into(),
                        input_count.into(),
                        output_count.into(),
                        witnesses.into(),
                        header_deps.into(),
                        bytes.into(),
                        2.into(), // status: rejected
                        created_at.into(),
                        now.into(),
                    ]],
                    pool,
                )
                .await
                {
                    log::error!("update rejected tx failed: {:?}", e);
                }
            } else {
                log::error!("get pg pool failed");
            }
        } else {
            log::trace!("query pending tx failed: {}", hex::encode(&tx_hash));
        }
    }

    /// a new tx is submitted to the pool, mark its inputs as dead cells
    pub async fn new_transaction(&self, tx: &TransactionView) {
        for input in tx.inputs() {
            self.dead_cells
                .write()
                .unwrap()
                .insert(input.previous_output());
        }
        let tx_hash = tx.hash().raw_data().to_vec();
        log::trace!("new_transaction: {}", hex::encode(&tx_hash));

        if let Ok(pool) = self.store.get_pg_pool() {
            if let Err(e) = insert_pool_pending_tx(tx, pool).await {
                log::error!("insert_pool_pending_tx failed: {:?}", e);
            }
        } else {
            log::error!("get pg pool failed");
        }
    }

    /// Return weather out_point referred cell consumed by pooled transaction
    pub fn is_consumed_by_pool_tx(&self, out_point: &OutPoint) -> bool {
        self.dead_cells.read().unwrap().contains(out_point)
    }

    /// the txs has been committed in a block, it should be removed from pending dead cells
    pub async fn transactions_committed(&self, txs: &[TransactionView]) {
        for tx in txs {
            self.transaction_committed(tx).await;
        }
    }

    /// return all dead cells
    pub fn dead_cells(&self) -> HashSet<OutPoint> {
        self.dead_cells.read().unwrap().clone()
    }
}

/// Pool service
#[derive(Clone)]
pub struct PoolService {
    pool: Option<Arc<Pool>>,
    async_handle: Handle,
    is_index_tx_pool_called: bool,
}

impl PoolService {
    /// Construct new Pool service instance
    pub fn new(index_tx_pool: bool, async_handle: Handle) -> Self {
        let pool = if index_tx_pool {
            Some(Arc::new(Pool::new(async_handle.clone())))
        } else {
            None
        };

        Self {
            pool,
            async_handle,
            is_index_tx_pool_called: false,
        }
    }

    /// Get the inner pool
    pub fn pool(&self) -> Option<Arc<Pool>> {
        self.pool.clone()
    }

    /// Processes that handle index pool transaction and expect to be spawned to run in tokio runtime
    pub fn index_tx_pool(
        &mut self,
        notify_controller: NotifyController,
        check_index_tx_pool_ready: JoinHandle<()>,
    ) {
        if self.is_index_tx_pool_called {
            return;
        }
        self.is_index_tx_pool_called = true;

        let service = self.clone();
        let stop: CancellationToken = new_tokio_exit_rx();

        self.async_handle.spawn(async move {
            let _check_index_tx_pool_ready = check_index_tx_pool_ready.await;
            if stop.is_cancelled() {
                info!(
                    "Indexer received exit signal, cancel subscribe_new_transaction task, exit now"
                );
                return;
            }

            info!("check_index_tx_pool_ready finished");

            let mut new_transaction_receiver = notify_controller
                .subscribe_new_transaction(SUBSCRIBER_NAME.to_string())
                .await;
            let mut reject_transaction_receiver = notify_controller
                .subscribe_reject_transaction(SUBSCRIBER_NAME.to_string())
                .await;

            loop {
                tokio::select! {
                    Some(tx_entry) = new_transaction_receiver.recv() => {
                        if let Some(pool) = service.pool.as_ref() {
                            pool.new_transaction(&tx_entry.transaction).await;
                        }
                    }
                    Some((tx_entry, _reject)) = reject_transaction_receiver.recv() => {
                        if let Some(pool) = service.pool.as_ref() {
                            pool.transaction_rejected(&tx_entry.transaction).await;
                        }
                    }
                    _ = stop.cancelled() => {
                        info!("index_tx_pool received exit signal, exit now");
                        break
                    },
                    else => break,
                }
            }
        });
    }
}

// db client for pool
use anyhow::{Result, anyhow};
use log::LevelFilter;
use sqlx::{
    AnyPool, ConnectOptions, Row, Transaction,
    any::{Any, AnyArguments, AnyConnectOptions, AnyPoolOptions},
    query::Query,
};
use std::str::FromStr;
use std::sync::OnceLock;
use std::{fmt::Debug, time::Duration};

const POOL_DB_URI_ENV: &str = "POOL_POSTGRES_URL";
const SQL_POSTGRES_CREATE_TABLE: &str = include_str!("../resources/create_postgres_table.sql");
const SQL_POSTGRES_CREATE_INDEX: &str = include_str!("../resources/create_postgres_index.sql");

#[derive(Clone, Default)]
pub struct PoolSQLXPool {
    risingwave_pool: Arc<OnceLock<AnyPool>>,
    db_uri: String,
}

impl Debug for PoolSQLXPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PoolSQLXPool")
            .field("db_uri", &self.db_uri)
            .finish()
    }
}

impl PoolSQLXPool {
    pub async fn connect(&mut self) -> Result<()> {
        // if not init, it will panic, see doc for more
        sqlx::any::install_default_drivers();
        let pool_options = AnyPoolOptions::new()
            .max_connections(10)
            .min_connections(0)
            .acquire_timeout(Duration::from_secs(60))
            .max_lifetime(Duration::from_secs(1800))
            .idle_timeout(Duration::from_secs(30));

        let pool = {
            let uri = std::env::var(POOL_DB_URI_ENV)?;
            let connection_options =
                AnyConnectOptions::from_str(&uri)?.log_statements(LevelFilter::Trace);
            let pool = pool_options.connect_with(connection_options).await?;
            log::info!("PostgreSQL is connected.");
            self.risingwave_pool
                .set(pool.clone())
                .map_err(|_| anyhow!("set pool failed"))?;

            self.create_tables_for_postgres().await?;

            self.db_uri = uri;
            pool
        };

        // Run test
        let query = "SELECT count(*) as count FROM ckb_transaction limit 1";
        let row = sqlx::query(query).fetch_one(&pool).await?;
        let count = row.get::<i64, _>("count");
        info!("ckb_transaction table row count: {:?}", count);

        Ok(())
    }

    pub fn new_query(sql: &str) -> Query<Any, AnyArguments> {
        sqlx::query(sql)
    }

    pub async fn transaction(&self) -> Result<Transaction<'_, Any>> {
        let pool = self.get_pg_pool()?;
        pool.begin().await.map_err(Into::into)
    }

    pub fn get_pg_pool(&self) -> Result<&AnyPool> {
        self.risingwave_pool
            .get()
            .ok_or_else(|| anyhow!("pg pool not inited!"))
    }

    async fn create_tables_for_postgres(&mut self) -> Result<()> {
        let commands_table = SQL_POSTGRES_CREATE_TABLE.split(';');
        let commands_index = SQL_POSTGRES_CREATE_INDEX.split(';');
        for command in commands_table.chain(commands_index) {
            if !command.trim().is_empty() {
                let pool = self.get_pg_pool()?;
                sqlx::query(command).execute(pool).await?;
            }
        }
        Ok(())
    }
}

use crate::Error;
use ckb_types::{H256, core::Capacity, packed::CellOutput, prelude::*};
use sql_builder::SqlBuilder;

const BATCH_SIZE_THRESHOLD: usize = 1_000;

enum FieldValue {
    Binary(Vec<u8>),
    BigInt(i64),
    Int(i32),
    SmallInt(i16),
    NoneSmallInt,
    NoneBinary,
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
            FieldValue::SmallInt(value) => query.bind(value),
            FieldValue::NoneSmallInt => query.bind(Option::<i16>::None),
            FieldValue::NoneBinary => query.bind(Option::<Vec<u8>>::None),
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

async fn query_pending_tx(
    tx_hash: &Vec<u8>,
    pool: &AnyPool,
) -> Result<Option<(Vec<u8>, i32, i32, Vec<u8>, Vec<u8>, i64, i64)>, Error> {
    sqlx::query(
        r#"
        SELECT version,
            input_count,
            output_count,
            witnesses,
            header_deps,
            bytes,
            created_at
        FROM
            ckb_transaction
        WHERE
            tx_hash = $1
        "#,
    )
    .bind(tx_hash.clone())
    .fetch_optional(pool)
    .await
    .map_err(|err| Error::DB(err.to_string()))
    .map(|row| {
        row.map(|row| {
            let version = row.get::<Vec<u8>, _>("version");
            let input_count = row.get::<i32, _>("input_count");
            let output_count = row.get::<i32, _>("output_count");
            let witnesses = row.get::<Vec<u8>, _>("witnesses");
            let header_deps = row.get::<Vec<u8>, _>("header_deps");
            let bytes = row.get::<i64, _>("bytes");
            let created_at = row.get::<i64, _>("created_at");

            (
                version,
                input_count,
                output_count,
                witnesses,
                header_deps,
                bytes,
                created_at,
            )
        })
    })
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

async fn bulk_insert(
    table: &str,
    fields: &[&str],
    rows: &[Vec<FieldValue>],
    pool: &AnyPool,
) -> Result<(), Error> {
    for bulk in rows.chunks(BATCH_SIZE_THRESHOLD) {
        // build query str
        let sql = build_bulk_insert_sql(table, fields, bulk)?;

        // bind
        let mut query = PoolSQLXPool::new_query(&sql);
        for row in bulk {
            for field in row {
                query = field.bind(query);
            }
        }

        // execute
        query
            .execute(pool)
            .await
            .map_err(|err| Error::DB(err.to_string()))?;
    }
    Ok(())
}

// status: 0 pending, 1 committed, 2 rejected
async fn insert_pool_pending_tx(tx_view: &TransactionView, pool: &AnyPool) -> Result<(), Error> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|err| Error::DB(err.to_string()))?
        .as_millis() as i64;

    let tx_hash = tx_view.hash().raw_data().to_vec();
    let tx_version = tx_view.version().to_be_bytes().to_vec();
    let tx_inputs_count = tx_view.inputs().len() as i32;
    let tx_outputs_count = tx_view.outputs().len() as i32;
    let tx_witnesses = tx_view.witnesses().as_bytes().to_vec();
    let tx_header_deps = tx_view.header_deps().as_bytes().to_vec();

    // get tx_size from block ext has some bug
    let bytes = tx_view.data().total_size();

    // insert transaction
    bulk_insert(
        "ckb_transaction",
        &[
            "tx_hash",
            "version",
            "input_count",
            "output_count",
            "witnesses",
            "header_deps",
            "bytes",
            "status",
            "created_at",
            "updated_at",
        ],
        &[vec![
            tx_hash.clone().into(),
            tx_version.into(),
            tx_inputs_count.into(),
            tx_outputs_count.into(),
            tx_witnesses.into(),
            tx_header_deps.into(),
            bytes.into(),
            0.into(), // status: pending
            now.into(),
            now.into(),
        ]],
        pool,
    )
    .await?;

    // process cell deps
    let mut tx_association_cell_dep_rows = Vec::new();
    for (cell_dep_index, cell_dep) in tx_view.cell_deps_iter().enumerate() {
        let outpoint_tx_hash = cell_dep.out_point().tx_hash().raw_data().to_vec();
        let outpoint_index: u32 = cell_dep.out_point().index().into();
        tx_association_cell_dep_rows.push(vec![
            tx_hash.clone().into(),
            cell_dep_index.into(),
            outpoint_tx_hash.into(),
            (outpoint_index as i32).into(),
            (u8::from(cell_dep.dep_type()) as i16).into(),
        ]);
    }
    bulk_insert(
        "tx_association_cell_dep",
        &[
            "tx_hash",
            "index",
            "outpoint_tx_hash",
            "outpoint_index",
            "dep_type",
        ],
        &tx_association_cell_dep_rows,
        pool,
    )
    .await?;

    // process inputs
    for (input_index, input) in tx_view.inputs().into_iter().enumerate() {
        let pre_outpoint_tx_hash = input.previous_output().tx_hash().raw_data().to_vec();
        let pre_outpoint_index: u32 = input.previous_output().index().into();
        let since = input.since().raw_data().to_vec();
        let input_index = input_index as i32;

        // insert to input table
        bulk_insert(
            "input",
            &[
                "tx_hash",
                "pre_outpoint_tx_hash",
                "pre_outpoint_index",
                "since",
                "input_index",
            ],
            &[vec![
                tx_hash.clone().into(),
                pre_outpoint_tx_hash.into(),
                (pre_outpoint_index as i32).into(),
                since.into(),
                input_index.into(),
            ]],
            pool,
        )
        .await?;
    }

    // process outputs
    for (output_index, output) in tx_view.outputs().into_iter().enumerate() {
        let output_capacity: u64 = output.capacity().into();

        // lock script
        let lock_script = output.lock();
        let lock_code_hash = lock_script.code_hash().raw_data().to_vec();
        let lock_hash_type = u8::from(lock_script.hash_type()) as i16;
        let lock_args = lock_script.args().raw_data().to_vec();
        let lock_script_hash = lock_script.calc_script_hash().raw_data().to_vec();

        // type script
        let output_type = output.type_().to_opt();
        let mut type_code_hash = None;
        let mut type_hash_type = None;
        let mut type_args = None;
        let mut type_script_hash: Option<Vec<u8>> = None;
        if let Some(output_type) = output_type {
            type_code_hash = Some(output_type.code_hash().raw_data().to_vec());
            type_hash_type = Some(u8::from(output_type.hash_type()) as i16);
            type_args = Some(output_type.args().raw_data().to_vec());
            type_script_hash = Some(output_type.calc_script_hash().raw_data().to_vec());
        };

        // data
        let output_data = tx_view
            .outputs_data()
            .get(output_index)
            .map(|data| data.raw_data().to_vec())
            .unwrap_or_default();
        let data_size = output_data.len();
        let data_hash: H256 = CellOutput::calc_data_hash(&output_data).into();
        let data_hash = data_hash.as_bytes().to_vec();

        // occupied capacity
        let occupied_capacity: u64 = output
            .occupied_capacity(Capacity::bytes(output_data.len()).unwrap())
            .unwrap()
            .as_u64();

        let _ = bulk_insert(
            "output",
            &[
                "tx_hash",
                "output_index",
                "capacity",
                "lock_code_hash",
                "lock_hash_type",
                "lock_args",
                "lock_script_hash",
                "type_code_hash",
                "type_hash_type",
                "type_args",
                "type_script_hash",
                "data",
                "data_size",
                "data_hash",
                "occupied_capacity",
            ],
            &[vec![
                tx_hash.clone().into(),
                output_index.into(),
                output_capacity.into(),
                lock_code_hash.into(),
                lock_hash_type.into(),
                lock_args.into(),
                lock_script_hash.into(),
                type_code_hash.map_or(FieldValue::NoneBinary, FieldValue::Binary),
                type_hash_type.map_or(FieldValue::NoneSmallInt, FieldValue::SmallInt),
                type_args.map_or(FieldValue::NoneBinary, FieldValue::Binary),
                type_script_hash.map_or(FieldValue::NoneBinary, FieldValue::Binary),
                output_data.into(),
                data_size.into(),
                data_hash.into(),
                occupied_capacity.into(),
            ]],
            pool,
        )
        .await?;
    }
    Ok(())
}
