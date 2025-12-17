mod remove;
mod update;

pub(crate) use remove::*;

use crate::{RichIndexerHandle, service::SUBSCRIBER_NAME, store::SQLXPool};

use ckb_async_runtime::Handle;
use ckb_indexer_sync::{CustomFilters, Error, IndexerSync, Pool};
use ckb_types::{
    H256,
    core::{BlockExt, BlockNumber, BlockView},
    packed::Byte32,
};
use std::sync::Arc;

use update::{init_block, update_block};

/// the database tables are as follows:
///
/// - block
/// - tx
/// - input
/// - output
/// - script
/// - block_association_proposal
/// - block_association_uncle
/// - tx_association_header_dep
/// - tx_association_cell_dep
///   The detailed table design can be found in the SQL files in the resources folder of this crate
///
/// Rich-Indexer, which is based on a relational database
#[derive(Clone)]
pub(crate) struct RichIndexer {
    async_rich_indexer: AsyncRichIndexer,
    async_runtime: Handle,
    request_limit: usize,
}

impl RichIndexer {
    /// Construct new Rich Indexer instance
    pub fn new(
        store: SQLXPool,
        pool: Option<Arc<Pool>>,
        custom_filters: CustomFilters,
        async_runtime: Handle,
        request_limit: usize,
    ) -> Self {
        Self {
            async_rich_indexer: AsyncRichIndexer::new(store, pool, custom_filters),
            async_runtime,
            request_limit,
        }
    }
}

impl IndexerSync for RichIndexer {
    /// Retrieves the tip of the indexer
    fn tip(&self) -> Result<Option<(BlockNumber, Byte32)>, Error> {
        let indexer_handle = RichIndexerHandle::new(
            self.async_rich_indexer.store.clone(),
            self.async_rich_indexer.pool.clone(),
            self.async_runtime.clone(),
            self.request_limit,
        );
        indexer_handle
            .get_indexer_tip()
            .map(|tip| tip.map(|tip| (tip.block_number.value(), tip.block_hash.0.into())))
            .map_err(|err| Error::DB(err.to_string()))
    }

    /// Appends a new block to the indexer
    fn append(
        &self,
        block: &BlockView,
        block_ext: &BlockExt,
        block_interval: u64,
    ) -> Result<(), Error> {
        let future = self
            .async_rich_indexer
            .append(block, block_ext, block_interval);
        self.async_runtime.block_on(future)
    }

    /// Rollback the indexer to a previous state
    fn rollback(&self) -> Result<(), Error> {
        let future = self.async_rich_indexer.rollback();
        self.async_runtime.block_on(future)
    }

    /// Return identity
    fn get_identity(&self) -> &str {
        SUBSCRIBER_NAME
    }

    /// Set init tip
    fn set_init_tip(&self, init_tip_number: u64, init_tip_hash: &H256) {
        let future = self
            .async_rich_indexer
            .set_init_tip(init_tip_number, init_tip_hash);
        self.async_runtime.block_on(future)
    }
}

/// Async rich-indexer.
#[derive(Clone)]
pub(crate) struct AsyncRichIndexer {
    /// storage
    pub(crate) store: SQLXPool,
    /// An optional overlay to index the pending txs in the ckb tx pool
    /// currently only supports removals of dead cells from the pending txs
    pub(crate) pool: Option<Arc<Pool>>,
    /// custom filters
    custom_filters: CustomFilters,
}

impl AsyncRichIndexer {
    /// Construct new AsyncRichIndexer instance
    pub fn new(store: SQLXPool, pool: Option<Arc<Pool>>, custom_filters: CustomFilters) -> Self {
        Self {
            store,
            pool,
            custom_filters,
        }
    }
}

impl AsyncRichIndexer {
    pub(crate) async fn append(
        &self,
        block: &BlockView,
        block_ext: &BlockExt,
        block_interval: u64,
    ) -> Result<(), Error> {
        let mut tx = self
            .store
            .transaction()
            .await
            .map_err(|err| Error::DB(err.to_string()))?;

        if self.custom_filters.is_block_filter_match(block) {
            update_block(block, block_ext, block_interval, &mut tx).await?;
        }

        tx.commit()
            .await
            .map_err(|err| Error::DB(err.to_string()))?;

        if let Some(pool) = self.pool.as_ref() {
            pool.transactions_committed(&block.transactions()).await;
        }

        Ok(())
    }

    pub(crate) async fn rollback(&self) -> Result<(), Error> {
        let mut tx = self
            .store
            .transaction()
            .await
            .map_err(|err| Error::DB(err.to_string()))?;

        rollback_block(&mut tx).await?;

        tx.commit().await.map_err(|err| Error::DB(err.to_string()))
    }

    pub(crate) async fn set_init_tip(&self, init_tip_number: u64, init_tip_hash: &H256) {
        let mut tx = self
            .store
            .transaction()
            .await
            .map_err(|err| Error::DB(err.to_string()))
            .expect("set_init_tip create transaction should be OK");

        init_block(init_tip_number, init_tip_hash, &mut tx)
            .await
            .expect("set_init_tip bulk_insert_and_return_ids should be OK");

        tx.commit().await.expect("set_init_tip commit should be OK");
    }
}

pub(crate) fn to_fixed_array<const LEN: usize>(input: &[u8]) -> [u8; LEN] {
    assert_eq!(input.len(), LEN);
    let mut list = [0; LEN];
    list.copy_from_slice(input);
    list
}
