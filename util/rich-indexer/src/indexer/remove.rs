use super::*;

use ckb_indexer_sync::Error;
use sql_builder::SqlBuilder;
use sqlx::{Any, Row, Transaction};

pub(crate) async fn rollback_block(tx: &mut Transaction<'_, Any>) -> Result<(), Error> {
    let block_id = if let Some(block_id) = query_tip_id(tx).await? {
        block_id
    } else {
        return Ok(());
    };

    let tx_id_list = query_tx_id_list_by_block_id(block_id, tx).await?;

    // update spent cells
    reset_spent_cells(&tx_id_list, tx).await?;

    // remove transactions, associations, inputs, output
    remove_batch_by_blobs("ckb_transaction", "id", &tx_id_list, tx).await?;
    remove_batch_by_blobs("tx_association_cell_dep", "tx_id", &tx_id_list, tx).await?;
    remove_batch_by_blobs("tx_association_header_dep", "tx_id", &tx_id_list, tx).await?;
    remove_batch_by_blobs("input", "consumed_tx_id", &tx_id_list, tx).await?;
    remove_batch_by_blobs("output", "tx_id", &tx_id_list, tx).await?;

    // remove block
    remove_batch_by_blobs("block", "id", &[block_id], tx).await?;

    Ok(())
}

async fn remove_batch_by_blobs(
    table_name: &str,
    column_name: &str,
    ids: &[i64],
    tx: &mut Transaction<'_, Any>,
) -> Result<(), Error> {
    if ids.is_empty() {
        return Ok(());
    }

    // build query str
    let mut query_builder = SqlBuilder::delete_from(table_name);
    let sql = query_builder
        .and_where_in(column_name, &sqlx_param_placeholders(1..ids.len())?)
        .sql()
        .map_err(|err| Error::DB(err.to_string()))?;

    // bind
    let mut query: sqlx::query::Query<'_, Any, sqlx::any::AnyArguments<'_>> = sqlx::query(&sql);
    for hash in ids {
        query = query.bind(hash);
    }

    // execute
    query
        .execute(tx.as_mut())
        .await
        .map_err(|err| Error::DB(err.to_string()))?;

    Ok(())
}

async fn reset_spent_cells(tx_id_list: &[i64], tx: &mut Transaction<'_, Any>) -> Result<(), Error> {
    let query = SqlBuilder::update_table("output")
        .set("is_spent", 0)
        .set("consumed_tx_hash", "")
        .set("input_index", -1)
        .and_where_in_query(
            "id",
            SqlBuilder::select_from("input")
                .field("output_id")
                .and_where_in("consumed_tx_id", tx_id_list)
                .query()
                .map_err(|err| Error::DB(err.to_string()))?,
        )
        .sql()
        .map_err(|err| Error::DB(err.to_string()))?;

    sqlx::query(&query)
        .execute(tx.as_mut())
        .await
        .map_err(|err| Error::DB(err.to_string()))?;

    Ok(())
}

async fn query_tip_id(tx: &mut Transaction<'_, Any>) -> Result<Option<i64>, Error> {
    SQLXPool::new_query(
        r#"
            SELECT id FROM block
            ORDER BY id DESC
            LIMIT 1
            "#,
    )
    .fetch_optional(tx.as_mut())
    .await
    .map(|res| res.map(|row| row.get::<i64, _>("id")))
    .map_err(|err| Error::DB(err.to_string()))
}

async fn query_tx_id_list_by_block_id(
    block_id: i64,
    tx: &mut Transaction<'_, Any>,
) -> Result<Vec<i64>, Error> {
    SQLXPool::new_query(
        r#"
        SELECT id FROM ckb_transaction
        WHERE block_id = $1
        ORDER BY id
        ASC
        "#,
    )
    .bind(block_id)
    .fetch_all(tx.as_mut())
    .await
    .map(|rows| {
        rows.into_iter()
            .map(|row| row.get::<i64, _>("id"))
            .collect()
    })
    .map_err(|err| Error::DB(err.to_string()))
}

fn sqlx_param_placeholders(range: std::ops::Range<usize>) -> Result<Vec<String>, Error> {
    if range.start == 0 {
        return Err(Error::Params("no valid parameter".to_owned()));
    }
    Ok((1..=range.end)
        .map(|i| format!("${}", i))
        .collect::<Vec<String>>())
}
