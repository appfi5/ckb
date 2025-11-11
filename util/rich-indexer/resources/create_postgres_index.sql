CREATE INDEX IF NOT EXISTS "index_block_table_block_hash" ON "block" ("block_hash");
CREATE INDEX IF NOT EXISTS "index_block_table_block_number" ON "block" ("block_number");
CREATE INDEX IF NOT EXISTS "index_block_table_timestamp" ON "block" ("timestamp");
CREATE INDEX IF NOT EXISTS "index_block_table_epoch_number" ON "block" ("epoch_number");

CREATE INDEX IF NOT EXISTS "index_uncle_block_table_block_number" ON "uncle_block" ("block_number");
CREATE INDEX IF NOT EXISTS "index_uncle_block_table_timestamp" ON "uncle_block" ("timestamp");

CREATE INDEX IF NOT EXISTS "index_tx_table_tx_hash" ON "ckb_transaction" ("tx_hash");
CREATE INDEX IF NOT EXISTS "index_tx_table_block_id" ON "ckb_transaction" ("block_id");
CREATE INDEX IF NOT EXISTS "index_tx_table_block_number" ON "ckb_transaction" ("block_number");
CREATE INDEX IF NOT EXISTS "index_tx_table_block_hash" ON "ckb_transaction" ("block_hash");
CREATE INDEX IF NOT EXISTS "index_tx_table_block_timestamp" ON "ckb_transaction" ("block_timestamp");

CREATE INDEX IF NOT EXISTS "index_tx_association_header_dep_table_tx_id" ON "tx_association_header_dep" ("tx_id");

CREATE INDEX IF NOT EXISTS "index_tx_association_cell_dep_table_tx_id" ON "tx_association_cell_dep" ("tx_id");

CREATE INDEX IF NOT EXISTS "idx_output_table_tx_id_output_index" ON "output" ("tx_id", "output_index");
CREATE INDEX IF NOT EXISTS "idx_output_table_tx_hash_output_index" ON "output" ("tx_hash", "output_index");
CREATE INDEX IF NOT EXISTS "idx_output_table_type_script_id" ON "output" ("type_script_id");
CREATE INDEX IF NOT EXISTS "idx_output_table_block_number" ON "output" ("block_number");
CREATE INDEX IF NOT EXISTS "idx_output_table_block_timestamp" ON "output" ("block_timestamp");
CREATE INDEX IF NOT EXISTS "idx_output_table_consumed_tx_hash_input_index" ON "output" ("consumed_tx_hash", "input_index");
CREATE INDEX IF NOT EXISTS "idx_output_table_consumed_block_number" ON "output" ("consumed_block_number");
CREATE INDEX IF NOT EXISTS "idx_output_table_consumed_timestamp_is_spent" ON "output" ("consumed_timestamp", "is_spent");
CREATE INDEX IF NOT EXISTS "idx_output_final_covering" ON "output" ("block_timestamp", "consumed_timestamp", "lock_script_id") INCLUDE ("occupied_capacity");
CREATE INDEX IF NOT EXISTS "idx_output_consumed_block_lock_capacity" ON "output" ("consumed_timestamp", "block_timestamp", "lock_script_id") INCLUDE ("occupied_capacity");
CREATE INDEX IF NOT EXISTS "idx_output_time_capacity" ON "output" ("consumed_timestamp", "block_timestamp") INCLUDE ("capacity");

CREATE INDEX IF NOT EXISTS "idx_output_data_table_output_id" ON "output_data" ("output_id");

CREATE INDEX IF NOT EXISTS "idx_input_table_consumed_tx_id" ON "input" ("consumed_tx_id");
CREATE INDEX IF NOT EXISTS "idx_input_table_consumed_tx_hash_input_index" ON "input" ("consumed_tx_hash", "input_index");

CREATE INDEX IF NOT EXISTS "idx_script_table_script_hash" ON "script" ("script_hash");
CREATE INDEX IF NOT EXISTS "idx_script_table_code_hash" ON "script" ("code_hash");
CREATE INDEX IF NOT EXISTS "idx_script_table_timestamp_is_typescript" ON "script" ("timestamp", "is_typescript");
