CREATE INDEX IF NOT EXISTS "index_block_table_block_hash" ON "block" ("block_hash");
CREATE INDEX IF NOT EXISTS "index_block_table_block_number" ON "block" ("block_number");

CREATE INDEX IF NOT EXISTS "index_uncle_block_table_block_number" ON "uncle_block" ("block_number");

CREATE INDEX IF NOT EXISTS "index_tx_table_tx_hash" ON "ckb_transaction" ("tx_hash");
CREATE INDEX IF NOT EXISTS "index_tx_table_block_id" ON "ckb_transaction" ("block_id");
CREATE INDEX IF NOT EXISTS "index_tx_table_block_number" ON "ckb_transaction" ("block_number");
CREATE INDEX IF NOT EXISTS "index_tx_table_block_hash" ON "ckb_transaction" ("block_hash");

CREATE INDEX IF NOT EXISTS "index_tx_association_header_dep_table_tx_id" ON "tx_association_header_dep" ("tx_id");

CREATE INDEX IF NOT EXISTS "index_tx_association_cell_dep_table_tx_id" ON "tx_association_cell_dep" ("tx_id");

CREATE INDEX IF NOT EXISTS "idx_output_table_tx_id_output_index" ON "output" ("tx_id", "output_index");
CREATE INDEX IF NOT EXISTS "idx_output_table_tx_hash" ON "output" ("tx_hash");
CREATE INDEX IF NOT EXISTS "idx_output_table_lock_script_id" ON "output" ("lock_script_id");
CREATE INDEX IF NOT EXISTS "idx_output_table_type_script_id" ON "output" ("type_script_id");
CREATE INDEX IF NOT EXISTS "idx_output_table_consumed_tx_hash_input_index" ON "output" ("consumed_tx_hash", "input_index");

CREATE INDEX IF NOT EXISTS "idx_input_table_consumed_tx_id" ON "input" ("consumed_tx_id");
CREATE INDEX IF NOT EXISTS "idx_input_table_tx_hash" ON "input" ("tx_hash");

CREATE INDEX IF NOT EXISTS "idx_script_table_script_hash" ON "script" ("script_hash");
CREATE INDEX IF NOT EXISTS "idx_script_table_code_hash" ON "script" ("code_hash");

CREATE INDEX IF NOT EXISTS "idx_address_table_address" ON "address" ("address");
CREATE INDEX IF NOT EXISTS "idx_address_table_script_id" ON "address" ("script_id");
