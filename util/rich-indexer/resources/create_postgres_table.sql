CREATE TABLE IF NOT EXISTS block(
    id BIGSERIAL PRIMARY KEY,
    block_hash BYTEA NOT NULL,
    block_number BIGINT NOT NULL,
    compact_target BYTEA,
    parent_hash BYTEA,
    nonce BYTEA,
    difficulty BYTEA,
    timestamp BIGINT,
    version BYTEA,
    transactions_root BYTEA,
    transactions_count INTEGER,
    epoch BYTEA,
    start_number BIGINT,
    epoch_length INTEGER,
    epoch_number BIGINT,
    dao BYTEA,
    proposals_hash BYTEA,
    extra_hash BYTEA,
    extension BYTEA,
    proposals BYTEA,
    proposals_count INTEGER,
    uncles_count INTEGER,
    uncle_block_hashes BYTEA,
    miner_script BYTEA,
    miner_message character varying,
    reward BIGINT,
    total_transaction_fee BIGINT,
    cell_consumed BIGINT,
    total_cell_capacity BIGINT,
    block_size INTEGER,
    cycles BIGINT,
    live_cell_changes INTEGER
);

CREATE TABLE IF NOT EXISTS uncle_block(
    id BIGSERIAL PRIMARY KEY,
    index INTEGER,
    block_hash BYTEA NOT NULL,
    block_number BIGINT NOT NULL,
    compact_target BYTEA,
    parent_hash BYTEA,
    nonce BYTEA,
    timestamp BIGINT,
    version BYTEA,
    transactions_root BYTEA,
    epoch BYTEA,
    dao BYTEA,
    proposals_hash BYTEA,
    extra_hash BYTEA,
    extension BYTEA,
    proposals BYTEA,
    difficulty BYTEA
);

CREATE TABLE IF NOT EXISTS ckb_transaction(
    id BIGSERIAL PRIMARY KEY,
    tx_hash BYTEA NOT NULL,
    version BYTEA NOT NULL,
    input_count INTEGER NOT NULL,
    output_count INTEGER NOT NULL,
    witnesses BYTEA,
    block_id BIGINT NOT NULL,
    block_number BIGINT NOT NULL,
    block_hash BYTEA NOT NULL,
    block_timestamp BIGINT NOT NULL,
    tx_index INTEGER NOT NULL,
    header_deps BYTEA,
    cycles BIGINT,
    transaction_fee BIGINT,
    bytes BIGINT,
    capacity_involved BIGINT
);

CREATE TABLE IF NOT EXISTS tx_association_header_dep(
    id BIGSERIAL,
    tx_id BIGINT NOT NULL,
    block_id BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS tx_association_cell_dep(
    id BIGSERIAL,
    tx_id BIGINT NOT NULL,
    index INTEGER NOT NULL,
    outpoint_tx_hash BYTEA NOT NULL,
    outpoint_index INTEGER NOT NULL,
    output_id BIGINT NOT NULL,
    dep_type SMALLINT NOT NULL
);

CREATE TABLE IF NOT EXISTS output(
    id BIGSERIAL PRIMARY KEY,
    tx_id BIGINT NOT NULL,
    tx_hash BYTEA NOT NULL,
    output_index INTEGER NOT NULL,
    capacity BIGINT NOT NULL,
    lock_script_id BIGINT,
    type_script_id BIGINT,
    data BYTEA,
    data_size INTEGER,
    data_hash BYTEA,
    occupied_capacity BIGINT,
    is_spent INTEGER DEFAULT 0,
    consumed_tx_hash BYTEA,
    input_index INTEGER,
    block_number BIGINT NOT NULL,
    block_timestamp BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS output_data(
    id BIGSERIAL PRIMARY KEY,
    output_id BIGINT NOT NULL,
    data BYTEA
);

CREATE TABLE IF NOT EXISTS input(
    id BIGSERIAL PRIMARY KEY,
    output_id BIGINT,
    pre_outpoint_tx_hash BYTEA NOT NULL,
    pre_outpoint_index INTEGER NOT NULL,
    since BYTEA NOT NULL,
    consumed_tx_id BIGINT NOT NULL,
    consumed_tx_hash BYTEA,
    input_index INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS script(
    id BIGSERIAL PRIMARY KEY,
    code_hash BYTEA NOT NULL,
    hash_type SMALLINT NOT NULL,
    args BYTEA,
    script_hash BYTEA NOT NULL,
    UNIQUE(code_hash, hash_type, args)
);
