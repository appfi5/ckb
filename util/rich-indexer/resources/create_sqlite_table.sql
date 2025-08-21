CREATE TABLE IF NOT EXISTS block(
    id INTEGER PRIMARY KEY,
    block_hash BLOB NOT NULL,
    block_number INTEGER NOT NULL,
    compact_target BLOB,
    parent_hash BLOB,
    nonce BLOB,
    difficulty BLOB,
    timestamp INTEGER,
    version BLOB,
    transactions_root BLOB,
    transactions_count INTEGER,
    epoch BLOB,
    start_number INTEGER,
    epoch_length INTEGER,
    epoch_number INTEGER,
    dao BLOB,
    proposals_hash BLOB,
    extra_hash BLOB,
    extension BLOB,
    proposals BLOB,
    proposals_count INTEGER,
    uncles_count INTEGER,
    uncle_block_hashes BLOB,
    miner_script BLOB,
    miner_message TEXT,
    reward INTEGER,
    total_transaction_fee INTEGER,
    cell_consumed INTEGER,
    total_cell_capacity INTEGER,
    block_size INTEGER,
    cycles INTEGER,
    live_cell_changes INTEGER
);

CREATE TABLE IF NOT EXISTS uncle_block(
    id INTEGER PRIMARY KEY,
    index INTEGER,
    block_hash BLOB NOT NULL,
    block_number INTEGER NOT NULL,
    compact_target BLOB,
    parent_hash BLOB,
    nonce BLOB,
    timestamp INTEGER,
    version BLOB,
    transactions_root BLOB,
    epoch BLOB,
    dao BLOB,
    proposals_hash BLOB,
    extra_hash BLOB,
    extension BLOB,
    proposals BLOB
);

CREATE TABLE IF NOT EXISTS ckb_transaction(
    id INTEGER PRIMARY KEY,
    tx_hash BLOB NOT NULL,
    version BLOB NOT NULL,
    input_count INTEGER NOT NULL,
    output_count INTEGER NOT NULL,
    witnesses BLOB,
    block_id INTEGER NOT NULL,
    block_number INTEGER NOT NULL,
    block_hash BLOB,
    block_timestamp INTEGER,
    tx_index INTEGER NOT NULL,
    header_deps BLOB,
    cycles INTEGER,
    transaction_fee INTEGER,
    bytes INTEGER,
    capacity_involved INTEGER
);

CREATE TABLE IF NOT EXISTS tx_association_header_dep(
    id INTEGER PRIMARY KEY,
    tx_id INTEGER NOT NULL,
    block_id INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS tx_association_cell_dep(
    id INTEGER PRIMARY KEY,
    tx_id INTEGER NOT NULL,
    index INTEGER NOT NULL,
    outpoint_tx_hash BLOB NOT NULL,
    outpoint_index INTEGER NOT NULL,
    output_id INTEGER NOT NULL,
    dep_type INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS output(
    id INTEGER PRIMARY KEY,
    tx_id INTEGER NOT NULL,
    tx_hash BLOB NOT NULL,
    output_index INTEGER NOT NULL,
    capacity INTEGER NOT NULL,
    lock_script_id INTEGER,
    type_script_id INTEGER,
    data BLOB,
    occupied_capacity INTEGER
    is_spent INTEGER DEFAULT 0,
    consumed_tx_hash BLOB,
    input_index INTEGER
);

CREATE TABLE IF NOT EXISTS input(
    id INTEGER PRIMARY KEY,
    output_id INTEGER,
    pre_outpoint_tx_hash BLOB NOT NULL,
    pre_outpoint_index INTEGER NOT NULL,
    since BLOB NOT NULL,
    consumed_tx_id INTEGER NOT NULL,
    consumed_tx_hash BLOB,
    input_index INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS script(
    id INTEGER PRIMARY KEY,
    code_hash BLOB NOT NULL,
    hash_type INTEGER NOT NULL,
    args BLOB,
    script_hash BLOB NOT NULL,
    UNIQUE(code_hash, hash_type, args)
);
