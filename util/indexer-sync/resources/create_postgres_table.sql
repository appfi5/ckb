CREATE TABLE IF NOT EXISTS ckb_transaction(
    tx_hash BYTEA PRIMARY KEY,
    version BYTEA,
    input_count INTEGER,
    output_count INTEGER,
    witnesses BYTEA,
    header_deps BYTEA,
    bytes BIGINT,
    status SMALLINT,
    created_at BIGINT,
    updated_at BIGINT
) APPEND ONLY WITH(retention_seconds = 300);

CREATE TABLE IF NOT EXISTS tx_association_cell_dep(
    tx_hash BYTEA,
    index INTEGER,
    outpoint_tx_hash BYTEA,
    outpoint_index INTEGER,
    dep_type SMALLINT,
    PRIMARY KEY("tx_hash","index")
) APPEND ONLY WITH(retention_seconds = 300);

CREATE TABLE IF NOT EXISTS input(
    tx_hash BYTEA,
    input_index INTEGER,
    pre_outpoint_tx_hash BYTEA,
    pre_outpoint_index INTEGER,
    since BYTEA,
    PRIMARY KEY("tx_hash","input_index")
) APPEND ONLY WITH(retention_seconds = 300);

CREATE TABLE IF NOT EXISTS output(
    tx_hash BYTEA,
    output_index INTEGER,
    capacity BIGINT,
    lock_code_hash BYTEA,
    lock_hash_type SMALLINT,
    lock_args BYTEA,
    lock_script_hash BYTEA,
    type_code_hash BYTEA,
    type_hash_type SMALLINT,
    type_args BYTEA,
    type_script_hash BYTEA,
    data BYTEA,
    data_size INTEGER,
    data_hash BYTEA,
    occupied_capacity BIGINT,
    PRIMARY KEY("tx_hash","output_index")
) APPEND ONLY WITH(retention_seconds = 300);
