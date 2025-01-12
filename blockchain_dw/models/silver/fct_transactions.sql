SELECT 
    t.tx_hash,
    t.block_hash,
    t.from_address,
    t.to_address,
    CAST(REPLACE(t.value, '0x', '') AS BIGINT) as value,
    CAST(REPLACE(r.cumulative_gas_used, '0x', '') AS BIGINT) as gas_used,
    CAST(REPLACE(t.gas_price, '0x', '') AS BIGINT) as gas_price,
    CAST(REPLACE(t.transaction_index, '0x', '') AS BIGINT) as transaction_index,
    to_timestamp(CAST(REPLACE(b.block_timestamp, '0x', '') AS BIGINT)) as tx_timestamp,
    t.tx_type
FROM {{ ref('stg_transactions') }} t
LEFT JOIN {{ ref('stg_blocks') }} b 
    ON t.block_hash = b.block_hash
LEFT JOIN {{ ref('stg_transaction_receipts') }} r 
    ON t.tx_hash = r.tx_hash