WITH block_transactions AS (
    SELECT 
        b.block_number,
        COUNT(DISTINCT t.tx_hash) as transaction_count,
        COUNT(DISTINCT t.from_address) + COUNT(DISTINCT t.to_address) as unique_addresses,
        SUM(t.value) as total_eth_transferred,
        AVG(t.gas_price) as avg_gas_price,
        COUNT(DISTINCT CASE WHEN t.to_address IN (SELECT address FROM {{ ref('dim_addresses') }} WHERE address_type = 'contract') 
            THEN t.tx_hash END) as total_contract_calls
    FROM {{ ref('fct_blocks') }} b
    LEFT JOIN {{ ref('fct_transactions') }} t ON b.block_hash = t.block_hash
    GROUP BY b.block_number
)

SELECT 
    b.block_number,
    b.block_timestamp,
    b.miner,
    bd.gas_used,
    bd.gas_limit,
    (bd.gas_used * 100.0 / NULLIF(bd.gas_limit, 0)) as gas_utilization_pct,
    COALESCE(bt.transaction_count, 0) as transaction_count,
    COALESCE(bt.unique_addresses, 0) as unique_addresses,
    COALESCE(bt.total_eth_transferred, 0) as total_eth_transferred,
    COALESCE(bt.avg_gas_price, 0) as avg_gas_price,
    COALESCE(bt.total_contract_calls, 0) as total_contract_calls
FROM {{ ref('fct_blocks') }} b
LEFT JOIN {{ ref('dim_block_details') }} bd ON b.block_hash = bd.block_hash
LEFT JOIN block_transactions bt ON b.block_number = bt.block_number 