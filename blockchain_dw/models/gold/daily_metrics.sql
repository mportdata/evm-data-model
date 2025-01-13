WITH block_times AS (
    SELECT 
        date_trunc('day', block_timestamp) as metric_date,
        EXTRACT(EPOCH FROM (LEAD(block_timestamp) OVER (ORDER BY block_number) - block_timestamp)) as block_time
    FROM {{ ref('fct_blocks') }}
),

daily_blocks AS (
    SELECT 
        date_trunc('day', block_timestamp) as metric_date,
        COUNT(*) as total_blocks,
        AVG(bt.block_time) as avg_block_time
    FROM {{ ref('fct_blocks') }} b
    LEFT JOIN block_times bt ON date_trunc('day', b.block_timestamp) = bt.metric_date
    GROUP BY date_trunc('day', block_timestamp)
),

daily_txs AS (
    SELECT 
        date_trunc('day', tx_timestamp) as metric_date,
        COUNT(*) as total_transactions,
        COUNT(DISTINCT from_address) as unique_senders,
        COUNT(DISTINCT to_address) as unique_receivers,
        COUNT(DISTINCT from_address) + COUNT(DISTINCT to_address) as daily_active_addresses,
        SUM(gas_used) as total_gas_used,
        AVG(gas_price) as avg_gas_price,
        SUM(value) as total_eth_volume
    FROM {{ ref('fct_transactions') }}
    GROUP BY date_trunc('day', tx_timestamp)
),

new_contracts AS (
    SELECT 
        date_trunc('day', tx_timestamp) as metric_date,
        COUNT(DISTINCT r.contract_address) as new_contracts_deployed
    FROM {{ ref('fct_transactions') }} t
    JOIN {{ ref('dim_transaction_receipts') }} r ON t.tx_hash = r.tx_hash
    WHERE r.contract_address IS NOT NULL
    GROUP BY date_trunc('day', tx_timestamp)
)

SELECT 
    db.metric_date,
    db.avg_block_time,
    db.total_blocks,
    COALESCE(dt.total_transactions, 0) as total_transactions,
    COALESCE(dt.daily_active_addresses, 0) as daily_active_addresses,
    COALESCE(dt.total_gas_used, 0) as total_gas_used,
    COALESCE(dt.avg_gas_price, 0) as avg_gas_price,
    COALESCE(nc.new_contracts_deployed, 0) as new_contracts_deployed,
    COALESCE(dt.total_eth_volume, 0) as total_eth_volume,
    COALESCE(dt.unique_senders, 0) as unique_senders,
    COALESCE(dt.unique_receivers, 0) as unique_receivers
FROM daily_blocks db
LEFT JOIN daily_txs dt ON db.metric_date = dt.metric_date
LEFT JOIN new_contracts nc ON db.metric_date = nc.metric_date 