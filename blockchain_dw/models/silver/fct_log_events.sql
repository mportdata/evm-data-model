WITH flattened_logs AS (
    SELECT 
        tx_hash,
        l.value as log_data
    FROM {{ ref('stg_transaction_receipts') }},
    UNNEST(logs) as l
)

SELECT 
    tx_hash,
    CAST(REPLACE(log_data->>'logIndex', '0x', '') AS BIGINT) as log_index,
    CAST(log_data->>'address' AS VARCHAR) as address,
    log_data->>'topics'[0] as event_signature,
    log_data->>'data' as data,
    CAST(REPLACE(log_data->>'blockNumber', '0x', '') AS BIGINT) as block_number
FROM flattened_logs