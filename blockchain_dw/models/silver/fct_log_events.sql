WITH flattened_logs AS (
    SELECT 
        tx_hash,
        UNNEST(logs) as log_entry
    FROM {{ ref('stg_transaction_receipts') }}
    WHERE array_length(logs) > 0  -- Only process transactions with logs
)

SELECT 
    tx_hash,
    CAST(REPLACE(log_entry->'logIndex', '0x', '') AS BIGINT) as log_index,
    CAST(log_entry->'address' AS VARCHAR) as address,
    CAST(log_entry->'topics'->1 AS VARCHAR) as event_signature,
    CAST(log_entry->'data' AS VARCHAR) as data,
    CAST(REPLACE(log_entry->'blockNumber', '0x', '') AS BIGINT) as block_number
FROM flattened_logs
WHERE tx_hash IS NOT NULL