WITH flattened_txs AS (
    SELECT 
        b.block_hash,
        b.block_timestamp,
        t as transaction_data
    FROM {{ ref('stg_blocks') }} b,
    UNNEST(b.transactions) AS t
)

SELECT 
    CAST(transaction_data->>'hash' AS VARCHAR) as tx_hash,
    CAST(transaction_data->>'blockHash' AS VARCHAR) as block_hash,
    CAST(transaction_data->>'from' AS VARCHAR) as from_address,
    CAST(transaction_data->>'to' AS VARCHAR) as to_address,
    CAST(transaction_data->>'value' AS VARCHAR) as value,
    CAST(transaction_data->>'gas' AS VARCHAR) as gas,
    CAST(transaction_data->>'gasPrice' AS VARCHAR) as gas_price,
    CAST(transaction_data->>'transactionIndex' AS VARCHAR) as transaction_index,
    CAST(transaction_data->>'type' AS VARCHAR) as tx_type,
    CAST(transaction_data->>'nonce' AS VARCHAR) as nonce,
    CAST(transaction_data->>'input' AS VARCHAR) as input_data,
    CAST(transaction_data->>'v' AS VARCHAR) as v,
    CAST(transaction_data->>'r' AS VARCHAR) as r,
    CAST(transaction_data->>'s' AS VARCHAR) as s,
    block_timestamp as raw_block_timestamp
FROM flattened_txs 