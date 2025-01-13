SELECT 
    tx_hash,
    TRY_CAST(REPLACE(status, '0x', '') AS VARCHAR) as status,
    contract_address,
    TRY_CAST(REPLACE(cumulative_gas_used, '0x', '') AS BIGINT) as cumulative_gas_used,
    TRY_CAST(REPLACE(effective_gas_price, '0x', '') AS BIGINT) as effective_gas_price
FROM {{ ref('stg_transaction_receipts') }}
WHERE tx_hash IS NOT NULL 