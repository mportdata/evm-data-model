SELECT 
    block_hash,
    parent_hash,
    TRY_CAST(REPLACE(block_number, '0x', '') AS BIGINT) as block_number,
    to_timestamp(TRY_CAST(REPLACE(block_timestamp, '0x', '') AS BIGINT)) as block_timestamp,
    miner_address as miner
FROM {{ ref('stg_blocks') }}
WHERE block_number IS NOT NULL