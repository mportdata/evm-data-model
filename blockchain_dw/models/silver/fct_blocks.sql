SELECT 
    block_hash,
    parent_hash,
    CAST(REPLACE(block_number, '0x', '') AS BIGINT) as block_number,
    to_timestamp(CAST(REPLACE(block_timestamp, '0x', '') AS BIGINT)) as block_timestamp,
    miner_address as miner
FROM {{ ref('stg_blocks') }}