SELECT 
    CAST(transactionHash AS VARCHAR) as tx_hash,
    CAST(status AS VARCHAR) as status,
    CAST(contractAddress AS VARCHAR) as contract_address,
    CAST(cumulativeGasUsed AS VARCHAR) as cumulative_gas_used,
    CAST(effectiveGasPrice AS VARCHAR) as effective_gas_price,
    CAST(blockHash AS VARCHAR) as block_hash,
    CAST(blockNumber AS VARCHAR) as block_number,
    CAST(transactionIndex AS VARCHAR) as transaction_index,
    CAST("from" AS VARCHAR) as from_address,
    CAST("to" AS VARCHAR) as to_address,
    CAST(logsBloom AS VARCHAR) as logs_bloom,
    CAST(type AS VARCHAR) as type,
    logs
FROM read_json_auto('s3://raw/transaction_receipts/*.json') 