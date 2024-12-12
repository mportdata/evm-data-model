```Mermaid
erDiagram
    blocks {
        string hash PK
        string parent_hash FK
        bigint number
        timestamp timestamp
        string miner
        bigint gas_limit
        bigint gas_used
        bigint base_fee_per_gas
        bigint size
        bigint difficulty
    }

    block_roots {
        string block_hash PK "FK to blocks"
        string receipts_root
        string state_root
        string transactions_root
    }

    block_consensus {
        string block_hash PK "FK to blocks"
        string nonce
        string mix_hash
        string sha3_uncles
    }

    block_metadata {
        string block_hash PK "FK to blocks"
        string logs_bloom
        string extra_data
    }

    block_running_totals {
        string block_hash PK "FK to blocks"
        bigint total_difficulty
        bigint cumulative_gas_used
    }

    blocks ||--|| block_roots : has
    blocks ||--|| block_consensus : has
    blocks ||--|| block_metadata : has
    blocks ||--|| block_running_totals : has
```
