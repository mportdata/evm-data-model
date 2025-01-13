WITH daily_sent_txs AS (
    SELECT 
        date_trunc('day', tx_timestamp) as activity_date,
        from_address as address,
        COUNT(*) as transactions_sent,
        SUM(gas_used * gas_price) as gas_spent,
        SUM(value) as total_eth_sent
    FROM {{ ref('fct_transactions') }}
    GROUP BY date_trunc('day', tx_timestamp), from_address
),

daily_received_txs AS (
    SELECT 
        date_trunc('day', tx_timestamp) as activity_date,
        to_address as address,
        COUNT(*) as transactions_received,
        SUM(value) as total_eth_received
    FROM {{ ref('fct_transactions') }}
    WHERE to_address IS NOT NULL
    GROUP BY date_trunc('day', tx_timestamp), to_address
),

daily_contract_interactions AS (
    SELECT 
        date_trunc('day', t.tx_timestamp) as activity_date,
        t.from_address as address,
        COUNT(*) as contract_calls
    FROM {{ ref('fct_transactions') }} t
    JOIN {{ ref('dim_addresses') }} a ON t.to_address = a.address
    WHERE a.address_type = 'contract'
    GROUP BY date_trunc('day', t.tx_timestamp), t.from_address
),

daily_unique_interactions AS (
    SELECT 
        date_trunc('day', tx_timestamp) as activity_date,
        from_address as address,
        COUNT(DISTINCT to_address) as unique_interactions
    FROM {{ ref('fct_transactions') }}
    GROUP BY date_trunc('day', tx_timestamp), from_address
)

SELECT 
    COALESCE(s.address, r.address) as address,
    COALESCE(s.activity_date, r.activity_date) as activity_date,
    a.address_type,
    COALESCE(s.transactions_sent, 0) as transactions_sent,
    COALESCE(r.transactions_received, 0) as transactions_received,
    COALESCE(c.contract_calls, 0) as contract_calls,
    COALESCE(s.gas_spent, 0) as total_gas_spent,
    COALESCE(s.total_eth_sent, 0) as total_eth_sent,
    COALESCE(r.total_eth_received, 0) as total_eth_received,
    COALESCE(u.unique_interactions, 0) as unique_interactions,
    MIN(t.tx_timestamp) OVER (PARTITION BY COALESCE(s.address, r.address)) as first_activity,
    MAX(t.tx_timestamp) OVER (PARTITION BY COALESCE(s.address, r.address)) as last_activity
FROM daily_sent_txs s
FULL OUTER JOIN daily_received_txs r 
    ON s.address = r.address 
    AND s.activity_date = r.activity_date
LEFT JOIN daily_contract_interactions c 
    ON COALESCE(s.address, r.address) = c.address 
    AND COALESCE(s.activity_date, r.activity_date) = c.activity_date
LEFT JOIN daily_unique_interactions u 
    ON COALESCE(s.address, r.address) = u.address 
    AND COALESCE(s.activity_date, r.activity_date) = u.activity_date
LEFT JOIN {{ ref('dim_addresses') }} a 
    ON COALESCE(s.address, r.address) = a.address
LEFT JOIN {{ ref('fct_transactions') }} t 
    ON COALESCE(s.address, r.address) IN (t.from_address, t.to_address)
WHERE COALESCE(s.address, r.address) IS NOT NULL 