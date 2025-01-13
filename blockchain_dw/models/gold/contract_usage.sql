WITH contract_interactions AS (
    SELECT 
        date_trunc('day', t.tx_timestamp) as usage_date,
        t.to_address as contract_address,
        COUNT(*) as total_calls,
        COUNT(DISTINCT t.from_address) as unique_callers,
        SUM(CASE WHEN r.status = '0x0' THEN 1 ELSE 0 END) as failed_transactions,
        SUM(CASE WHEN r.status = '0x1' THEN 1 ELSE 0 END) as successful_transactions,
        SUM(t.value) as total_eth_received,
        SUM(t.gas_used) as total_gas_used,
        COUNT(DISTINCT t.from_address) as daily_active_users
    FROM {{ ref('fct_transactions') }} t
    JOIN {{ ref('dim_addresses') }} a ON t.to_address = a.address
    LEFT JOIN {{ ref('dim_transaction_receipts') }} r ON t.tx_hash = r.tx_hash
    WHERE a.address_type = 'contract'
    GROUP BY date_trunc('day', t.tx_timestamp), t.to_address
),

function_signatures AS (
    SELECT 
        date_trunc('day', t.tx_timestamp) as usage_date,
        t.to_address as contract_address,
        SUBSTRING(l.event_signature, 1, 10) as function_sig,
        COUNT(*) as call_count
    FROM {{ ref('fct_transactions') }} t
    JOIN {{ ref('fct_log_events') }} l ON t.tx_hash = l.tx_hash
    GROUP BY 
        date_trunc('day', t.tx_timestamp),
        t.to_address,
        SUBSTRING(l.event_signature, 1, 10)
),

most_called_functions AS (
    SELECT 
        usage_date,
        contract_address,
        function_sig as most_called_function
    FROM (
        SELECT 
            usage_date,
            contract_address,
            function_sig,
            ROW_NUMBER() OVER (
                PARTITION BY usage_date, contract_address 
                ORDER BY call_count DESC
            ) as rn
        FROM function_signatures
    ) ranked
    WHERE rn = 1
)

SELECT 
    ci.contract_address,
    ci.usage_date,
    ci.total_calls,
    ci.unique_callers,
    ci.failed_transactions,
    ci.successful_transactions,
    ci.total_eth_received,
    ci.total_gas_used,
    mcf.most_called_function,
    ci.daily_active_users
FROM contract_interactions ci
LEFT JOIN most_called_functions mcf 
    ON ci.contract_address = mcf.contract_address 
    AND ci.usage_date = mcf.usage_date 