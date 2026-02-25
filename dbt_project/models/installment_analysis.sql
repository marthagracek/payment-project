SELECT
    payment_installments,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(payment_failed) AS failed_orders,
    ROUND(CAST(SUM(payment_failed) * 100.0 / COUNT(DISTINCT order_id) AS numeric), 2) AS failure_rate_pct,
    ROUND(CAST(SUM(revenue_at_risk) AS numeric), 2) AS revenue_at_risk,
    ROUND(CAST(AVG(amount) AS numeric), 2) AS avg_order_value
FROM {{ ref('stg_orders') }}
WHERE payment_method = 'credit_card'
GROUP BY payment_installments
ORDER BY payment_installments