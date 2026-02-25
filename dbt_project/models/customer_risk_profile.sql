SELECT
    customer_id,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(payment_failed) AS total_failures,
    ROUND(CAST(SUM(payment_failed) * 100.0 / COUNT(DISTINCT order_id) AS numeric), 2) AS failure_rate_pct,
    ROUND(CAST(SUM(revenue_at_risk) AS numeric), 2) AS total_revenue_at_risk,
    CASE
        WHEN SUM(payment_failed) * 100.0 / COUNT(DISTINCT order_id) > 30 THEN 'High Risk'
        WHEN SUM(payment_failed) * 100.0 / COUNT(DISTINCT order_id) > 15 THEN 'Medium Risk'
        ELSE 'Low Risk'
    END AS risk_segment
FROM {{ ref('stg_orders') }}
GROUP BY customer_id