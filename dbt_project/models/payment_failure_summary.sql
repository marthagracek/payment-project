SELECT
    payment_method,
    DATE_TRUNC('month', order_date) AS month,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(payment_failed) AS failed_orders,
    ROUND(CAST(SUM(payment_failed) * 100.0 / COUNT(DISTINCT order_id) AS numeric), 2) AS failure_rate_pct,
    ROUND(CAST(SUM(revenue_at_risk) AS numeric), 2) AS monthly_revenue_at_risk,
    ROUND(CAST(SUM(CASE WHEN retry_success = 1 THEN amount ELSE 0 END) AS numeric), 2) AS recovered_revenue,
    ROUND(CAST(SUM(CASE WHEN retry_success = 1 THEN amount ELSE 0 END) * 100.0 / NULLIF(SUM(revenue_at_risk), 0) AS numeric), 2) AS recovery_rate_pct
FROM {{ ref('stg_orders') }}
GROUP BY payment_method, DATE_TRUNC('month', order_date)
ORDER BY month, payment_method