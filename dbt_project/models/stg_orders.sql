SELECT
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp::timestamp AS order_date,
    payment_type AS payment_method,
    payment_installments,
    payment_value AS amount,
    payment_failed,
    retry_attempted,
    retry_success,
    revenue_at_risk
FROM enriched_orders
WHERE payment_value IS NOT NULL