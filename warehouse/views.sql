-- Curated analytical views for the KPI analyst system
-- The LLM should query these views rather than raw tables.

-- Enriched fact table: one row per order with all dimensions
CREATE OR REPLACE VIEW fact_orders_enriched AS
SELECT
    o.order_id,
    o.order_date,
    o.customer_id,
    o.customer_type,
    o.channel,
    o.region,
    o.category,
    o.campaign,
    o.payment_type,
    o.order_total,
    o.order_status,
    o.is_cancelled,
    o.is_refunded,
    o.is_pending,
    o.n_items,
    COALESCE(r.refund_amount, 0) AS refund_amount,
    r.refund_date,
    r.reason AS refund_reason,
    p.amount AS payment_amount,
    p.payment_status
FROM orders o
LEFT JOIN refunds r ON o.order_id = r.order_id
LEFT JOIN payments p ON o.order_id = p.order_id;

-- Daily KPI summary across all dimensions
CREATE OR REPLACE VIEW daily_kpi_summary AS
SELECT
    order_date,
    COUNT(*) AS total_orders,
    COUNT(*) FILTER (WHERE NOT is_cancelled AND NOT is_pending) AS paid_orders,
    SUM(order_total) FILTER (WHERE NOT is_cancelled AND NOT is_pending) AS gmv,
    AVG(order_total) FILTER (WHERE NOT is_cancelled AND NOT is_pending) AS aov,
    SUM(CASE WHEN is_refunded THEN 1 ELSE 0 END)::DOUBLE / NULLIF(COUNT(*), 0) AS refund_rate,
    SUM(CASE WHEN is_cancelled THEN 1 ELSE 0 END)::DOUBLE / NULLIF(COUNT(*), 0) AS cancellation_rate,
    SUM(CASE WHEN customer_type = 'new' THEN 1 ELSE 0 END)::DOUBLE / NULLIF(COUNT(*), 0) AS new_customer_ratio,
    SUM(order_total) FILTER (WHERE NOT is_cancelled AND NOT is_pending) AS revenue
FROM orders
GROUP BY order_date
ORDER BY order_date;

-- Refund summary by date and category
CREATE OR REPLACE VIEW refund_summary AS
SELECT
    r.refund_date,
    o.category,
    o.region,
    o.channel,
    COUNT(*) AS refund_count,
    SUM(r.refund_amount) AS total_refund_amount,
    AVG(r.refund_amount) AS avg_refund_amount,
    r.reason AS refund_reason
FROM refunds r
JOIN orders o ON r.order_id = o.order_id
GROUP BY r.refund_date, o.category, o.region, o.channel, r.reason
ORDER BY r.refund_date;

-- Channel performance summary
CREATE OR REPLACE VIEW channel_performance AS
SELECT
    order_date,
    channel,
    COUNT(*) AS total_orders,
    COUNT(*) FILTER (WHERE NOT is_cancelled AND NOT is_pending) AS paid_orders,
    SUM(order_total) FILTER (WHERE NOT is_cancelled AND NOT is_pending) AS gmv,
    AVG(order_total) FILTER (WHERE NOT is_cancelled AND NOT is_pending) AS aov,
    SUM(CASE WHEN is_refunded THEN 1 ELSE 0 END)::DOUBLE / NULLIF(COUNT(*), 0) AS refund_rate,
    SUM(CASE WHEN customer_type = 'new' THEN 1 ELSE 0 END)::DOUBLE / NULLIF(COUNT(*), 0) AS new_customer_ratio
FROM orders
GROUP BY order_date, channel
ORDER BY order_date, channel;

-- Category performance summary
CREATE OR REPLACE VIEW category_performance AS
SELECT
    order_date,
    category,
    COUNT(*) AS total_orders,
    COUNT(*) FILTER (WHERE NOT is_cancelled AND NOT is_pending) AS paid_orders,
    SUM(order_total) FILTER (WHERE NOT is_cancelled AND NOT is_pending) AS gmv,
    AVG(order_total) FILTER (WHERE NOT is_cancelled AND NOT is_pending) AS aov,
    SUM(CASE WHEN is_refunded THEN 1 ELSE 0 END)::DOUBLE / NULLIF(COUNT(*), 0) AS refund_rate,
    SUM(CASE WHEN is_cancelled THEN 1 ELSE 0 END)::DOUBLE / NULLIF(COUNT(*), 0) AS cancellation_rate
FROM orders
GROUP BY order_date, category
ORDER BY order_date, category;

-- Region performance summary
CREATE OR REPLACE VIEW region_performance AS
SELECT
    order_date,
    region,
    COUNT(*) AS total_orders,
    COUNT(*) FILTER (WHERE NOT is_cancelled AND NOT is_pending) AS paid_orders,
    SUM(order_total) FILTER (WHERE NOT is_cancelled AND NOT is_pending) AS gmv,
    AVG(order_total) FILTER (WHERE NOT is_cancelled AND NOT is_pending) AS aov,
    SUM(CASE WHEN is_refunded THEN 1 ELSE 0 END)::DOUBLE / NULLIF(COUNT(*), 0) AS refund_rate
FROM orders
GROUP BY order_date, region
ORDER BY order_date, region;

-- Customer segment performance
CREATE OR REPLACE VIEW customer_segment_performance AS
SELECT
    order_date,
    customer_type,
    channel,
    COUNT(*) AS total_orders,
    COUNT(*) FILTER (WHERE NOT is_cancelled AND NOT is_pending) AS paid_orders,
    SUM(order_total) FILTER (WHERE NOT is_cancelled AND NOT is_pending) AS gmv,
    AVG(order_total) FILTER (WHERE NOT is_cancelled AND NOT is_pending) AS aov
FROM orders
GROUP BY order_date, customer_type, channel
ORDER BY order_date, customer_type, channel;
