# Data Dictionary

## Tables

### orders
Primary fact table for all e-commerce orders.

| Column | Type | Description |
|--------|------|-------------|
| order_id | INTEGER | Unique order identifier |
| order_date | DATE | Date the order was placed |
| customer_id | INTEGER | Foreign key to customers |
| customer_type | VARCHAR | 'new' or 'returning' |
| channel | VARCHAR | Acquisition channel |
| region | VARCHAR | Customer region |
| category | VARCHAR | Primary product category |
| campaign | VARCHAR | Active campaign at time of order |
| payment_type | VARCHAR | Payment method used |
| order_total | DOUBLE | Total order value in USD |
| order_status | VARCHAR | 'completed', 'cancelled', 'refunded', 'pending' |
| is_cancelled | BOOLEAN | True if order was cancelled |
| is_refunded | BOOLEAN | True if order was refunded |
| is_pending | BOOLEAN | True if order is pending |
| n_items | INTEGER | Number of line items |

### order_items
Line items within each order.

| Column | Type | Description |
|--------|------|-------------|
| item_id | INTEGER | Unique item identifier |
| order_id | INTEGER | Foreign key to orders |
| product_id | INTEGER | Foreign key to products |
| quantity | INTEGER | Quantity ordered |
| unit_price | DOUBLE | Price per unit |
| line_total | DOUBLE | quantity × unit_price |

### products
Product catalog.

| Column | Type | Description |
|--------|------|-------------|
| product_id | INTEGER | Unique product identifier |
| product_name | VARCHAR | Product name |
| category | VARCHAR | Product category |
| base_price | DOUBLE | Base price in USD |

### customers
Customer records.

| Column | Type | Description |
|--------|------|-------------|
| customer_id | INTEGER | Unique customer identifier |
| signup_date | DATE | Account creation date |
| region | VARCHAR | Customer home region |
| acquisition_channel | VARCHAR | How the customer was first acquired |

### payments
Payment records for each order.

| Column | Type | Description |
|--------|------|-------------|
| payment_id | INTEGER | Unique payment identifier |
| order_id | INTEGER | Foreign key to orders |
| payment_type | VARCHAR | Payment method |
| amount | DOUBLE | Payment amount |
| payment_status | VARCHAR | 'completed', 'failed', 'pending' |
| payment_date | DATE | Date of payment |

### refunds
Refund records for refunded orders.

| Column | Type | Description |
|--------|------|-------------|
| refund_id | INTEGER | Unique refund identifier |
| order_id | INTEGER | Foreign key to orders |
| refund_date | DATE | Date refund was processed |
| refund_amount | DOUBLE | Amount refunded |
| reason | VARCHAR | Refund reason |

## Curated Views

### fact_orders_enriched
Denormalized view joining orders with refunds and payments. One row per order. Use this for most dimensional analyses.

### daily_kpi_summary
Daily aggregate KPIs: total_orders, paid_orders, gmv, aov, refund_rate, cancellation_rate, new_customer_ratio, revenue.

### channel_performance
Daily KPIs broken down by channel.

### category_performance
Daily KPIs broken down by product category.

### region_performance
Daily KPIs broken down by region.

### customer_segment_performance
Daily KPIs broken down by customer_type and channel.

### refund_summary
Refund counts and amounts by date, category, region, channel, and reason.

## Dimension Values

- **Channels**: organic_search, paid_search, social, email, direct, affiliate
- **Regions**: North America, Europe, Asia Pacific, Latin America
- **Categories**: electronics, apparel, home_garden, beauty, sports, books
- **Customer Types**: new, returning
- **Payment Types**: credit_card, debit_card, paypal, apple_pay, bank_transfer
- **Campaigns**: none, spring_sale, summer_blast, back_to_school, fall_promo, black_friday, holiday_special
- **Order Statuses**: completed, cancelled, refunded, pending
- **Refund Reasons**: defective, wrong_item, not_as_described, changed_mind
