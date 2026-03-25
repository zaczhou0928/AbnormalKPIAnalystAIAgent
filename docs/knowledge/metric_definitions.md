# Metric Definitions

## GMV (Gross Merchandise Value)
- **Definition**: Total dollar value of all paid, non-cancelled, non-pending orders.
- **Formula**: `SUM(order_total) WHERE order_status NOT IN ('cancelled', 'pending')`
- **Granularity**: Daily, aggregable to weekly/monthly.
- **Caveats**:
  - GMV excludes cancelled and pending orders.
  - Refunded orders ARE included in GMV (refund is a separate metric).
  - GMV does not equal revenue after refunds.
  - A metric definition change in August 2025 began excluding pending orders. This can cause an apparent GMV drop that is not a real demand change.

## Order Count
- **Definition**: Number of orders placed.
- **Variants**:
  - `total_orders`: All orders including cancelled.
  - `paid_orders`: Excludes cancelled and pending.
- **Caveats**:
  - Total orders includes cancelled — use paid_orders for demand metrics.
  - One customer may place multiple orders per day.

## AOV (Average Order Value)
- **Definition**: Average order total for paid, non-cancelled orders.
- **Formula**: `GMV / paid_orders`
- **Caveats**:
  - AOV is sensitive to mix shifts — a surge of low-value category orders can drop AOV without any pricing change.
  - Outlier high-value orders can skew AOV; median may be more robust.
  - Compare AOV within categories for cleaner signal.

## Conversion Rate
- **Definition**: Proportion of sessions that result in an order.
- **Approximation**: In this dataset, conversion rate is approximated at the channel level. A drop in conversion with stable traffic indicates funnel issues.
- **Caveats**:
  - We do not have raw session data; conversion is inferred from order patterns.
  - Channel-level conversion changes are the primary signal.

## Refund Rate
- **Definition**: Proportion of orders that were refunded.
- **Formula**: `COUNT(refunded orders) / COUNT(all orders)`
- **Caveats**:
  - Refunds may occur days or weeks after order placement. The refund_date is when the refund was processed, not the order date.
  - Partial refunds exist — refund_amount may be less than order_total.
  - Category-level refund rates vary significantly (apparel ~12%, books ~3%).

## Cancellation Rate
- **Definition**: Proportion of orders cancelled before fulfillment.
- **Formula**: `COUNT(cancelled orders) / COUNT(all orders)`
- **Caveats**:
  - Cancellations happen pre-shipment; refunds happen post-delivery.
  - A spike in cancellations may indicate payment issues, price concerns, or shipping delay announcements.

## New Customer Ratio
- **Definition**: Proportion of orders placed by first-time customers.
- **Formula**: `COUNT(orders WHERE customer_type='new') / COUNT(all orders)`
- **Caveats**:
  - "New" is defined at time of order placement based on customer_type field.
  - A channel-specific new customer surge (e.g., viral social campaign) will raise the overall ratio.
  - Returning customers have higher AOV on average.

## Revenue
- **Definition**: Same as GMV in this dataset (total paid order value before refunds).
- **Caveats**: Net revenue (after refunds) = GMV - total refund amount. This dataset uses GMV and revenue interchangeably.
