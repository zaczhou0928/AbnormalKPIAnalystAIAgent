# Business Rules

## Order Processing
- An order is considered "paid" if `order_status` is `completed` or `refunded` (not `cancelled` or `pending`).
- Cancelled orders should be excluded from GMV, AOV, and revenue calculations.
- Pending orders were historically included in GMV but excluded starting August 2025 (metric definition change).

## Campaign Rules
- Only one campaign is active at a time per order.
- Campaign attribution is based on the campaign active at time of purchase.
- "none" means no campaign was active.
- Campaign calendar:
  - Spring Sale: March–April
  - Summer Blast: June–July
  - Back to School: August
  - Fall Promo: September–October
  - Black Friday: November 20–30
  - Holiday Special: December

## Channel Attribution
- Channel represents the acquisition channel for the session that led to the order.
- Channel names: `organic_search`, `paid_search`, `social`, `email`, `direct`, `affiliate`.
- A channel-level metric change (e.g., conversion drop in paid_search only) typically indicates a channel-specific issue, not a sitewide problem.
- Affiliate channel metrics can be volatile due to influencer-driven spikes.

## Region Rules
- Four regions: North America, Europe, Asia Pacific, Latin America.
- Currency is normalized to USD.
- Regional pricing differences exist but are not tracked in this dataset.
- APAC may have different seasonal patterns than NA/EU.

## Refund Policy
- Refunds can be requested up to 14 days after delivery.
- Refund processing takes 1–14 days.
- Partial refunds are possible (refund_amount ≤ order_total).
- Common refund reasons: defective, wrong_item, not_as_described, changed_mind.

## Customer Classification
- `new`: First order ever from this customer.
- `returning`: Customer has ordered before.
- New customer acquisition cost is higher; new customer ratio is a health metric.
- Returning customers typically have 15–25% higher AOV.

## Payment Types
- Supported: credit_card, debit_card, paypal, apple_pay, bank_transfer.
- Credit card is the dominant payment method (~40% of orders).
- A payment gateway outage will primarily affect one payment type and may show as order count drop.
- Some customers switch payment types during outages (e.g., credit_card → paypal).

## Investigation Guidelines
- Always compare anomaly window against a baseline window of similar length.
- Prefer 2-week baseline windows ending just before the anomaly period.
- When analyzing dimensional breakdowns, check at least: channel, region, category.
- If the movement is concentrated in a single dimension slice, the root cause is likely dimension-specific.
- If the movement is spread across all slices, the root cause is likely sitewide (checkout bug, definition change, etc.).
- Small sample sizes (< 30 orders in a slice) should be flagged as low-confidence.
