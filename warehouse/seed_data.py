"""Synthetic e-commerce data generator with realistic anomaly injection.

Generates deterministic, reproducible data for 12 months covering:
- orders, order_items, products, customers, payments
- channels, regions, campaigns
- refunds/cancellations
- daily KPI aggregates

Uses fixed random seeds for full reproducibility.
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from agentic_kpi_analyst.logging_utils import get_logger

logger = get_logger(__name__)

SEED = 42
START_DATE = date(2025, 1, 1)
END_DATE = date(2025, 12, 31)
BASE_DAILY_ORDERS = 250

# --- Dimension values ---

REGIONS = ["North America", "Europe", "Asia Pacific", "Latin America"]
REGION_WEIGHTS = [0.40, 0.30, 0.20, 0.10]

CHANNELS = ["organic_search", "paid_search", "social", "email", "direct", "affiliate"]
CHANNEL_WEIGHTS = [0.25, 0.20, 0.18, 0.15, 0.12, 0.10]

CATEGORIES = ["electronics", "apparel", "home_garden", "beauty", "sports", "books"]
CATEGORY_WEIGHTS = [0.30, 0.22, 0.18, 0.13, 0.10, 0.07]

CUSTOMER_TYPES = ["new", "returning"]
CUSTOMER_TYPE_WEIGHTS = [0.35, 0.65]

PAYMENT_TYPES = ["credit_card", "debit_card", "paypal", "apple_pay", "bank_transfer"]
PAYMENT_TYPE_WEIGHTS = [0.40, 0.20, 0.20, 0.12, 0.08]

CAMPAIGNS = [
    "none", "spring_sale", "summer_blast", "back_to_school",
    "fall_promo", "black_friday", "holiday_special",
]

# Price ranges by category (min, max)
CATEGORY_PRICE_RANGES: dict[str, tuple[float, float]] = {
    "electronics": (29.99, 899.99),
    "apparel": (14.99, 199.99),
    "home_garden": (9.99, 349.99),
    "beauty": (7.99, 129.99),
    "sports": (12.99, 299.99),
    "books": (5.99, 49.99),
}

# Base refund rates by category
CATEGORY_REFUND_RATES: dict[str, float] = {
    "electronics": 0.08,
    "apparel": 0.12,
    "home_garden": 0.06,
    "beauty": 0.05,
    "sports": 0.07,
    "books": 0.03,
}


def _get_active_campaign(d: date) -> str:
    """Return the active campaign for a given date."""
    month = d.month
    if month in (3, 4):
        return "spring_sale"
    elif month in (6, 7):
        return "summer_blast"
    elif month == 8:
        return "back_to_school"
    elif month in (9, 10):
        return "fall_promo"
    elif month == 11 and d.day >= 20:
        return "black_friday"
    elif month == 12:
        return "holiday_special"
    return "none"


def _seasonal_multiplier(d: date) -> float:
    """Seasonal order volume multiplier."""
    month = d.month
    day_of_week = d.weekday()
    base = 1.0

    # Monthly seasonality
    monthly = {
        1: 0.85, 2: 0.88, 3: 0.95, 4: 1.0, 5: 1.02, 6: 1.05,
        7: 1.0, 8: 0.98, 9: 1.05, 10: 1.08, 11: 1.25, 12: 1.40,
    }
    base *= monthly.get(month, 1.0)

    # Weekend dip
    if day_of_week >= 5:
        base *= 0.82

    return base


class AnomalyInjector:
    """Injects realistic anomalies into generated data."""

    def __init__(self, rng: np.random.Generator) -> None:
        self.rng = rng
        self.cases: list[dict[str, Any]] = []
        self._define_cases()

    def _define_cases(self) -> None:
        """Define the 15+ labeled anomaly cases."""
        self.cases = [
            {
                "case_id": "ANO-001",
                "kpi_name": "conversion_rate",
                "affected_start_date": "2025-03-10",
                "affected_end_date": "2025-03-16",
                "description": "Conversion rate dropped 30% in paid_search channel",
                "expected_primary_cause": "paid_search channel landing page broke, dropping conversion",
                "expected_secondary_factors": ["slight overall traffic increase masked the issue"],
                "recommended_dimensions": ["channel"],
                "should_trigger_human_review": False,
                "injection": {"type": "channel_conversion_drop", "channel": "paid_search", "factor": 0.7},
            },
            {
                "case_id": "ANO-002",
                "kpi_name": "refund_rate",
                "affected_start_date": "2025-04-05",
                "affected_end_date": "2025-04-18",
                "description": "Refund rate spiked 3x in electronics category",
                "expected_primary_cause": "Defective batch of electronics products caused mass refunds",
                "expected_secondary_factors": ["some spillover into apparel returns"],
                "recommended_dimensions": ["category"],
                "should_trigger_human_review": True,
                "injection": {"type": "category_refund_spike", "category": "electronics", "factor": 3.0},
            },
            {
                "case_id": "ANO-003",
                "kpi_name": "aov",
                "affected_start_date": "2025-05-01",
                "affected_end_date": "2025-05-14",
                "description": "AOV dropped 20% in Asia Pacific region",
                "expected_primary_cause": "Promotional pricing error in APAC region discounted high-value items",
                "expected_secondary_factors": [],
                "recommended_dimensions": ["region"],
                "should_trigger_human_review": False,
                "injection": {"type": "region_aov_drop", "region": "Asia Pacific", "factor": 0.8},
            },
            {
                "case_id": "ANO-004",
                "kpi_name": "gmv",
                "affected_start_date": "2025-06-15",
                "affected_end_date": "2025-06-28",
                "description": "GMV spiked 40% due to campaign mix shift",
                "expected_primary_cause": "Summer campaign drove outsized electronics purchases",
                "expected_secondary_factors": ["affiliate channel saw 2x traffic from influencer push"],
                "recommended_dimensions": ["campaign", "channel", "category"],
                "should_trigger_human_review": False,
                "injection": {"type": "campaign_gmv_spike", "campaign": "summer_blast", "factor": 1.4},
            },
            {
                "case_id": "ANO-005",
                "kpi_name": "order_count",
                "affected_start_date": "2025-07-10",
                "affected_end_date": "2025-07-17",
                "description": "Order count dropped 45% due to payment processing failure",
                "expected_primary_cause": "Credit card payment gateway outage blocked 40% of transactions",
                "expected_secondary_factors": ["some customers switched to PayPal"],
                "recommended_dimensions": ["payment_type"],
                "should_trigger_human_review": True,
                "injection": {"type": "payment_failure", "payment_type": "credit_card", "factor": 0.55},
            },
            {
                "case_id": "ANO-006",
                "kpi_name": "gmv",
                "affected_start_date": "2025-08-01",
                "affected_end_date": "2025-08-07",
                "description": "GMV appeared to drop but was actually a metric definition change",
                "expected_primary_cause": "New accounting rule excluded pending orders from GMV calculation",
                "expected_secondary_factors": ["no real demand change occurred"],
                "recommended_dimensions": ["category", "region"],
                "should_trigger_human_review": True,
                "injection": {"type": "definition_edge_case", "factor": 0.85},
            },
            {
                "case_id": "ANO-007",
                "kpi_name": "new_customer_ratio",
                "affected_start_date": "2025-02-14",
                "affected_end_date": "2025-02-21",
                "description": "New customer ratio jumped from 35% to 55%",
                "expected_primary_cause": "Viral social media campaign brought influx of first-time buyers",
                "expected_secondary_factors": ["Valentine's Day seasonal effect"],
                "recommended_dimensions": ["channel", "customer_type"],
                "should_trigger_human_review": False,
                "injection": {"type": "new_customer_surge", "channel": "social", "factor": 2.5},
            },
            {
                "case_id": "ANO-008",
                "kpi_name": "revenue",
                "affected_start_date": "2025-09-01",
                "affected_end_date": "2025-09-14",
                "description": "Revenue dropped 25% in North America",
                "expected_primary_cause": "Competitor launched aggressive pricing in North America",
                "expected_secondary_factors": ["organic search traffic declined 15%"],
                "recommended_dimensions": ["region", "channel"],
                "should_trigger_human_review": False,
                "injection": {"type": "region_revenue_drop", "region": "North America", "factor": 0.75},
            },
            {
                "case_id": "ANO-009",
                "kpi_name": "cancellation_rate",
                "affected_start_date": "2025-10-05",
                "affected_end_date": "2025-10-12",
                "description": "Cancellation rate doubled across all categories",
                "expected_primary_cause": "Shipping delay announcement caused pre-emptive cancellations",
                "expected_secondary_factors": ["electronics and home_garden most affected"],
                "recommended_dimensions": ["category"],
                "should_trigger_human_review": True,
                "injection": {"type": "cancellation_spike", "factor": 2.0},
            },
            {
                "case_id": "ANO-010",
                "kpi_name": "aov",
                "affected_start_date": "2025-11-20",
                "affected_end_date": "2025-11-30",
                "description": "AOV spiked 60% during Black Friday",
                "expected_primary_cause": "Black Friday bundle deals increased average cart size, especially electronics",
                "expected_secondary_factors": ["returning customers bought higher-value bundles"],
                "recommended_dimensions": ["campaign", "category", "customer_type"],
                "should_trigger_human_review": False,
                "injection": {"type": "aov_spike_campaign", "campaign": "black_friday", "factor": 1.6},
            },
            {
                "case_id": "ANO-011",
                "kpi_name": "order_count",
                "affected_start_date": "2025-03-25",
                "affected_end_date": "2025-03-31",
                "description": "Order count dropped 20% in email channel only",
                "expected_primary_cause": "Email delivery issue — emails landing in spam for major ISPs",
                "expected_secondary_factors": [],
                "recommended_dimensions": ["channel"],
                "should_trigger_human_review": False,
                "injection": {"type": "channel_order_drop", "channel": "email", "factor": 0.8},
            },
            {
                "case_id": "ANO-012",
                "kpi_name": "refund_rate",
                "affected_start_date": "2025-05-20",
                "affected_end_date": "2025-06-02",
                "description": "Refund rate spiked in apparel category in Europe",
                "expected_primary_cause": "Size chart error on European apparel listings caused wrong-size orders",
                "expected_secondary_factors": ["affected new customers more than returning"],
                "recommended_dimensions": ["category", "region", "customer_type"],
                "should_trigger_human_review": False,
                "injection": {"type": "category_region_refund", "category": "apparel", "region": "Europe", "factor": 2.5},
            },
            {
                "case_id": "ANO-013",
                "kpi_name": "gmv",
                "affected_start_date": "2025-12-10",
                "affected_end_date": "2025-12-20",
                "description": "GMV 50% above forecast during holiday season",
                "expected_primary_cause": "Holiday special campaign combined with organic viral moment",
                "expected_secondary_factors": ["electronics and beauty categories led the surge"],
                "recommended_dimensions": ["campaign", "category", "channel"],
                "should_trigger_human_review": False,
                "injection": {"type": "holiday_gmv_surge", "factor": 1.5},
            },
            {
                "case_id": "ANO-014",
                "kpi_name": "conversion_rate",
                "affected_start_date": "2025-08-15",
                "affected_end_date": "2025-08-22",
                "description": "Conversion rate dropped across all channels by 15%",
                "expected_primary_cause": "Site-wide checkout flow regression after deploy",
                "expected_secondary_factors": ["mobile users more affected than desktop"],
                "recommended_dimensions": ["channel"],
                "should_trigger_human_review": True,
                "injection": {"type": "sitewide_conversion_drop", "factor": 0.85},
            },
            {
                "case_id": "ANO-015",
                "kpi_name": "order_count",
                "affected_start_date": "2025-11-01",
                "affected_end_date": "2025-11-07",
                "description": "Order count from affiliate channel surged 3x",
                "expected_primary_cause": "Major influencer partnership launched driving affiliate traffic",
                "expected_secondary_factors": ["mostly new customers in beauty and apparel"],
                "recommended_dimensions": ["channel", "category", "customer_type"],
                "should_trigger_human_review": False,
                "injection": {"type": "channel_order_surge", "channel": "affiliate", "factor": 3.0},
            },
            {
                "case_id": "ANO-016",
                "kpi_name": "aov",
                "affected_start_date": "2025-04-20",
                "affected_end_date": "2025-04-27",
                "description": "AOV dropped 15% due to books category promotion flooding orders",
                "expected_primary_cause": "Free book promotion drove high volume of low-value orders, diluting AOV",
                "expected_secondary_factors": ["books category share of orders jumped from 7% to 25%"],
                "recommended_dimensions": ["category"],
                "should_trigger_human_review": False,
                "injection": {"type": "category_mix_shift", "category": "books", "volume_factor": 4.0, "price_factor": 0.5},
            },
        ]

    def get_labeled_cases(self) -> list[dict[str, Any]]:
        """Return labeled cases without injection details (for evaluation)."""
        return [
            {k: v for k, v in c.items() if k != "injection"}
            for c in self.cases
        ]

    def should_modify_order(
        self,
        d: date,
        channel: str,
        category: str,
        region: str,
        payment_type: str,
        customer_type: str,
        campaign: str,
    ) -> dict[str, Any]:
        """Check if any anomaly applies and return modification instructions."""
        mods: dict[str, Any] = {}

        for case in self.cases:
            inj = case["injection"]
            start = date.fromisoformat(case["affected_start_date"])
            end = date.fromisoformat(case["affected_end_date"])

            if not (start <= d <= end):
                continue

            itype = inj["type"]

            if itype == "channel_conversion_drop" and channel == inj["channel"]:
                mods["drop_probability"] = 1.0 - inj["factor"]

            elif itype == "category_refund_spike" and category == inj["category"]:
                mods["refund_rate_multiplier"] = inj["factor"]

            elif itype == "region_aov_drop" and region == inj["region"]:
                mods["price_multiplier"] = inj["factor"]

            elif itype == "campaign_gmv_spike" and campaign == inj.get("campaign"):
                mods["price_multiplier"] = mods.get("price_multiplier", 1.0) * inj["factor"]

            elif itype == "payment_failure" and payment_type == inj["payment_type"]:
                mods["drop_probability"] = 1.0 - inj["factor"]

            elif itype == "definition_edge_case":
                mods["mark_pending"] = True
                mods["pending_rate"] = 1.0 - inj["factor"]

            elif itype == "new_customer_surge" and channel == inj["channel"]:
                mods["force_new_customer"] = True
                mods["volume_multiplier"] = inj["factor"]

            elif itype == "region_revenue_drop" and region == inj["region"]:
                mods["price_multiplier"] = mods.get("price_multiplier", 1.0) * inj["factor"]

            elif itype == "cancellation_spike":
                mods["cancellation_rate_multiplier"] = inj["factor"]

            elif itype == "aov_spike_campaign" and campaign == inj.get("campaign"):
                mods["price_multiplier"] = mods.get("price_multiplier", 1.0) * inj["factor"]

            elif itype == "channel_order_drop" and channel == inj["channel"]:
                mods["drop_probability"] = max(
                    mods.get("drop_probability", 0), 1.0 - inj["factor"]
                )

            elif itype == "category_region_refund" and category == inj["category"] and region == inj["region"]:
                mods["refund_rate_multiplier"] = inj["factor"]

            elif itype == "holiday_gmv_surge":
                mods["price_multiplier"] = mods.get("price_multiplier", 1.0) * inj["factor"]
                mods["volume_multiplier"] = mods.get("volume_multiplier", 1.0) * 1.2

            elif itype == "sitewide_conversion_drop":
                mods["drop_probability"] = max(
                    mods.get("drop_probability", 0), 1.0 - inj["factor"]
                )

            elif itype == "channel_order_surge" and channel == inj["channel"]:
                mods["volume_multiplier"] = mods.get("volume_multiplier", 1.0) * inj["factor"]

            elif itype == "category_mix_shift" and category == inj["category"]:
                mods["volume_multiplier"] = mods.get("volume_multiplier", 1.0) * inj["volume_factor"]
                mods["price_multiplier"] = mods.get("price_multiplier", 1.0) * inj["price_factor"]

        return mods


def generate_all(output_dir: str | Path, seed: int = SEED) -> dict[str, pd.DataFrame]:
    """Generate all synthetic data tables and save to parquet.

    Returns dict of table_name -> DataFrame.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    rng = np.random.default_rng(seed)
    injector = AnomalyInjector(rng)

    logger.info("generating_synthetic_data", output_dir=str(output_dir))

    # --- Static dimension tables ---
    regions_df = pd.DataFrame({
        "region_id": range(1, len(REGIONS) + 1),
        "region_name": REGIONS,
    })

    channels_df = pd.DataFrame({
        "channel_id": range(1, len(CHANNELS) + 1),
        "channel_name": CHANNELS,
    })

    categories = CATEGORIES
    products = []
    product_id = 1
    for cat in categories:
        pmin, pmax = CATEGORY_PRICE_RANGES[cat]
        n_products = rng.integers(15, 30)
        for i in range(n_products):
            products.append({
                "product_id": product_id,
                "product_name": f"{cat}_{i+1:03d}",
                "category": cat,
                "base_price": round(float(rng.uniform(pmin, pmax)), 2),
            })
            product_id += 1
    products_df = pd.DataFrame(products)

    campaigns_df = pd.DataFrame({
        "campaign_id": range(1, len(CAMPAIGNS) + 1),
        "campaign_name": CAMPAIGNS,
    })

    # --- Generate customers ---
    n_customers = 25_000
    customers = []
    for cid in range(1, n_customers + 1):
        signup_offset = int(rng.integers(0, 365))
        signup_date = START_DATE + timedelta(days=signup_offset)
        customers.append({
            "customer_id": cid,
            "signup_date": signup_date,
            "region": rng.choice(REGIONS, p=REGION_WEIGHTS),
            "acquisition_channel": rng.choice(CHANNELS, p=CHANNEL_WEIGHTS),
        })
    customers_df = pd.DataFrame(customers)

    # --- Generate orders day by day ---
    all_orders = []
    all_order_items = []
    all_payments = []
    all_refunds = []
    order_id = 1
    item_id = 1
    payment_id = 1
    refund_id = 1

    current = START_DATE
    while current <= END_DATE:
        seasonal = _seasonal_multiplier(current)
        campaign = _get_active_campaign(current)
        n_orders_today = int(BASE_DAILY_ORDERS * seasonal)
        # Small daily noise
        n_orders_today = max(50, int(n_orders_today * rng.normal(1.0, 0.05)))

        for _ in range(n_orders_today):
            channel = str(rng.choice(CHANNELS, p=CHANNEL_WEIGHTS))
            region = str(rng.choice(REGIONS, p=REGION_WEIGHTS))
            category = str(rng.choice(CATEGORIES, p=CATEGORY_WEIGHTS))
            customer_type = str(rng.choice(CUSTOMER_TYPES, p=CUSTOMER_TYPE_WEIGHTS))
            payment_type = str(rng.choice(PAYMENT_TYPES, p=PAYMENT_TYPE_WEIGHTS))

            # Check anomaly modifications
            mods = injector.should_modify_order(
                current, channel, category, region, payment_type, customer_type, campaign,
            )

            # Drop orders (conversion/payment failure)
            if "drop_probability" in mods:
                if rng.random() < mods["drop_probability"]:
                    continue

            # Volume multiplier (generate extra orders)
            vol_mult = mods.get("volume_multiplier", 1.0)
            n_copies = 1
            if vol_mult > 1.0:
                # Probabilistically create extra copies
                n_copies = int(vol_mult) if rng.random() < (vol_mult - int(vol_mult)) else max(1, int(vol_mult))
                if n_copies < 1:
                    n_copies = 1

            if "force_new_customer" in mods:
                customer_type = "new"

            for _ in range(n_copies):
                customer_id = int(rng.integers(1, n_customers + 1))

                # Determine number of items (1-4)
                n_items = int(rng.choice([1, 1, 1, 2, 2, 3, 4]))

                order_total = 0.0
                is_cancelled = False
                is_refunded = False
                is_pending = False

                items_for_order = []
                # Pick products from the order's primary category (mostly)
                cat_products = products_df[products_df["category"] == category]
                if len(cat_products) == 0:
                    cat_products = products_df

                for item_idx in range(n_items):
                    prod = cat_products.sample(1, random_state=int(rng.integers(0, 2**31)))
                    price = float(prod["base_price"].values[0])

                    # Apply price multiplier from anomalies
                    price *= mods.get("price_multiplier", 1.0)

                    # Small random price variation
                    price *= float(rng.normal(1.0, 0.05))
                    price = round(max(1.0, price), 2)
                    qty = int(rng.choice([1, 1, 1, 2, 2, 3]))

                    items_for_order.append({
                        "item_id": item_id,
                        "order_id": order_id,
                        "product_id": int(prod["product_id"].values[0]),
                        "quantity": qty,
                        "unit_price": price,
                        "line_total": round(price * qty, 2),
                    })
                    order_total += price * qty
                    item_id += 1

                order_total = round(order_total, 2)

                # Cancellation logic
                cancel_rate = 0.04
                if "cancellation_rate_multiplier" in mods:
                    cancel_rate *= mods["cancellation_rate_multiplier"]
                if rng.random() < cancel_rate:
                    is_cancelled = True

                # Pending/definition edge case
                if "mark_pending" in mods:
                    if rng.random() < mods.get("pending_rate", 0.15):
                        is_pending = True

                # Refund logic
                base_refund_rate = CATEGORY_REFUND_RATES.get(category, 0.05)
                if "refund_rate_multiplier" in mods:
                    base_refund_rate *= mods["refund_rate_multiplier"]
                if not is_cancelled and rng.random() < base_refund_rate:
                    is_refunded = True

                order_status = "completed"
                if is_cancelled:
                    order_status = "cancelled"
                elif is_pending:
                    order_status = "pending"
                elif is_refunded:
                    order_status = "refunded"

                all_orders.append({
                    "order_id": order_id,
                    "order_date": current,
                    "customer_id": customer_id,
                    "customer_type": customer_type,
                    "channel": channel,
                    "region": region,
                    "category": category,
                    "campaign": campaign,
                    "payment_type": payment_type,
                    "order_total": order_total,
                    "order_status": order_status,
                    "is_cancelled": is_cancelled,
                    "is_refunded": is_refunded,
                    "is_pending": is_pending,
                    "n_items": n_items,
                })

                all_order_items.extend(items_for_order)

                # Payment
                all_payments.append({
                    "payment_id": payment_id,
                    "order_id": order_id,
                    "payment_type": payment_type,
                    "amount": order_total,
                    "payment_status": "failed" if is_cancelled else ("pending" if is_pending else "completed"),
                    "payment_date": current,
                })
                payment_id += 1

                # Refund record
                if is_refunded:
                    refund_days = int(rng.integers(1, 15))
                    all_refunds.append({
                        "refund_id": refund_id,
                        "order_id": order_id,
                        "refund_date": current + timedelta(days=refund_days),
                        "refund_amount": round(order_total * float(rng.uniform(0.5, 1.0)), 2),
                        "reason": str(rng.choice(["defective", "wrong_item", "not_as_described", "changed_mind"])),
                    })
                    refund_id += 1

                order_id += 1

        current += timedelta(days=1)

    # Build DataFrames
    orders_df = pd.DataFrame(all_orders)
    order_items_df = pd.DataFrame(all_order_items)
    payments_df = pd.DataFrame(all_payments)
    refunds_df = pd.DataFrame(all_refunds)

    # Save all tables
    tables = {
        "orders": orders_df,
        "order_items": order_items_df,
        "products": products_df,
        "customers": customers_df,
        "payments": payments_df,
        "channels": channels_df,
        "regions": regions_df,
        "campaigns": campaigns_df,
        "refunds": refunds_df,
    }

    for name, df in tables.items():
        path = output_dir / f"{name}.parquet"
        df.to_parquet(path, index=False)
        logger.info("saved_table", table=name, rows=len(df), path=str(path))

    # Save labeled anomaly cases
    cases = injector.get_labeled_cases()
    cases_path = output_dir / "anomaly_cases.json"
    with open(cases_path, "w") as f:
        json.dump(cases, f, indent=2, default=str)
    logger.info("saved_anomaly_cases", n_cases=len(cases), path=str(cases_path))

    return tables


if __name__ == "__main__":
    from agentic_kpi_analyst.logging_utils import setup_logging
    setup_logging("INFO")
    generate_all("data/generated")
