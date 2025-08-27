#!/usr/bin/env python3
"""
seed_data.py

Generates realistic fake retail data to CSVs under a local folder (default: sample_data).
Inspired by Databricks' Brickhouse Brands demo structure, but simplified for a single-page app.

Entities:
- stores, products, customers, orders, order_items, inventory_snapshots, promotions

Run:
  python src/seed/seed_data.py --scale small --days 14
"""

from __future__ import annotations
import argparse
import csv
import os
import random
import string
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, date, time, timezone
from math import ceil, sin, pi
from typing import Dict, List, Tuple, Optional

# Import config for default output directory
try:
    from ..config import get_config
except ImportError:
    # Fallback for relative imports or direct execution
    sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
    try:
        from config import get_config
    except ImportError:
        # If config is not available, we'll use hardcoded default
        get_config = None

# -----------------------------
# Config & helper structures
# -----------------------------

REGIONS = ["West", "Central", "East"]
CITIES_BY_REGION = {
    "West": ["Vancouver", "Seattle", "Portland", "San Francisco", "San Jose", "Calgary"],
    "Central": ["Denver", "Dallas", "Houston", "Chicago", "Minneapolis", "Kansas City"],
    "East": ["New York", "Boston", "Philadelphia", "Toronto", "Montreal", "Ottawa"],
}

CATEGORIES = {
    "Beverages": ["SparkleCo", "H2Only", "BeanWorks", "Leaf&Lime"],
    "Snacks": ["CrunchLabs", "NuttyBuddy", "SweetTreats", "SaltyWave"],
    "Household": ["HomeGuard", "ShinePro", "EcoClean", "FreshNest"],
    "Personal Care": ["GlowCare", "PureForm", "DailyZen", "Wellness+",
    ],
    "Produce": ["GreenFields", "SunValley", "OrchardPrime"],
    "Frozen": ["ArcticBite", "FrostyFarm", "CoolCuisine"],
}

PAYMENT_TYPES = ["card", "cash", "mobile"]

PROMO_TYPES = ["BOGO-lite", "PercentOff", "PriceDrop"]

@dataclass
class Scale:
    stores: int
    products: int
    customers: int
    orders_estimate: int  # over the full window (rough target)

SCALES: Dict[str, Scale] = {
    "small":  Scale(10,   200,   2_000,   4_000),
    "medium": Scale(50,  1_000, 25_000,  75_000),
    "large":  Scale(200, 5_000, 120_000, 500_000),
}


# -----------------------------
# Utility functions
# -----------------------------

def ensure_dir(path: str) -> None:
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)

def rand_sku() -> str:
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=8))

def zipf_like_index(n: int, s: float = 1.15) -> int:
    """
    Return a product index [0, n-1] with a bias toward lower indices (popular items).
    s ~1.0-1.3 controls skew.
    """
    # Invert a Pareto-ish draw to an index
    r = random.random()
    # Normalize using harmonic approximation; simple monotonic mapping:
    idx = int((r ** (1.0 / (1.0 + s))) * n)
    if idx >= n:
        idx = n - 1
    return idx

def diurnal_multiplier(ts: datetime) -> float:
    """
    Smooth midday/evening peaks: combine two sinusoids for ~12:00 and ~18:00.
    Returns ~0.6 to ~1.4 multiplier.
    """
    hour = ts.hour + ts.minute / 60.0
    peak1 = 0.5 * (1 + sin((hour - 12) / 24 * 2 * pi))
    peak2 = 0.5 * (1 + sin((hour - 18) / 24 * 2 * pi))
    base = 0.6 + 0.8 * (0.6 * peak1 + 0.4 * peak2)
    return base

def weekend_multiplier(ts: datetime) -> float:
    return 1.15 if ts.weekday() >= 5 else 1.0  # Sat/Sun uplift

def price_round(p: float) -> float:
    return round(max(p, 0.01), 2)

def random_lat_lon(region: str) -> Tuple[float, float]:
    # very rough bounding boxes
    boxes = {
        "West": (37.0, 49.5, -123.5, -121.0),
        "Central": (32.0, 45.0, -106.0, -93.0),
        "East": (40.0, 46.0, -79.0, -70.0),
    }
    lat_min, lat_max, lon_min, lon_max = boxes[region]
    return (
        round(random.uniform(lat_min, lat_max), 6),
        round(random.uniform(lon_min, lon_max), 6),
    )


# -----------------------------
# Core generators
# -----------------------------

def gen_stores(n: int, start_date: date) -> List[Dict]:
    stores = []
    for i in range(1, n + 1):
        region = random.choice(REGIONS)
        city = random.choice(CITIES_BY_REGION[region])
        lat, lon = random_lat_lon(region)
        opened = start_date - timedelta(days=random.randint(60, 365 * 5))
        stores.append({
            "store_id": i,
            "name": f"Store {i:03d}",
            "region": region,
            "city": city,
            "latitude": lat,
            "longitude": lon,
            "opened_date": opened.isoformat(),
        })
    return stores

def gen_products(n: int) -> List[Dict]:
    products = []
    product_id = 1
    for category, brands in CATEGORIES.items():
        # distribute products roughly evenly across categories
        per_cat = max(1, int(n / len(CATEGORIES)))
        for _ in range(per_cat):
            brand = random.choice(brands)
            sku = rand_sku()
            base_price = price_round(random.uniform(1.0, 30.0) * random.choice([0.99, 0.95, 0.9, 1.0]))
            products.append({
                "product_id": product_id,
                "sku": sku,
                "name": f"{brand} {category} {random.randint(10, 999)}",
                "category": category,
                "brand": brand,
                "base_price": base_price,
            })
            product_id += 1
            if product_id > n:
                return products
    # if rounding leaves gap, fill arbitrarily
    while product_id <= n:
        category = random.choice(list(CATEGORIES.keys()))
        brand = random.choice(CATEGORIES[category])
        products.append({
            "product_id": product_id,
            "sku": rand_sku(),
            "name": f"{brand} {category} {random.randint(10, 999)}",
            "category": category,
            "brand": brand,
            "base_price": price_round(random.uniform(1.0, 30.0)),
        })
        product_id += 1
    return products

def gen_customers(n: int) -> List[Dict]:
    segs = ["casual", "loyal", "bargain", "premium"]
    customers = []
    for i in range(1, n + 1):
        region = random.choice(REGIONS)
        city = random.choice(CITIES_BY_REGION[region])
        signup = (datetime.now(timezone.utc) - timedelta(days=random.randint(30, 365 * 4))).replace(tzinfo=None)
        customers.append({
            "customer_id": i,
            "segment": random.choices(segs, weights=[0.5, 0.2, 0.2, 0.1])[0],
            "signup_ts": signup.isoformat(timespec="seconds"),
            "home_region": region,
            "home_city": city,
        })
    return customers

def gen_promotions(products: List[Dict], start_d: date, end_d: date) -> List[Dict]:
    promos = []
    for p in products:
        # ~25% of products have an active promo window in the period
        if random.random() < 0.25:
            duration = random.randint(5, 14)
            start = start_d + timedelta(days=random.randint(0, max(0, (end_d - start_d).days - duration)))
            end = start + timedelta(days=duration)
            promo_type = random.choice(PROMO_TYPES)
            disc = round(random.uniform(0.05, 0.30), 2)
            promos.append({
                "promo_id": rand_sku(),
                "product_id": p["product_id"],
                "start_date": start.isoformat(),
                "end_date": end.isoformat(),
                "promo_type": promo_type,
                "discount_pct": disc,
            })
    return promos

def _promo_lookup(promos: List[Dict]) -> Dict[int, List[Tuple[date, date, float]]]:
    by_prod: Dict[int, List[Tuple[date, date, float]]] = {}
    for pr in promos:
        pid = pr["product_id"]
        s = date.fromisoformat(pr["start_date"])
        e = date.fromisoformat(pr["end_date"])
        d = float(pr["discount_pct"])
        by_prod.setdefault(pid, []).append((s, e, d))
    return by_prod

def is_promo_active(pid: int, ts: datetime, promo_idx: Dict[int, List[Tuple[date, date, float]]]) -> float:
    if pid not in promo_idx:
        return 0.0
    d = ts.date()
    for (s, e, disc) in promo_idx[pid]:
        if s <= d <= e:
            return disc
    return 0.0

def gen_orders_and_items(
    stores: List[Dict],
    customers: List[Dict],
    products: List[Dict],
    start_dt: datetime,
    end_dt: datetime,
    orders_estimate: int,
    seed: int,
) -> Tuple[List[Dict], List[Dict]]:
    rnd = random.Random(seed + 777)
    # Popularity index: pre-sort products by a stable random key to create consistent "top sellers"
    product_order = list(range(len(products)))
    rnd.shuffle(product_order)

    store_bias = {s["store_id"]: rnd.uniform(0.7, 1.3) for s in stores}

    total_minutes = int((end_dt - start_dt).total_seconds() // 60)
    # base rate per minute to reach target; we’ll modulate by diurnal/weekend/store
    base_per_minute = max(1e-6, orders_estimate / max(1, total_minutes))

    orders: List[Dict] = []
    items: List[Dict] = []

    current = start_dt
    order_counter = 0

    while current <= end_dt:
        # expected orders this minute across all stores
        diurnal = diurnal_multiplier(current)
        wknd = weekend_multiplier(current)
        exp_minute = base_per_minute * diurnal * wknd

        # sample a Poisson-like small integer using a geometric trick
        minute_orders = 0
        p = exp_minute / (1.0 + exp_minute)
        while rnd.random() < p:
            minute_orders += 1

        for _ in range(minute_orders):
            order_counter += 1
            order_id = f"O{seed}{order_counter:08d}"

            store = rnd.choice(stores)
            # additional store multiplier
            if rnd.random() > store_bias[store["store_id"]]:
                # small chance to skip this store’s order
                continue

            customer = rnd.choice(customers)
            payment = rnd.choices(PAYMENT_TYPES, weights=[0.7, 0.15, 0.15])[0]
            order_disc = round(max(0.0, rnd.gauss(0.05, 0.03)), 2)
            order_disc = min(order_disc, 0.25) if rnd.random() < 0.6 else 0.0

            orders.append({
                "order_id": order_id,
                "store_id": store["store_id"],
                "customer_id": customer["customer_id"],
                "order_ts": current.isoformat(timespec="seconds"),
                "payment_type": payment,
                "discount_pct": order_disc,
            })

            # basket size: 1–8, skew small
            basket_size = 1 + int(abs(rnd.gauss(1.0, 1.0)) * 2)
            basket_size = min(max(1, basket_size), 8)

            # choose products with popularity skew
            for line_no in range(1, basket_size + 1):
                # bias product index via zipf-like transform of shuffled base
                base_idx = zipf_like_index(len(products), s=1.15)
                pid = products[product_order[base_idx]]["product_id"]
                prod = products[pid - 1]

                qty = 1 if rnd.random() < 0.75 else rnd.randint(2, 5)

                # promo may be active → extra discount
                # build promo index lazily outside loop; to keep function pure, perform a light local check
                # (we'll compute exact promo in outer call via lookup to keep logic centralized)
                # here, we just pick base price; price adjustments will be computed by caller if needed
                unit_price = float(prod["base_price"])

                items.append({
                    "order_id": order_id,
                    "line_number": line_no,
                    "product_id": pid,
                    "qty": qty,
                    "unit_price": unit_price,  # provisional; final price after discounts applied later
                    "extended_price": price_round(unit_price * qty),
                })

        current += timedelta(minutes=1)

    return orders, items

def apply_discounts_with_promotions(
    orders: List[Dict],
    items: List[Dict],
    products_by_id: Dict[int, Dict],
    promo_idx: Dict[int, List[Tuple[date, date, float]]]
) -> None:
    # Map orders by id for order-level discount
    order_map = {o["order_id"]: o for o in orders}
    for it in items:
        order = order_map[it["order_id"]]
        ts = datetime.fromisoformat(order["order_ts"])
        base_price = float(products_by_id[it["product_id"]]["base_price"])
        # order-level discount first
        price_after_order_disc = base_price * (1.0 - float(order["discount_pct"]))
        # promo discount if active
        promo_disc = is_promo_active(it["product_id"], ts, promo_idx)
        final_unit = price_after_order_disc * (1.0 - promo_disc)
        final_unit = price_round(final_unit)
        it["unit_price"] = final_unit
        it["extended_price"] = price_round(final_unit * int(it["qty"]))

def gen_inventory_snapshots(
    stores: List[Dict],
    products: List[Dict],
    start_d: date,
    end_d: date,
) -> List[Dict]:
    snaps: List[Dict] = []
    days = (end_d - start_d).days + 1
    for d in range(days):
        snap_date = start_d + timedelta(days=d)
        snap_ts = datetime.combine(snap_date, time(6, 0, 0))  # 6am snapshot
        for s in stores:
            # sample subset of products per store to keep file sizes reasonable
            # (for small scale we keep all)
            for p in products:
                on_hand = max(0, int(random.gauss(40, 15)))
                safety = max(5, int(on_hand * random.uniform(0.15, 0.35)))
                on_order = int(on_hand < safety) * random.randint(10, 60)
                reorder = int(on_hand < safety) * random.randint(10, 40)
                snaps.append({
                    "snapshot_ts": snap_ts.isoformat(timespec="seconds"),
                    "store_id": s["store_id"],
                    "product_id": p["product_id"],
                    "on_hand": on_hand,
                    "on_order": on_order,
                    "safety_stock": safety,
                    "reorder_qty": reorder,
                })
    return snaps


# -----------------------------
# CSV writer
# -----------------------------

def write_csv(path: str, rows: List[Dict], headers: List[str]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for r in rows:
            w.writerow(r)


# -----------------------------
# Main
# -----------------------------

def main(argv: Optional[List[str]] = None) -> int:
    # Get defaults from config if available
    default_output_dir = "sample_data"  # fallback
    default_scale = "small"  # fallback
    default_days = 14  # fallback
    default_seed = 42  # fallback
    
    if get_config:
        try:
            config = get_config()
            default_output_dir = config.data_dir
            default_scale = config.default_seed_scale
            default_days = config.default_seed_days
            default_seed = config.default_seed_value
        except Exception:
            pass  # Use fallbacks if config fails
    
    parser = argparse.ArgumentParser(description="Generate fake retail data to CSVs.")
    parser.add_argument("--scale", choices=SCALES.keys(), default=default_scale)
    parser.add_argument("--days", type=int, default=default_days, help="Number of days of order history.")
    parser.add_argument("--start-date", type=str, default=None, help="YYYY-MM-DD (defaults to today - days + 1)")
    parser.add_argument("--output-dir", type=str, default=default_output_dir)
    parser.add_argument("--seed", type=int, default=default_seed)
    parser.add_argument("--no-overwrite", action="store_true", help="Fail if CSVs already exist.")
    args = parser.parse_args(argv)

    random.seed(args.seed)

    scale = SCALES[args.scale]
    outdir = args.output_dir
    ensure_dir(outdir)

    # file paths
    files = {
        "stores": os.path.join(outdir, "stores.csv"),
        "products": os.path.join(outdir, "products.csv"),
        "customers": os.path.join(outdir, "customers.csv"),
        "orders": os.path.join(outdir, "orders.csv"),
        "order_items": os.path.join(outdir, "order_items.csv"),
        "inventory_snapshots": os.path.join(outdir, "inventory_snapshots.csv"),
        "promotions": os.path.join(outdir, "promotions.csv"),
    }
    if args.no_overwrite:
        for p in files.values():
            if os.path.exists(p):
                print(f"Refusing to overwrite existing file: {p}", file=sys.stderr)
                return 2

    # time window
    if args.start_date:
        start_d = date.fromisoformat(args.start_date)
    else:
        start_d = (datetime.now(timezone.utc).date() - timedelta(days=args.days - 1))
    end_d = start_d + timedelta(days=args.days - 1)

    start_dt = datetime.combine(start_d, time(0, 0, 0))
    end_dt = datetime.combine(end_d, time(23, 59, 0))

    # generate core dims
    stores = gen_stores(scale.stores, start_d)
    products = gen_products(scale.products)
    customers = gen_customers(scale.customers)

    # promotions (before orders)
    promotions = gen_promotions(products, start_d, end_d)
    promo_idx = _promo_lookup(promotions)
    products_by_id = {p["product_id"]: p for p in products}

    # orders & items
    orders, items = gen_orders_and_items(
        stores=stores,
        customers=customers,
        products=products,
        start_dt=start_dt,
        end_dt=end_dt,
        orders_estimate=scale.orders_estimate,
        seed=args.seed,
    )
    # apply discounts/promos to line prices
    apply_discounts_with_promotions(orders, items, products_by_id, promo_idx)

    # inventory snapshots
    inventory = gen_inventory_snapshots(stores, products, start_d, end_d)

    # write CSVs
    write_csv(files["stores"], stores,
              ["store_id", "name", "region", "city", "latitude", "longitude", "opened_date"])
    write_csv(files["products"], products,
              ["product_id", "sku", "name", "category", "brand", "base_price"])
    write_csv(files["customers"], customers,
              ["customer_id", "segment", "signup_ts", "home_region", "home_city"])
    write_csv(files["orders"], orders,
              ["order_id", "store_id", "customer_id", "order_ts", "payment_type", "discount_pct"])
    write_csv(files["order_items"], items,
              ["order_id", "line_number", "product_id", "qty", "unit_price", "extended_price"])
    write_csv(files["inventory_snapshots"], inventory,
              ["snapshot_ts", "store_id", "product_id", "on_hand", "on_order", "safety_stock", "reorder_qty"])
    write_csv(files["promotions"], promotions,
              ["promo_id", "product_id", "start_date", "end_date", "promo_type", "discount_pct"])

    # simple summary
    print(f"Generated data in {outdir}")
    print(f" stores: {len(stores)} | products: {len(products)} | customers: {len(customers)}")
    print(f" orders: {len(orders)} | order_items: {len(items)} | promotions: {len(promotions)}")
    print(f" inventory_snapshots: {len(inventory)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
