#!/usr/bin/env python3
"""
Databricks Pipeline: Generate Sample Data

This pipeline generates synthetic retail data using the functions from data_generators.py
and writes the data to Delta tables in the specified catalog and schema.

The pipeline generates data in the correct sequence to maintain referential integrity:
1. stores, products, customers (dimension tables)
2. promotions (depends on products)
3. orders and order_items (depend on all dimensions)
4. inventory_snapshots (depends on stores and products)

Parameters (command-line arguments):
- catalog: Target catalog name
- schema: Target schema name
- scale: Data scale (small, medium, large)
- days: Number of days of order history
- overwrite: Whether to overwrite existing data
- seed: Random seed for reproducible data generation

Note: Catalog and schema are passed as parameters to handle target-specific naming
(e.g., dev_max_howarth_sqlw_to_lakebase_backend in dev vs sqlw_to_lakebase_backend in prod)
"""

import sys
import os
from datetime import datetime, timedelta, date, time, timezone
from typing import Dict, List, Tuple

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Import the data generation functions from the local data_generators.py file
from data_generators import (
    gen_stores, gen_products, gen_customers, gen_promotions,
    gen_orders_and_items, apply_discounts_with_promotions,
    gen_inventory_snapshots, _promo_lookup
)

# Get parameters from command line arguments
if len(sys.argv) != 7:
    print("Usage: generate_data_job.py <catalog> <schema> <scale> <days> <overwrite> <seed>")
    print("Note: Catalog and schema are passed as parameters to handle target-specific naming")
    sys.exit(1)

catalog = sys.argv[1]
schema = sys.argv[2]
scale = sys.argv[3]
days = int(sys.argv[4])
overwrite = sys.argv[5].lower() == "true"
seed = int(sys.argv[6])

print(f"Starting data generation pipeline with parameters:")
print(f"  Catalog: {catalog}")
print(f"  Schema: {schema}")
print(f"  Scale: {scale}")
print(f"  Days: {days}")
print(f"  Overwrite: {overwrite}")
print(f"  Seed: {seed}")

# Initialize Spark session
spark = SparkSession.builder.appName("SampleDataGeneration").getOrCreate()

# Set the catalog and schema from parameters
print(f"Setting catalog to: {catalog}")
spark.sql(f"USE CATALOG {catalog}")

print(f"Setting schema to: {schema}")
spark.sql(f"USE SCHEMA {schema}")

# Define the data scales (matching data_generators.py)
SCALES = {
    "small": {"stores": 10, "products": 200, "customers": 2000, "orders_estimate": 4000},
    "medium": {"stores": 50, "products": 1000, "customers": 25000, "orders_estimate": 75000},
    "large": {"stores": 200, "products": 5000, "customers": 120000, "orders_estimate": 500000}
}

if scale not in SCALES:
    raise ValueError(f"Invalid scale '{scale}'. Must be one of: {list(SCALES.keys())}")

scale_config = SCALES[scale]

# Set random seed for reproducibility
import random
random.seed(seed)

# Calculate time window
start_d = (datetime.now(timezone.utc).date() - timedelta(days=days - 1))
end_d = start_d + timedelta(days=days - 1)
start_dt = datetime.combine(start_d, time(0, 0, 0))
end_dt = datetime.combine(end_d, time(23, 59, 0))

print(f"Generating data for period: {start_d} to {end_d}")

def write_dataframe_to_delta(df, table_name: str, overwrite: bool = False):
    """Write a DataFrame to a Delta table with proper error handling."""
    full_table_name = f"{catalog}.{schema}.{table_name}"

    try:
        if overwrite:
            print(f"Writing {table_name} (overwrite mode)...")
            df.write.format("delta").mode("overwrite").saveAsTable(full_table_name)
        else:
            print(f"Writing {table_name} (append mode)...")
            df.write.format("delta").mode("append").saveAsTable(full_table_name)

        # Verify the write
        row_count = spark.table(full_table_name).count()
        print(f"✓ Successfully wrote {table_name} with {row_count} rows")

    except Exception as e:
        print(f"✗ Failed to write {table_name}: {str(e)}")
        raise

def create_dataframe_from_dicts(data: List[Dict], table_name: str):
    """Convert a list of dictionaries to a Spark DataFrame."""
    if not data:
        raise ValueError(f"No data generated for {table_name}")

    # Convert to DataFrame
    df = spark.createDataFrame(data)
    print(f"Generated {table_name}: {len(data)} rows")
    return df

try:
    print("\n=== Starting Data Generation ===")

    # Step 1: Generate dimension tables
    print("\n1. Generating stores...")
    stores_data = gen_stores(scale_config["stores"], start_d)
    stores_df = create_dataframe_from_dicts(stores_data, "stores")
    write_dataframe_to_delta(stores_df, "stores", overwrite)

    print("\n2. Generating products...")
    products_data = gen_products(scale_config["products"])
    products_df = create_dataframe_from_dicts(products_data, "products")
    write_dataframe_to_delta(products_df, "products", overwrite)

    print("\n3. Generating customers...")
    customers_data = gen_customers(scale_config["customers"])
    customers_df = create_dataframe_from_dicts(customers_data, "customers")
    write_dataframe_to_delta(customers_df, "customers", overwrite)

    # Step 2: Generate promotions (depends on products)
    print("\n4. Generating promotions...")
    promotions_data = gen_promotions(products_data, start_d, end_d)
    promotions_df = create_dataframe_from_dicts(promotions_data, "promotions")
    write_dataframe_to_delta(promotions_df, "promotions", overwrite)

    # Step 3: Generate orders and order items (depends on all dimensions)
    print("\n5. Generating orders and order items...")
    orders_data, items_data = gen_orders_and_items(
        stores=stores_data,
        customers=customers_data,
        products=products_data,
        start_dt=start_dt,
        end_dt=end_dt,
        orders_estimate=scale_config["orders_estimate"],
        seed=seed
    )

    # Apply discounts and promotions to order items
    promo_idx = _promo_lookup(promotions_data)
    products_by_id = {p["product_id"]: p for p in products_data}
    apply_discounts_with_promotions(orders_data, items_data, products_by_id, promo_idx)

    # Write orders
    orders_df = create_dataframe_from_dicts(orders_data, "orders")
    write_dataframe_to_delta(orders_df, "orders", overwrite)

    # Write order items
    items_df = create_dataframe_from_dicts(items_data, "order_items")
    write_dataframe_to_delta(items_df, "order_items", overwrite)

    # Step 4: Generate inventory snapshots (depends on stores and products)
    print("\n6. Generating inventory snapshots...")
    inventory_data = gen_inventory_snapshots(stores_data, products_data, start_d, end_d)
    inventory_df = create_dataframe_from_dicts(inventory_data, "inventory_snapshots")
    write_dataframe_to_delta(inventory_df, "inventory_snapshots", overwrite)

    print("\n=== Data Generation Complete ===")
    print(f"Successfully generated and wrote all tables to {catalog}.{schema}")
    print(f"Summary:")
    print(f"  Stores: {len(stores_data)}")
    print(f"  Products: {len(products_data)}")
    print(f"  Customers: {len(customers_data)}")
    print(f"  Orders: {len(orders_data)}")
    print(f"  Order Items: {len(items_data)}")
    print(f"  Promotions: {len(promotions_data)}")
    print(f"  Inventory Snapshots: {len(inventory_data)}")

except Exception as e:
    print(f"\n✗ Pipeline failed: {str(e)}")
    raise

finally:
    # Clean up
    if 'spark' in locals():
        spark.stop()
