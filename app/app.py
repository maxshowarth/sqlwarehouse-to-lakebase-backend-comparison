import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

# Configuration
from config import get_config

# DataAccess interface + CSV implementation
from backend.data_access.csv_backend import CsvDataAccess  # conforms to DataAccess
# (If you later add a factory, you can switch to: from data_access.util import get_data_access)

st.set_page_config(page_title="Store Orders — CSV via DataAccess", layout="wide")

# -----------------------------------------------------------------------------
# Backend selection (CSV for now). We point at your configured data directory:
# -----------------------------------------------------------------------------
config = get_config()
DATA_DIR = Path(config.data_dir)
da = CsvDataAccess(data_dir=DATA_DIR)

# -----------------------------------------------------------------------------
# Sidebar filters (all choices sourced via the DataAccess layer)
# -----------------------------------------------------------------------------
st.sidebar.header("Filters")

min_ts, max_ts = da.get_date_bounds()
min_d, max_d = min_ts.date(), max_ts.date()
date_range = st.sidebar.date_input("Order date range", (min_d, max_d), min_value=min_d, max_value=max_d)

stores_df = da.list_stores()
store_options = ["(All)"] + stores_df["store_name"].sort_values().tolist()
store_sel = st.sidebar.selectbox("Store", store_options)

cats_df = da.list_categories()
cat_options = ["(All)"] + cats_df["category"].sort_values().tolist()
cat_sel = st.sidebar.selectbox("Category", cat_options)

prod_search = st.sidebar.text_input("Product search (contains)")

row_limit = st.sidebar.number_input(
    "Max table rows", 
    min_value=config.min_row_limit, 
    max_value=config.max_row_limit, 
    value=config.default_row_limit, 
    step=100
)
slice_by = st.sidebar.radio("Slice chart by", ["None", "store", "category", "hour"], horizontal=True)

# Normalize filters for the backend
start_ts = pd.to_datetime(date_range[0])
end_ts = pd.to_datetime(date_range[1]) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
store_name = None if store_sel == "(All)" else store_sel
category = None if cat_sel == "(All)" else cat_sel

# -----------------------------------------------------------------------------
# Queries via the interface (each interaction triggers fresh calls)
# -----------------------------------------------------------------------------
t0 = time.perf_counter()
kpis = da.get_kpis(start_ts, end_ts, store_name, category, prod_search)
t_kpis = (time.perf_counter() - t0) * 1000.0

t0 = time.perf_counter()
orders_df = da.get_orders(
    start_ts, end_ts, store_name, category, prod_search,
    limit=int(row_limit), order_by="order_ts_desc"
)
t_orders = (time.perf_counter() - t0) * 1000.0

slice_val = None if slice_by == "None" else slice_by
t0 = time.perf_counter()
counts_df = da.get_product_counts(
    start_ts, end_ts, slice_val, store_name, category, prod_search, top_n=25
)
t_counts = (time.perf_counter() - t0) * 1000.0

# -----------------------------------------------------------------------------
# KPIs
# -----------------------------------------------------------------------------
c1, c2, c3, c4 = st.columns(4)
c1.metric("Orders (distinct)", f"{kpis.orders_distinct:,}")
c2.metric("Lines", f"{kpis.lines:,}")
c3.metric("Units", f"{kpis.units:,}")
c4.metric("Revenue", f"${kpis.revenue:,.2f}")

# Optional: tiny latency readout (useful later when comparing backends)
with st.expander("Query timings (ms)"):
    st.write(
        {
            "get_kpis": round(t_kpis, 2),
            "get_orders": round(t_orders, 2),
            "get_product_counts": round(t_counts, 2),
        }
    )

# -----------------------------------------------------------------------------
# Orders table
# -----------------------------------------------------------------------------
st.markdown("### Orders (filtered)")
show_cols = [
    "order_ts", "order_id", "store_name", "city", "region",
    "product_name", "category", "qty", "unit_price", "extended_price",
]
st.dataframe(orders_df[show_cols], use_container_width=True)

# -----------------------------------------------------------------------------
# Product count chart
# -----------------------------------------------------------------------------
st.markdown("### Product counts (filtered)")
if slice_val is None:
    # shape: product_name, item_count
    st.bar_chart(counts_df.set_index("product_name")["item_count"])
else:
    # shape: slice_key, product_name, item_count  -> pivot for chart
    pivot = counts_df.pivot_table(index="product_name", columns="slice_key", values="item_count", fill_value=0)
    st.bar_chart(pivot)

# -----------------------------------------------------------------------------
# Additional charts (derived from the already-filtered orders_df)
# These aggregate client-side over the filtered rows returned by get_orders.
# -----------------------------------------------------------------------------
st.markdown("### Orders and revenue over time")
ts = (
    orders_df.assign(order_hour=orders_df["order_ts"].dt.floor("H"))
             .groupby("order_hour", as_index=False)
             .agg(orders=("order_id", "nunique"), revenue=("extended_price", "sum"))
             .sort_values("order_hour")
)
st.line_chart(ts, x="order_hour", y=["orders", "revenue"], use_container_width=True)

st.markdown("### Revenue by region")
rev_region = (
    orders_df.groupby("region", as_index=False)["extended_price"].sum()
             .rename(columns={"extended_price": "revenue"})
             .sort_values("revenue", ascending=False)
)
st.bar_chart(rev_region, x="region", y="revenue", use_container_width=True)

st.markdown("### Top products by revenue")
top_n_rev = st.sidebar.slider(
    "Top N (revenue)", 
    min_value=config.min_top_n, 
    max_value=config.max_top_n, 
    value=config.default_top_n, 
    step=1
)
top_prod_rev = (
    orders_df.groupby("product_name", as_index=False)["extended_price"].sum()
             .rename(columns={"extended_price": "revenue"})
             .sort_values("revenue", ascending=False)
             .head(int(top_n_rev))
)
st.bar_chart(top_prod_rev, x="product_name", y="revenue", use_container_width=True)

# -----------------------------------------------------------------------------
# Footer
# -----------------------------------------------------------------------------
with st.expander("Data source & architecture"):
    st.write(
        f"This page now reads data via a **DataAccess** interface (CSV-backed for local dev) "
        f"from `{config.data_dir}/`. The UI is decoupled from the data source, so we can later "
        "swap to **SQL Warehouse** or **Lakebase** by changing the implementation behind the interface."
    )

# -----------------------------------------------------------------------------
# Version badge (bottom-left)
# -----------------------------------------------------------------------------
st.markdown(
    """
    <style>
    .version-badge {
        position: fixed;
        left: 12px;
        bottom: 12px;
        background: rgba(0, 0, 0, 0.6);
        color: #ffffff;
        padding: 6px 10px;
        border-radius: 6px;
        font-size: 12px;
        z-index: 1000;
    }
    </style>
    """,
    unsafe_allow_html=True,
)
