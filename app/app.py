import streamlit as st
import pandas as pd
from pathlib import Path

st.set_page_config(page_title="Store Orders — CSV Demo", layout="wide")

DATA_DIR = Path("backend/sample_data")

@st.cache_data(show_spinner=False)
def load_data():
    orders = pd.read_csv(DATA_DIR / "orders.csv", parse_dates=["order_ts"])
    order_items = pd.read_csv(DATA_DIR / "order_items.csv")
    products = pd.read_csv(DATA_DIR / "products.csv")
    stores = pd.read_csv(DATA_DIR / "stores.csv", parse_dates=["opened_date"])

    # Rename to avoid "name" collision after joins
    products = products.rename(columns={"name": "product_name"})
    stores = stores.rename(columns={"name": "store_name"})

    # Join -> long "order lines" view (one row per order item)
    lines_df = (
        orders
        .merge(order_items, on="order_id", how="inner")
        .merge(products[["product_id", "product_name", "category", "brand"]], on="product_id", how="left")
        .merge(stores[["store_id", "store_name", "region", "city"]], on="store_id", how="left")
    )
    # Convenience columns
    lines_df["order_date"] = lines_df["order_ts"].dt.date
    lines_df["hour"] = lines_df["order_ts"].dt.hour
    lines_df["extended_price"] = lines_df["extended_price"].astype(float)
    return lines_df, orders, products, stores

df, orders_df, products_df, stores_df = load_data()

# ---------------- Sidebar filters ----------------
st.sidebar.header("Filters")

# Date range (based on available data)
min_d = df["order_ts"].min().date()
max_d = df["order_ts"].max().date()
date_range = st.sidebar.date_input("Order date range", (min_d, max_d), min_value=min_d, max_value=max_d)

# Store filter
store_options = ["(All)"] + sorted(stores_df["store_name"].unique().tolist())
store_sel = st.sidebar.selectbox("Store", store_options)

# Category filter
cat_options = ["(All)"] + sorted(products_df["category"].unique().tolist())
cat_sel = st.sidebar.selectbox("Category", cat_options)

# Product search
prod_search = st.sidebar.text_input("Product search (contains)")

# Row limit for the table
row_limit = st.sidebar.number_input("Max table rows", min_value=100, max_value=100000, value=2000, step=100)

# Slicer for the chart
slice_by = st.sidebar.radio("Slice chart by", ["None", "store", "category", "hour"], horizontal=True)

# ---------------- Apply filters ----------------
flt = df.copy()
# Date window
if isinstance(date_range, tuple) and len(date_range) == 2:
    start_d, end_d = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1]) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    flt = flt[(flt["order_ts"] >= start_d) & (flt["order_ts"] <= end_d)]

# Store
if store_sel != "(All)":
    flt = flt[flt["store_name"] == store_sel]

# Category
if cat_sel != "(All)":
    flt = flt[flt["category"] == cat_sel]

# Product search
if prod_search.strip():
    s = prod_search.strip().lower()
    flt = flt[flt["product_name"].str.lower().str.contains(s, na=False)]

# ---------------- Header & KPIs ----------------
col1, col2, col3, col4 = st.columns(4)
col1.metric("Orders (distinct)", f"{flt['order_id'].nunique():,}")
col2.metric("Lines", f"{len(flt):,}")
col3.metric("Units", f"{int(flt['qty'].sum()):,}")
col4.metric("Revenue", f"${flt['extended_price'].sum():,.2f}")

st.markdown("### Orders (filtered)")
show_cols = [
    "order_ts", "order_id", "store_name", "city", "region",
    "product_name", "category", "qty", "unit_price", "extended_price"
]
st.dataframe(flt[show_cols].sort_values("order_ts", ascending=False).head(int(row_limit)), use_container_width=True)

# ---------------- Product count chart ----------------
st.markdown("### Product counts (filtered)")

if slice_by == "None":
    agg = (flt.groupby("product_name", as_index=False)["qty"].sum()
             .rename(columns={"qty": "item_count"})
             .sort_values("item_count", ascending=False)
             .head(25))
    st.bar_chart(agg.set_index("product_name")["item_count"])
else:
    key = {"store": "store_name", "category": "category", "hour": "hour"}[slice_by]
    agg = (flt.groupby([key, "product_name"], as_index=False)["qty"].sum()
             .rename(columns={"qty": "item_count"}))
    # Show top within each slice by overall item_count
    # For display simplicity, pivot to wide (top 10 products overall across slices)
    top_products = (agg.groupby("product_name")["item_count"].sum()
                      .sort_values(ascending=False).head(10).index.tolist())
    agg = agg[agg["product_name"].isin(top_products)]
    pivot = agg.pivot_table(index="product_name", columns=key, values="item_count", fill_value=0)
    st.bar_chart(pivot)

# ---------------- Footer ----------------
with st.expander("Data sources"):
    st.write("This page reads CSVs directly from `sample_data/` (orders, order_items, products, stores). "
             "We’ll later swap this for SQL Warehouse and Lakebase backends without changing the UI.")

# ---------------- Additional charts ----------------
st.markdown("### Orders and revenue over time")
ts = (
    flt.assign(order_hour=flt["order_ts"].dt.floor("H"))
      .groupby("order_hour", as_index=False)
      .agg(orders=("order_id", "nunique"), revenue=("extended_price", "sum"))
      .sort_values("order_hour")
)
st.line_chart(ts, x="order_hour", y=["orders", "revenue"], use_container_width=True)

st.markdown("### Revenue by region")
rev_region = (
    flt.groupby("region", as_index=False)["extended_price"].sum()
       .rename(columns={"extended_price": "revenue"})
       .sort_values("revenue", ascending=False)
)
st.bar_chart(rev_region, x="region", y="revenue", use_container_width=True)

st.markdown("### Top products by revenue")
top_n_rev = st.sidebar.slider("Top N (revenue)", min_value=5, max_value=30, value=10, step=1)
top_prod_rev = (
    flt.groupby("product_name", as_index=False)["extended_price"].sum()
       .rename(columns={"extended_price": "revenue"})
       .sort_values("revenue", ascending=False)
       .head(int(top_n_rev))
)
st.bar_chart(top_prod_rev, x="product_name", y="revenue", use_container_width=True)

# ---------------- Version badge (bottom-left) ----------------
st.markdown(
    f"""
    <style>
    .version-badge {{
        position: fixed;
        left: 12px;
        bottom: 12px;
        background: rgba(0, 0, 0, 0.6);
        color: #ffffff;
        padding: 6px 10px;
        border-radius: 6px;
        font-size: 12px;
        z-index: 1000;
    }}
    </style>
    <div class="version-badge">Version {APP_VERSION}</div>
    """,
    unsafe_allow_html=True,
)
