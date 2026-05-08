"""
dashboard/app.py
=================
Streamlit dashboard for Amazon Review Sentiment Analysis.

Pages:
  📡 Online  — Live prediction stream (auto-refresh)
  📊 Offline — Analytics & historical charts
"""

import os
import time
import requests
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
API_BASE = os.getenv("API_BASE_URL", "http://backend:8000")
REFRESH_INTERVAL = int(os.getenv("DASHBOARD_REFRESH", "5"))  # seconds

COLORS = {
    "positive": "#2ECC71",
    "neutral":  "#F39C12",
    "negative": "#E74C3C",
    "bg":       "#0E1117",
    "card":     "#1E2130",
    "accent":   "#4A90D9",
}

PRODUCT_HIGHLIGHT = "B001E4KFG0"

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Amazon Sentiment — Big Data Dashboard",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Custom CSS
# ---------------------------------------------------------------------------
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=DM+Sans:wght@300;400;600;700&display=swap');

    :root {
        --bg: #0E1117;
        --card: #1A1F2E;
        --border: #2D3348;
        --positive: #2ECC71;
        --neutral: #F39C12;
        --negative: #E74C3C;
        --accent: #4A90D9;
        --text: #E8ECF0;
        --muted: #8892A0;
    }

    html, body, [class*="css"] {
        font-family: 'DM Sans', sans-serif;
        background-color: var(--bg);
        color: var(--text);
    }

    /* Sidebar */
    section[data-testid="stSidebar"] {
        background: #141824;
        border-right: 1px solid var(--border);
    }

    /* Metric cards */
    div[data-testid="metric-container"] {
        background: var(--card);
        border: 1px solid var(--border);
        border-radius: 12px;
        padding: 16px;
    }

    /* Tables */
    .stDataFrame { border-radius: 8px; overflow: hidden; }

    /* Headers */
    h1, h2, h3 { font-family: 'Space Mono', monospace; }

    /* Tags */
    .tag-positive { background: #1a4731; color: #2ECC71; padding: 2px 10px; border-radius: 20px; font-size: 0.75rem; font-weight: 700; }
    .tag-neutral  { background: #3d2e10; color: #F39C12; padding: 2px 10px; border-radius: 20px; font-size: 0.75rem; font-weight: 700; }
    .tag-negative { background: #3d1010; color: #E74C3C; padding: 2px 10px; border-radius: 20px; font-size: 0.75rem; font-weight: 700; }

    /* Hero banner */
    .hero-banner {
        background: linear-gradient(135deg, #141824 0%, #1a2035 50%, #0d1520 100%);
        border: 1px solid var(--border);
        border-radius: 16px;
        padding: 24px 32px;
        margin-bottom: 24px;
    }

    /* Live indicator */
    @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.4; } }
    .live-dot {
        display: inline-block;
        width: 10px; height: 10px;
        background: #2ECC71;
        border-radius: 50%;
        animation: pulse 1.5s ease-in-out infinite;
        margin-right: 8px;
    }

    /* Cards */
    .stat-card {
        background: var(--card);
        border: 1px solid var(--border);
        border-radius: 12px;
        padding: 20px;
        text-align: center;
    }
</style>
""", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------
@st.cache_data(ttl=3)
def fetch_predictions(limit: int = 50) -> list:
    try:
        r = requests.get(f"{API_BASE}/predictions", params={"limit": limit}, timeout=5)
        r.raise_for_status()
        return r.json().get("predictions", [])
    except Exception as e:
        st.warning(f"⚠️ API error (predictions): {e}")
        return []


@st.cache_data(ttl=10)
def fetch_stats() -> dict:
    try:
        r = requests.get(f"{API_BASE}/stats", timeout=5)
        r.raise_for_status()
        return r.json()
    except Exception:
        return {}


@st.cache_data(ttl=10)
def fetch_distribution(product_id: str = None) -> dict:
    params = {}
    if product_id:
        params["product_id"] = product_id
    try:
        r = requests.get(f"{API_BASE}/sentiment-distribution", params=params, timeout=5)
        r.raise_for_status()
        return r.json()
    except Exception:
        return {}


@st.cache_data(ttl=30)
def fetch_predictions_by_date() -> list:
    try:
        r = requests.get(f"{API_BASE}/predictions-by-date", timeout=5)
        r.raise_for_status()
        return r.json().get("data", [])
    except Exception:
        return []


@st.cache_data(ttl=30)
def fetch_top_products(sentiment: str = "positive", limit: int = 10) -> list:
    try:
        r = requests.get(f"{API_BASE}/top-products",
                         params={"sentiment": sentiment, "limit": limit}, timeout=5)
        r.raise_for_status()
        return r.json().get("products", [])
    except Exception:
        return []


@st.cache_data(ttl=10)
def fetch_product_stats(product_id: str) -> dict:
    try:
        r = requests.get(f"{API_BASE}/product/{product_id}", timeout=5)
        r.raise_for_status()
        return r.json()
    except Exception:
        return {}


def api_healthy() -> bool:
    try:
        r = requests.get(f"{API_BASE}/health", timeout=3)
        return r.status_code == 200
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Sentiment badge helper
# ---------------------------------------------------------------------------
def sentiment_badge(s: str) -> str:
    if not s:
        return ""
    cls = f"tag-{s.lower()}"
    icon = {"positive": "✅", "neutral": "🔶", "negative": "❌"}.get(s.lower(), "")
    return f'<span class="{cls}">{icon} {s.upper()}</span>'


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
with st.sidebar:
    st.markdown("""
    <div style='text-align:center; padding: 16px 0 8px;'>
        <span style='font-size:2.5rem;'>🛒</span>
        <h2 style='font-family:Space Mono,monospace; font-size:1rem; margin:8px 0 4px; color:#4A90D9;'>AMAZON SENTIMENT</h2>
        <p style='color:#8892A0; font-size:0.75rem; margin:0;'>Big Data Real-Time Analytics</p>
    </div>
    <hr style='border-color:#2D3348; margin: 12px 0;'>
    """, unsafe_allow_html=True)

    page = st.radio(
        "Navigation",
        ["📡 Online — Live Stream", "📊 Offline — Analytics"],
        label_visibility="collapsed",
    )

    st.markdown("<hr style='border-color:#2D3348; margin:16px 0;'>", unsafe_allow_html=True)

    # API status indicator
    healthy = api_healthy()
    status_color = "#2ECC71" if healthy else "#E74C3C"
    status_text  = "API Online" if healthy else "API Offline"
    st.markdown(
        f"<div style='display:flex;align-items:center;gap:8px;'>"
        f"<div style='width:8px;height:8px;background:{status_color};border-radius:50%;'></div>"
        f"<span style='color:{status_color};font-size:0.8rem;font-weight:600;'>{status_text}</span>"
        f"</div>",
        unsafe_allow_html=True
    )

    st.markdown(
        f"<p style='color:#8892A0;font-size:0.72rem;margin-top:8px;'>Backend: <code>{API_BASE}</code></p>",
        unsafe_allow_html=True
    )

    st.markdown("<hr style='border-color:#2D3348; margin:16px 0;'>", unsafe_allow_html=True)
    st.markdown(
        "<p style='color:#8892A0;font-size:0.72rem;'>IASD Mini-Project · Big Data 2025-2026</p>",
        unsafe_allow_html=True
    )


# ===========================================================================
# PAGE 1 — ONLINE LIVE STREAM
# ===========================================================================
if page == "📡 Online — Live Stream":

    # Header
    st.markdown("""
    <div class='hero-banner'>
        <h1 style='margin:0; font-size:1.6rem; color:#4A90D9;'>
            <span class='live-dot'></span>Live Prediction Stream
        </h1>
        <p style='color:#8892A0; margin:8px 0 0; font-size:0.9rem;'>
            Real-time sentiment predictions from Spark Structured Streaming → MongoDB
        </p>
    </div>
    """, unsafe_allow_html=True)

    # Controls row
    col_r, col_l, col_s = st.columns([2, 1, 1])
    with col_r:
        auto_refresh = st.toggle("Auto-refresh", value=True)
        n_rows = st.slider("Reviews to display", 10, 200, 50, step=10)
    with col_l:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("🔄 Refresh Now", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
    with col_s:
        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown(
            f"<p style='color:#8892A0;font-size:0.8rem;margin-top:8px;'>⏱ Refresh: {REFRESH_INTERVAL}s</p>",
            unsafe_allow_html=True
        )

    # Fetch live predictions
    preds = fetch_predictions(limit=n_rows)

    if not preds:
        st.info("⏳ No predictions yet. Make sure the Kafka producer and Spark streaming are running.")
    else:
        df = pd.DataFrame(preds)

        # Stat cards
        st.markdown("<br>", unsafe_allow_html=True)
        c1, c2, c3, c4 = st.columns(4)
        pos_count = len(df[df["prediction"] == "positive"]) if "prediction" in df.columns else 0
        neu_count = len(df[df["prediction"] == "neutral"])  if "prediction" in df.columns else 0
        neg_count = len(df[df["prediction"] == "negative"]) if "prediction" in df.columns else 0

        c1.metric("Total Shown",   len(df))
        c2.metric("✅ Positive",   pos_count, f"{pos_count/len(df)*100:.0f}%")
        c3.metric("🔶 Neutral",    neu_count, f"{neu_count/len(df)*100:.0f}%")
        c4.metric("❌ Negative",   neg_count, f"{neg_count/len(df)*100:.0f}%")

        # Mini donut chart
        if "prediction" in df.columns:
            dist_df = df["prediction"].value_counts().reset_index()
            dist_df.columns = ["sentiment", "count"]
            fig_mini = px.pie(
                dist_df, names="sentiment", values="count",
                hole=0.55,
                color="sentiment",
                color_discrete_map={"positive": COLORS["positive"],
                                    "neutral":  COLORS["neutral"],
                                    "negative": COLORS["negative"]},
                title="Live Batch Distribution",
            )
            fig_mini.update_layout(
                height=220, margin=dict(t=40, b=0, l=0, r=0),
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                font_color="#E8ECF0", legend=dict(orientation="h"),
            )
            st.plotly_chart(fig_mini, use_container_width=True)

        # Live table
        st.markdown("### 📋 Latest Predictions")
        display_cols = [c for c in ["ProductId", "Score", "Summary", "prediction", "timestamp"]
                        if c in df.columns]
        display_df = df[display_cols].copy()

        if "prediction" in display_df.columns:
            display_df["Sentiment"] = display_df["prediction"].apply(
                lambda x: {"positive": "✅ Positive", "neutral": "🔶 Neutral", "negative": "❌ Negative"}.get(x, x)
            )
            display_df = display_df.drop(columns=["prediction"])

        if "Summary" in display_df.columns:
            display_df["Summary"] = display_df["Summary"].str[:80] + "..."

        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True,
            height=420,
        )

    # Auto-refresh
    if auto_refresh:
        time.sleep(REFRESH_INTERVAL)
        st.cache_data.clear()
        st.rerun()


# ===========================================================================
# PAGE 2 — OFFLINE ANALYTICS DASHBOARD
# ===========================================================================
elif page == "📊 Offline — Analytics":

    # Header
    st.markdown("""
    <div class='hero-banner'>
        <h1 style='margin:0; font-size:1.6rem; color:#4A90D9;'>📊 Offline Analytics Dashboard</h1>
        <p style='color:#8892A0; margin:8px 0 0; font-size:0.9rem;'>
            Aggregated analysis of all predicted reviews stored in MongoDB
        </p>
    </div>
    """, unsafe_allow_html=True)

    # -------------------------
    # KPI Cards
    # -------------------------
    stats = fetch_stats()

    if stats:
        st.markdown("### 📈 Overview")
        k1, k2, k3, k4, k5 = st.columns(5)
        k1.metric("Total Reviews",    f"{stats.get('total_reviews', 0):,}")
        k2.metric("✅ Positive",       f"{stats.get('positive', 0):,}",
                  f"{stats.get('positive_pct', 0)}%")
        k3.metric("🔶 Neutral",        f"{stats.get('neutral', 0):,}",
                  f"{stats.get('neutral_pct', 0)}%")
        k4.metric("❌ Negative",       f"{stats.get('negative', 0):,}",
                  f"{stats.get('negative_pct', 0)}%")
        k5.metric("🏷️ Products",        f"{stats.get('total_products', 0):,}")
    else:
        st.warning("⚠️ Could not load statistics from API.")

    st.markdown("---")

    # -------------------------
    # Row 1: Pie + Time Series
    # -------------------------
    col_pie, col_time = st.columns([1, 2])

    with col_pie:
        st.markdown("#### 🥧 Global Sentiment Distribution")
        dist = fetch_distribution()
        if dist and dist.get("total", 0) > 0:
            pie_data = pd.DataFrame([
                {"Sentiment": "Positive", "Count": dist.get("positive", 0)},
                {"Sentiment": "Neutral",  "Count": dist.get("neutral",  0)},
                {"Sentiment": "Negative", "Count": dist.get("negative", 0)},
            ])
            fig_pie = px.pie(
                pie_data, names="Sentiment", values="Count",
                color="Sentiment",
                color_discrete_map={
                    "Positive": COLORS["positive"],
                    "Neutral":  COLORS["neutral"],
                    "Negative": COLORS["negative"],
                },
                hole=0.45,
            )
            fig_pie.update_traces(textposition="inside", textinfo="percent+label")
            fig_pie.update_layout(
                height=350, margin=dict(t=20, b=20, l=20, r=20),
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                font_color="#E8ECF0", showlegend=False,
            )
            st.plotly_chart(fig_pie, use_container_width=True)
        else:
            st.info("No distribution data yet.")

    with col_time:
        st.markdown("#### 📅 Predictions by Date (Monthly)")
        date_data = fetch_predictions_by_date()
        if date_data:
            date_df = pd.DataFrame(date_data)
            fig_time = go.Figure()
            for sentiment, color in [("positive", COLORS["positive"]),
                                      ("neutral",  COLORS["neutral"]),
                                      ("negative", COLORS["negative"])]:
                if sentiment in date_df.columns:
                    fig_time.add_trace(go.Bar(
                        x=date_df["date"], y=date_df[sentiment],
                        name=sentiment.capitalize(), marker_color=color,
                    ))
            fig_time.update_layout(
                barmode="stack",
                height=350,
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                font_color="#E8ECF0",
                xaxis=dict(title="Month", gridcolor="#2D3348"),
                yaxis=dict(title="Review Count", gridcolor="#2D3348"),
                legend=dict(orientation="h", y=1.1),
                margin=dict(t=20, b=40, l=40, r=20),
            )
            st.plotly_chart(fig_time, use_container_width=True)
        else:
            st.info("No time-series data yet.")

    st.markdown("---")

    # -------------------------
    # Row 2: Product B001E4KFG0
    # -------------------------
    st.markdown(f"#### 🔍 Product Analysis: `{PRODUCT_HIGHLIGHT}`")

    prod_col1, prod_col2 = st.columns([1, 2])

    with prod_col1:
        prod_stats = fetch_product_stats(PRODUCT_HIGHLIGHT)
        if prod_stats and prod_stats.get("total_reviews", 0) > 0:
            pie_prod = px.pie(
                pd.DataFrame([
                    {"Sentiment": "Positive", "Count": prod_stats.get("positive", 0)},
                    {"Sentiment": "Neutral",  "Count": prod_stats.get("neutral",  0)},
                    {"Sentiment": "Negative", "Count": prod_stats.get("negative", 0)},
                ]),
                names="Sentiment", values="Count",
                color="Sentiment",
                color_discrete_map={"Positive": COLORS["positive"],
                                    "Neutral":  COLORS["neutral"],
                                    "Negative": COLORS["negative"]},
                title=f"Scoring — {PRODUCT_HIGHLIGHT}",
                hole=0.5,
            )
            pie_prod.update_layout(
                height=300, margin=dict(t=40, b=10, l=10, r=10),
                paper_bgcolor="rgba(0,0,0,0)", font_color="#E8ECF0",
                showlegend=True, legend=dict(orientation="h"),
            )
            st.plotly_chart(pie_prod, use_container_width=True)

            # Stats underneath
            p1, p2, p3 = st.columns(3)
            p1.metric("✅", f"{prod_stats.get('positive_pct',0)}%")
            p2.metric("🔶", f"{prod_stats.get('neutral_pct', 0)}%")
            p3.metric("❌", f"{prod_stats.get('negative_pct',0)}%")
        else:
            st.info(f"No data yet for product {PRODUCT_HIGHLIGHT}")

    with prod_col2:
        # Custom product lookup
        st.markdown("**🔎 Search any product:**")
        search_id = st.text_input("Enter ProductId", value=PRODUCT_HIGHLIGHT)
        if search_id:
            s = fetch_product_stats(search_id)
            if s and s.get("total_reviews", 0) > 0:
                sc1, sc2, sc3 = st.columns(3)
                sc1.metric("Total Reviews", s.get("total_reviews", 0))
                sc2.metric("✅ Positive",   f"{s.get('positive_pct',0)}%")
                sc3.metric("❌ Negative",   f"{s.get('negative_pct',0)}%")

                if s.get("sample_reviews"):
                    samp_df = pd.DataFrame(s["sample_reviews"])[
                        ["Score", "Summary", "prediction"]
                    ].rename(columns={"prediction": "Predicted"})
                    if "Summary" in samp_df.columns:
                        samp_df["Summary"] = samp_df["Summary"].str[:70] + "..."
                    st.dataframe(samp_df, use_container_width=True, hide_index=True)
            else:
                st.info(f"No data for product `{search_id}`")

    st.markdown("---")

    # -------------------------
    # Row 3: Top Products
    # -------------------------
    st.markdown("#### 🏆 Top Products by Sentiment")

    tab_pos, tab_neg = st.tabs(["✅ Top Positive", "❌ Top Negative"])

    with tab_pos:
        top_pos = fetch_top_products("positive", 10)
        if top_pos:
            pos_df = pd.DataFrame(top_pos)
            fig_pos = px.bar(
                pos_df, x="count", y="ProductId", orientation="h",
                color_discrete_sequence=[COLORS["positive"]],
                labels={"count": "Review Count", "ProductId": "Product ID"},
            )
            fig_pos.update_layout(
                height=350, paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)", font_color="#E8ECF0",
                xaxis=dict(gridcolor="#2D3348"), yaxis=dict(categoryorder="total ascending"),
                margin=dict(t=20, b=20, l=100, r=20),
            )
            st.plotly_chart(fig_pos, use_container_width=True)
        else:
            st.info("No data yet.")

    with tab_neg:
        top_neg = fetch_top_products("negative", 10)
        if top_neg:
            neg_df = pd.DataFrame(top_neg)
            fig_neg = px.bar(
                neg_df, x="count", y="ProductId", orientation="h",
                color_discrete_sequence=[COLORS["negative"]],
                labels={"count": "Review Count", "ProductId": "Product ID"},
            )
            fig_neg.update_layout(
                height=350, paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)", font_color="#E8ECF0",
                xaxis=dict(gridcolor="#2D3348"), yaxis=dict(categoryorder="total ascending"),
                margin=dict(t=20, b=20, l=100, r=20),
            )
            st.plotly_chart(fig_neg, use_container_width=True)
        else:
            st.info("No data yet.")

    st.markdown("---")

    # -------------------------
    # Footer
    # -------------------------
    st.markdown(
        "<p style='text-align:center;color:#8892A0;font-size:0.75rem;'>"
        "Amazon Review Sentiment · IASD Big Data 2025-2026 · "
        f"Built with Apache Kafka · Spark Streaming · MongoDB · FastAPI · Streamlit"
        "</p>",
        unsafe_allow_html=True
    )
