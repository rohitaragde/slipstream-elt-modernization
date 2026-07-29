"""
Slipstream Analytics Dashboard.

Reads directly from slipstream.duckdb (read-only) — no separate data
pipeline for the dashboard itself; it visualizes exactly what the six
dbt models and the quality-gate run history already produced.

Run from the repo root:
    streamlit run dashboard/app.py

Sections, top to bottom:
    1. Hero header
    2. KPI row
    3. Revenue by plan (total billable per tier)
    4. Revenue share by plan (donut)  +  Loyalty tier value
    5. Customers by state  +  Payments over time
    6. Pipeline health (row-count history from the quality gate)
    7. Tech-stack footer
"""

import json
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# --------------------------------------------------------------------------- paths

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "dashboard"/"dashboard.duckdb"
DBT_RESULTS = BASE_DIR / "slipstream_dbt" / "target" / "run_results.json"

# --------------------------------------------------------------------------- palette

PALETTE = ["#7F5AF0", "#FF6B9D", "#FF9F5A", "#2CD9C5", "#4EA8FF", "#FFD166"]
GRADIENT_HERO = "linear-gradient(120deg, #6C4CE0 0%, #B24CE0 45%, #FF6B9D 100%)"
GRADIENT_ACCENT = "linear-gradient(120deg, #7F5AF0 0%, #FF6B9D 100%)"

# dark-theme tokens — single source of truth for chart + text colours
BG_APP = "#13111F"       # deep indigo-charcoal, warmer than flat black
TEXT = "#ECEAF6"         # primary text on dark
MUTED = "#A29CC0"        # secondary / captions
GRID = "rgba(255,255,255,0.06)"

st.set_page_config(
    page_title="Slipstream Analytics",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# --------------------------------------------------------------------------- styling

st.markdown(
    f"""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');

    html, body, [class*="css"] {{
        font-family: 'Inter', sans-serif;
    }}
    .stApp {{
        background:
            radial-gradient(900px 500px at 15% -8%, rgba(127,90,240,0.18) 0%, rgba(127,90,240,0) 60%),
            radial-gradient(900px 500px at 85% -8%, rgba(255,107,157,0.12) 0%, rgba(255,107,157,0) 60%),
            {BG_APP};
    }}
    .block-container {{
        padding-top: 1.5rem;
        padding-bottom: 3rem;
        max-width: 1200px;
    }}
    #MainMenu, header, footer {{ visibility: hidden; }}

    /* --- glass cards: this styles every st.container(border=True) --- */
    [data-testid="stVerticalBlockBorderWrapper"] {{
        background: rgba(255,255,255,0.05);
        border: 1px solid rgba(255,255,255,0.10) !important;
        border-radius: 20px;
        padding: 1.4rem 1.5rem 1.1rem 1.5rem;
        box-shadow: 0 12px 32px -14px rgba(0,0,0,0.6);
    }}

    .hero {{
        background: {GRADIENT_HERO};
        border-radius: 24px;
        padding: 2.6rem 2.6rem 2.2rem 2.6rem;
        margin-bottom: 1.8rem;
        box-shadow: 0 20px 48px -14px rgba(124, 58, 237, 0.55);
    }}
    .hero h1 {{
        color: white;
        font-size: 2.4rem;
        font-weight: 800;
        margin: 0 0 0.35rem 0;
        letter-spacing: -0.02em;
    }}
    .hero p {{
        color: rgba(255,255,255,0.92);
        font-size: 1.05rem;
        margin: 0;
        font-weight: 500;
    }}
    .hero .badge {{
        display: inline-block;
        background: rgba(255,255,255,0.18);
        color: white;
        border-radius: 100px;
        padding: 0.3rem 0.9rem;
        font-size: 0.78rem;
        font-weight: 600;
        margin-top: 1rem;
        margin-right: 0.5rem;
        letter-spacing: 0.02em;
    }}

    .kpi-card {{
        background: rgba(255,255,255,0.04);
        border-radius: 18px;
        padding: 1.3rem 1.4rem 1.1rem 1.4rem;
        box-shadow: 0 12px 32px -16px rgba(0,0,0,0.6);
        border: 1px solid rgba(255,255,255,0.08);
        position: relative;
        overflow: hidden;
        height: 100%;
    }}
    .kpi-card::before {{
        content: "";
        position: absolute;
        top: 0; left: 0; right: 0;
        height: 5px;
        background: var(--accent, {GRADIENT_ACCENT});
    }}
    .kpi-label {{
        color: {MUTED};
        font-size: 0.78rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.04em;
        margin-bottom: 0.35rem;
    }}
    .kpi-value {{
        font-size: 1.9rem;
        font-weight: 800;
        color: #F5F4FF;
        letter-spacing: -0.02em;
        line-height: 1.1;
    }}
    .kpi-sub {{
        color: {MUTED};
        font-size: 0.78rem;
        font-weight: 500;
        margin-top: 0.3rem;
    }}

    .section-title {{
        font-size: 1.05rem;
        font-weight: 700;
        color: {TEXT};
        margin-bottom: 0.15rem;
    }}
    .section-sub {{
        font-size: 0.82rem;
        color: {MUTED};
        margin-bottom: 0.9rem;
        font-weight: 500;
    }}

    .footer-badge {{
        display: inline-block;
        background: rgba(127,90,240,0.16);
        color: #C9B8FF;
        border: 1px solid rgba(127,90,240,0.25);
        border-radius: 100px;
        padding: 0.35rem 1rem;
        font-size: 0.8rem;
        font-weight: 600;
        margin: 0.2rem 0.3rem 0.2rem 0;
    }}
    .footer-note {{
        color: {MUTED};
        font-size: 0.82rem;
        margin-top: 0.8rem;
        font-weight: 500;
    }}
    </style>
    """,
    unsafe_allow_html=True,
)


# --------------------------------------------------------------------------- chart theme helper

def style_dark(fig):
    """Single source of truth for the dark chart theme. Call on every fig
    right before st.plotly_chart, then never set bgcolor per-figure again."""
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",   # transparent — chart sits on the glass card
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter", color=TEXT),
        legend=dict(font=dict(color=TEXT)),
        margin=dict(l=10, r=10, t=30, b=10),
    )
    fig.update_xaxes(gridcolor=GRID, zeroline=False, linecolor=GRID)
    fig.update_yaxes(gridcolor=GRID, zeroline=False, linecolor=GRID)
    return fig


def card(title, sub):
    """Render the title + subtitle inside the current container."""
    st.markdown(f"<div class='section-title'>{title}</div>", unsafe_allow_html=True)
    st.markdown(f"<div class='section-sub'>{sub}</div>", unsafe_allow_html=True)


# --------------------------------------------------------------------------- data


@st.cache_data(ttl=300)
def load_data():
    con = duckdb.connect(str(DB_PATH), read_only=True)

    customers = con.execute("SELECT COUNT(*) FROM trn_cust_profile").fetchone()[0]

    plans = con.execute("SELECT COUNT(*) FROM trn_plan_master").fetchone()[0]

    revenue_split = con.execute(
        """
        SELECT
            (SELECT SUM(billable_data_amt)  FROM trn_data_plan)  AS data_rev,
            (SELECT SUM(billable_voice_amt) FROM trn_voice_plan) AS voice_rev,
            (SELECT SUM(billable_sms_amt)   FROM trn_voice_plan) AS sms_rev
        """
    ).fetchdf()

    revenue_by_plan = con.execute(
        """
        WITH d AS (
            SELECT plan_type, SUM(billable_data_amt) AS data_rev
            FROM trn_data_plan GROUP BY plan_type
        ),
        v AS (
            SELECT plan_type,
                   SUM(billable_voice_amt) AS voice_rev,
                   SUM(billable_sms_amt)   AS sms_rev
            FROM trn_voice_plan GROUP BY plan_type
        )
        SELECT
            COALESCE(d.plan_type, v.plan_type) AS plan_type,
            COALESCE(d.data_rev, 0)  AS data_rev,
            COALESCE(v.voice_rev, 0) AS voice_rev,
            COALESCE(v.sms_rev, 0)   AS sms_rev
        FROM d FULL OUTER JOIN v ON d.plan_type = v.plan_type
        ORDER BY plan_type
        """
    ).fetchdf()

    by_state = con.execute(
        """
        SELECT state_cd, COUNT(*) AS customers
        FROM trn_cust_profile
        WHERE state_cd IS NOT NULL
        GROUP BY state_cd
        ORDER BY customers DESC
        LIMIT 10
        """
    ).fetchdf()

    missing_state = con.execute(
        "SELECT COUNT(*) FROM trn_cust_profile WHERE state_cd IS NULL"
    ).fetchone()[0]

    loyalty = con.execute(
        """
        SELECT loyalty_badge, COUNT(*) AS customers, SUM(loyalty_spent) AS total_spent
        FROM trn_loyalty
        GROUP BY loyalty_badge
        ORDER BY total_spent DESC
        """
    ).fetchdf()

    payments = con.execute(
        """
        SELECT DATE_TRUNC('month', paid_date) AS month, COUNT(*) AS payments
        FROM trn_cust_status
        WHERE paid_date IS NOT NULL
        GROUP BY 1
        ORDER BY 1
        """
    ).fetchdf()

    try:
        row_history = con.execute(
            """
            SELECT run_ts, table_name, row_count
            FROM quality_row_history
            ORDER BY run_ts
            """
        ).fetchdf()
        run_count = con.execute(
            "SELECT COUNT(DISTINCT run_ts) FROM quality_row_history"
        ).fetchone()[0]
    except Exception:
        row_history, run_count = pd.DataFrame(), 0

    con.close()
    return {
        "customers": customers,
        "plans": plans,
        "revenue_split": revenue_split,
        "revenue_by_plan": revenue_by_plan,
        "by_state": by_state,
        "missing_state": missing_state,
        "loyalty": loyalty,
        "payments": payments,
        "row_history": row_history,
        "run_count": run_count,
    }


def load_test_status():
    """Best-effort read of the last dbt test run. Never raises."""
    try:
        with open(DBT_RESULTS) as f:
            results = json.load(f).get("results", [])
        tests = [r for r in results if r.get("unique_id", "").startswith("test.")]
        if not tests:
            return None, None
        failed = sum(1 for r in tests if r.get("status") not in ("pass", "success"))
        return len(tests) - failed, len(tests)
    except Exception:
        return None, None


data = load_data()
tests_passed, tests_total = load_test_status()

# --------------------------------------------------------------------------- hero

st.markdown(
    f"""
    <div class="hero">
        <h1>⚡ Slipstream Analytics</h1>
        <p>A legacy Teradata billing pipeline, modernized on Python, DuckDB, dbt &amp; Airflow.</p>
        <span class="badge">🔗 Live from slipstream.duckdb</span>
        <span class="badge">🕐 {datetime.now():%d %b %Y, %H:%M}</span>
    </div>
    """,
    unsafe_allow_html=True,
)

# --------------------------------------------------------------------------- KPIs

total_rev = float(data["revenue_split"].iloc[0].sum())
tests_label = f"{tests_passed}/{tests_total}" if tests_total else "—"

kpis = [
    ("💰", "Total billable", f"${total_rev/1e6:,.1f}M", "data + voice + SMS", PALETTE[0]),
    ("👥", "Customers", f"{data['customers']:,}", "active subscriber profiles", PALETTE[1]),
    ("📋", "Active plans", f"{data['plans']}", "rate-card tiers", PALETTE[2]),
    ("🔁", "Pipeline runs", f"{data['run_count']}", "recorded by the quality gate", PALETTE[3]),
    ("✅", "Data tests", tests_label, "last dbt test run", PALETTE[4]),
]

cols = st.columns(5)
for col, (icon, label, value, sub, color) in zip(cols, kpis):
    with col:
        st.markdown(
            f"""
            <div class="kpi-card" style="--accent:{color}">
                <div class="kpi-label">{icon}&nbsp; {label}</div>
                <div class="kpi-value">{value}</div>
                <div class="kpi-sub">{sub}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

st.markdown(
    "<div class='footer-note' style='margin:0.8rem 0 1.4rem 0;'>"
    "* Billable totals reflect synthetic demo usage volumes and are not representative "
    "of real billing figures — see the project README for details."
    "</div>",
    unsafe_allow_html=True,
)

# per-plan colour map so the bar and the donut share colours (PLN-500 = same hue in both)
rbp = data["revenue_by_plan"].copy()
rbp["total"] = rbp[["data_rev", "voice_rev", "sms_rev"]].sum(axis=1)
plan_order = rbp.sort_values("total", ascending=False)["plan_type"].tolist()
plan_color = {p: PALETTE[i % len(PALETTE)] for i, p in enumerate(plan_order)}

# --------------------------------------------------------------------------- revenue by plan

with st.container(border=True):
    card("Revenue by plan", "Total billable amount by plan tier")
    order_df = rbp.sort_values("total", ascending=False)
    fig = go.Figure(
        go.Bar(
            x=order_df["plan_type"],
            y=order_df["total"],
            marker_color=[plan_color[p] for p in order_df["plan_type"]],
            hovertemplate="%{x}<br>$%{y:,.0f}<extra></extra>",
        )
    )
    fig.update_layout(height=360, yaxis=dict(tickprefix="$"), xaxis=dict(title=None))
    st.plotly_chart(style_dark(fig), use_container_width=True, config={"displayModeBar": False})

# --------------------------------------------------------------------------- share + loyalty

c1, c2 = st.columns(2)

with c1:
    with st.container(border=True):
        card("Revenue share by plan", "Each tier's slice of total billable")
        fig = go.Figure(
            go.Pie(
                labels=rbp["plan_type"],
                values=rbp["total"],
                hole=0.62,
                sort=False,
                marker=dict(colors=[plan_color[p] for p in rbp["plan_type"]]),
                textinfo="label+percent",
                textfont=dict(family="Inter", size=13, color="white"),
                hovertemplate="%{label}<br>$%{value:,.0f} (%{percent})<extra></extra>",
            )
        )
        fig.update_layout(
            height=320,
            showlegend=False,
            annotations=[
                dict(text=f"${total_rev/1e6:,.0f}M", x=0.5, y=0.5, font_size=22,
                     font_family="Inter", font_color=TEXT, showarrow=False)
            ],
        )
        st.plotly_chart(style_dark(fig), use_container_width=True, config={"displayModeBar": False})

with c2:
    with st.container(border=True):
        card("Loyalty tier value", "Lifetime spend by loyalty badge")
        ldf = data["loyalty"].copy()
        # semantic tier order + per-tier colour, so height (spend) and label agree
        tier_order = ["Bronze", "Silver", "Gold", "Platinum"]
        tier_color = {"Bronze": "#FF9F5A", "Silver": "#9B96B8",
                      "Gold": "#FFD166", "Platinum": "#2CD9C5"}
        ldf["loyalty_badge"] = pd.Categorical(
            ldf["loyalty_badge"], categories=tier_order, ordered=True
        )
        ldf = ldf.sort_values("loyalty_badge")
        fig = go.Figure(
            go.Bar(
                x=ldf["loyalty_badge"].astype(str),
                y=ldf["total_spent"],
                marker_color=[tier_color.get(b, PALETTE[0]) for b in ldf["loyalty_badge"]],
                text=ldf["total_spent"].map(lambda v: f"${v/1e3:,.1f}k"),
                textposition="outside",
                customdata=ldf["customers"],
                hovertemplate="%{x}<br>Spend $%{y:,.0f}<br>%{customdata} customers<extra></extra>",
            )
        )
        fig.update_layout(
            height=320,
            yaxis=dict(title="Lifetime spend", tickprefix="$"),
            xaxis=dict(title=None),
        )
        st.plotly_chart(style_dark(fig), use_container_width=True, config={"displayModeBar": False})

# --------------------------------------------------------------------------- state + payments

c1, c2 = st.columns(2)

with c1:
    with st.container(border=True):
        sub = "Top 10 states by subscriber count"
        if data["missing_state"]:
            sub += f" · {data['missing_state']} customer(s) have no address on file"
        card("Customers by state", sub)

        sdf = data["by_state"].sort_values("customers")
        fig = go.Figure(
            go.Bar(
                x=sdf["customers"],
                y=sdf["state_cd"],
                orientation="h",
                marker=dict(
                    color=sdf["customers"],
                    colorscale=[[0, PALETTE[0]], [1, PALETTE[1]]],
                ),
                hovertemplate="%{y}: %{x} customers<extra></extra>",
            )
        )
        fig.update_layout(height=340, xaxis=dict(title=None), yaxis=dict(title=None))
        st.plotly_chart(style_dark(fig), use_container_width=True, config={"displayModeBar": False})

with c2:
    with st.container(border=True):
        card("Payments over time", "Payments recorded per month, from trn_cust_status")
        pdf = data["payments"]
        fig = go.Figure(
            go.Scatter(
                x=pdf["month"],
                y=pdf["payments"],
                mode="lines",
                line=dict(width=0),
                fill="tozeroy",
                fillcolor="rgba(127,90,240,0.22)",
            )
        )
        fig.add_trace(
            go.Scatter(
                x=pdf["month"],
                y=pdf["payments"],
                mode="lines+markers",
                line=dict(color=PALETTE[0], width=3),
                marker=dict(size=6, color=PALETTE[0]),
                showlegend=False,
                hovertemplate="%{x|%b %Y}: %{y} payments<extra></extra>",
            )
        )
        fig.update_layout(height=340, showlegend=False,
                          yaxis=dict(title=None), xaxis=dict(title=None))
        st.plotly_chart(style_dark(fig), use_container_width=True, config={"displayModeBar": False})

# --------------------------------------------------------------------------- pipeline health

with st.container(border=True):
    card("Pipeline health",
         "Row counts recorded by the data-quality gate on every pipeline run")

    rh = data["row_history"]
    if rh.empty:
        st.info("No pipeline run history yet — run the DAG (or `python quality/report.py`) at least once.")
    else:
        labels = {
            "stg_cust_profile": "Customer profile", "stg_cust_status": "Customer status",
            "stg_loyalty": "Loyalty", "stg_plan_master": "Plan master",
            "stg_data_plan": "Data usage", "stg_voice_plan": "Voice usage",
        }
        rh = rh.copy()
        rh["label"] = rh["table_name"].map(lambda t: labels.get(t, t))
        rh["run_no"] = rh["run_ts"].rank(method="dense").astype(int)

        fig = px.line(
            rh, x="run_no", y="row_count", color="label", markers=True,
            color_discrete_sequence=PALETTE,
        )
        fig.update_layout(
            height=340,
            legend=dict(orientation="h", yanchor="bottom", y=1.02,
                        xanchor="right", x=1, title=None),
            yaxis=dict(title="rows"),
            xaxis=dict(title="pipeline run #", dtick=1),
        )
        st.plotly_chart(style_dark(fig), use_container_width=True, config={"displayModeBar": False})

# --------------------------------------------------------------------------- footer

st.markdown(
    """
    <div style="margin-top: 1.2rem;">
        <span class="footer-badge">🐍 Python</span>
        <span class="footer-badge">🦆 DuckDB</span>
        <span class="footer-badge">🔧 dbt</span>
        <span class="footer-badge">🌀 Airflow</span>
        <span class="footer-badge">💬 Slack alerting</span>
        <span class="footer-badge">✅ 26 data tests</span>
    </div>
    <div class="footer-note">
        Ingestion → dbt transformation → data-quality gate → Airflow orchestration →
        this dashboard. Full source and write-up on GitHub.
    </div>
    """,
    unsafe_allow_html=True,
)
