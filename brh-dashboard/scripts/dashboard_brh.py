"""BRH Haiti Banking System Dashboard.

Run:
    streamlit run scripts/dashboard_brh.py

Requires:
    pip install streamlit plotly pandas openpyxl

The CSV must be generated first:
    python scripts/parse_brh_ratios.py
"""

from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

# ── Config ────────────────────────────────────────────────────────────────────

DATA_FILE    = Path(__file__).parent.parent / "data" / "processed" / "brh_ratios.csv"
FX_DATA_FILE = Path(__file__).parent.parent / "data" / "processed" / "brh_fx_positions.csv"

# Individual banks to show in comparisons (excludes system-level subtotals)
INDIVIDUAL_BANKS = ["BNC", "BPH", "BUH", "CAPITALBK", "SOGEBK", "UNIBNK", "SOGEBL", "CBNA"]

# Display configuration per metric
METRIC_CONFIG: dict[str, dict] = {
    "npl_ratio_gross": {
        "label": "Gross NPL Ratio",
        "unit": "%",
        "scale": 100,
        "higher_is_worse": True,
        "warning_line": 10.0,
        "warning_label": "10% alert",
        "description": "Non-performing loans as % of gross loans. Lower is better.",
    },
    "provision_coverage": {
        "label": "Provision Coverage",
        "unit": "x",
        "scale": 1,
        "higher_is_worse": False,
        "warning_line": 1.0,
        "warning_label": "1x minimum",
        "description": "Loan loss provisions ÷ gross NPLs. ≥1 means fully covered.",
    },
    "equity_to_assets": {
        "label": "Capital Ratio (Equity / Assets)",
        "unit": "%",
        "scale": 100,
        "higher_is_worse": False,
        "warning_line": None,
        "description": "Shareholders' equity as % of total assets.",
    },
    "deposits_to_assets": {
        "label": "Deposits / Total Assets",
        "unit": "%",
        "scale": 100,
        "higher_is_worse": False,
        "warning_line": None,
        "description": "Total deposits as % of total assets.",
    },
    "net_npl_to_equity": {
        "label": "Net NPL / Equity",
        "unit": "%",
        "scale": 100,
        "higher_is_worse": True,
        "warning_line": None,
        "description": "Net NPLs (after provisions) as % of equity.",
    },
    "roa_cumul": {
        "label": "ROA — Fiscal Year",
        "unit": "%",
        "scale": 100,
        "higher_is_worse": False,
        "warning_line": None,
        "description": "Return on assets (cumulative fiscal year).",
    },
    "roe_cumul": {
        "label": "ROE — Fiscal Year",
        "unit": "%",
        "scale": 100,
        "higher_is_worse": False,
        "warning_line": None,
        "description": "Return on equity (cumulative fiscal year).",
    },
    "nim_cumul": {
        "label": "Net Interest Margin — Fiscal Year",
        "unit": "%",
        "scale": 100,
        "higher_is_worse": False,
        "warning_line": None,
        "description": "Net interest revenue as % of gross interest revenue.",
    },
    "avg_loan_yield_c": {
        "label": "Average Loan Yield — Fiscal Year",
        "unit": "%",
        "scale": 100,
        "higher_is_worse": False,
        "warning_line": None,
        "description": "Average annual interest rate earned on the loan portfolio.",
    },
    "avg_deposit_rate_c": {
        "label": "Average Deposit Rate — Fiscal Year",
        "unit": "%",
        "scale": 100,
        "higher_is_worse": False,
        "warning_line": None,
        "description": "Average annual interest rate paid on deposits.",
    },
    "cost_to_income_c": {
        "label": "Cost-to-Income — Fiscal Year",
        "unit": "%",
        "scale": 100,
        "higher_is_worse": True,
        "warning_line": 80.0,
        "warning_label": "80% alert",
        "description": "Operating expenses as % of net banking income (interest + fees).",
    },
    "productivity_c": {
        "label": "Employee Productivity — Fiscal Year (HTG '000)",
        "unit": "HTG '000",
        "scale": 1,
        "higher_is_worse": False,
        "warning_line": None,
        "description": "Net banking income per employee (thousands of HTG).",
    },
}

# Metrics shown on the System Overview tab (KPI cards + time-series grid)
OVERVIEW_METRICS = [
    "npl_ratio_gross",
    "provision_coverage",
    "equity_to_assets",
    "cost_to_income_c",
    "roa_cumul",
    "roe_cumul",
]

PLOTLY_TEMPLATE = "plotly_white"


# ── Data helpers ──────────────────────────────────────────────────────────────

@st.cache_data
def load_data() -> pd.DataFrame:
    df = pd.read_csv(DATA_FILE, parse_dates=["date"])
    return df


FX_STRUCTURAL_LIMIT = 0.50   # % of equity — Circulaire 81-6 (June 2019)


@st.cache_data
def load_fx_data() -> pd.DataFrame:
    df = pd.read_csv(FX_DATA_FILE, parse_dates=["date"])
    # Add fiscal year (Haiti FY = Oct–Sep; months ≥ Oct belong to the *next* FY)
    df["fy"] = df["date"].apply(lambda d: d.year + 1 if d.month >= 10 else d.year)
    # Classify each month-bank observation by violation type.
    # "structural": end-of-month position exceeded the 0.50% structural limit.
    # "cambiste":   position was within the structural limit but days_exceeded > 0,
    #               meaning the intraday trading position (position cambiste) was
    #               non-zero at end of some business days during the month.
    # Caveat: before Circulaire 81-6 (June 2019) the structural limit was tighter,
    # so some pre-2019 "cambiste" obs. with positions near 0.50% may actually be
    # structural violations under the then-applicable circulaire.
    df["violation_type"] = "compliant"
    df.loc[
        (df["fx_position"] > FX_STRUCTURAL_LIMIT) & (df["days_exceeded"] > 0),
        "violation_type",
    ] = "structural"
    df.loc[
        (df["fx_position"].fillna(0) <= FX_STRUCTURAL_LIMIT) & (df["days_exceeded"] > 0),
        "violation_type",
    ] = "cambiste"
    return df


def system_series(df: pd.DataFrame, metric: str) -> pd.DataFrame:
    """Time series for the consolidated SYSTÈME bank."""
    return (
        df[(df["metric"] == metric) & (df["bank"] == "SYSTÈME")]
        .dropna(subset=["value"])
        .sort_values("date")
        .copy()
    )


def latest_by_bank(df: pd.DataFrame, metric: str) -> pd.DataFrame:
    """Most recent value for each individual bank."""
    sub = df[(df["metric"] == metric) & (df["bank"].isin(INDIVIDUAL_BANKS))].copy()
    if sub.empty:
        return sub
    latest_date = sub["date"].max()
    return sub[sub["date"] == latest_date].dropna(subset=["value"]).copy()


def fmt(val: float, cfg: dict) -> str:
    """Format a raw value for display."""
    scaled = val * cfg["scale"]
    if cfg["unit"] == "%":
        return f"{scaled:.1f}%"
    elif cfg["unit"] == "x":
        return f"{scaled:.2f}x"
    else:
        return f"{scaled:,.0f}"


def delta_color(val_now: float, val_prev: float, higher_is_worse: bool) -> str:
    """Return Streamlit delta_color string (normal = green arrow up)."""
    improved = val_now < val_prev if higher_is_worse else val_now > val_prev
    return "normal" if improved else "inverse"


def line_chart(
    s: pd.DataFrame,
    cfg: dict,
    height: int = 280,
    show_warning: bool = True,
) -> go.Figure:
    """Return a Plotly line chart for a system-level time series."""
    y = s["value"] * cfg["scale"]
    fig = px.line(
        s, x="date", y=y,
        markers=True,
        labels={"y": cfg["unit"], "date": ""},
        template=PLOTLY_TEMPLATE,
    )
    fig.update_traces(line_color="#2c7bb6", marker_color="#2c7bb6")
    if show_warning and cfg.get("warning_line") is not None:
        fig.add_hline(
            y=cfg["warning_line"],
            line_dash="dash",
            line_color="#e74c3c",
            annotation_text=cfg.get("warning_label", ""),
            annotation_position="bottom right",
        )
    fig.update_layout(margin=dict(t=8, b=8, l=8, r=8), height=height)
    return fig


# ── App ───────────────────────────────────────────────────────────────────────

def main() -> None:
    st.set_page_config(
        page_title="Haiti BRH — Banking System Monitor",
        layout="wide",
        initial_sidebar_state="collapsed",
    )

    st.title("Haiti — Banking System Monitor")
    st.caption(
        "Source: Banque de la République d'Haïti (BRH), Direction de la Supervision  |  "
        "All ratios from `sysratfinclé` quarterly sheets"
    )

    if not DATA_FILE.exists():
        st.error(
            f"**Data file not found:** `{DATA_FILE}`\n\n"
            "Run the parser first:\n```\npython scripts/parse_brh_ratios.py\n```"
        )
        return

    df = load_data()
    latest_date = df["date"].max()

    st.sidebar.markdown(f"**Latest data:** {latest_date.strftime('%B %Y')}")
    st.sidebar.markdown(
        f"**Date range:** {df['date'].min().strftime('%b %Y')} — "
        f"{df['date'].max().strftime('%b %Y')}"
    )
    st.sidebar.markdown(f"**Sheets parsed:** {df['date'].nunique()}")

    tab_overview, tab_banks, tab_profitability, tab_fx, tab_data = st.tabs(
        ["System Overview", "Bank Comparison", "Profitability Detail", "FX Positions", "Raw Data"]
    )

    # ── Tab 1: System Overview ────────────────────────────────────────────────

    with tab_overview:
        st.subheader(f"System-Wide Indicators — {latest_date.strftime('%B %Y')}")

        # KPI cards (top row)
        kpi_keys = ["npl_ratio_gross", "provision_coverage", "equity_to_assets", "cost_to_income_c"]
        cols = st.columns(len(kpi_keys))
        for i, mkey in enumerate(kpi_keys):
            cfg = METRIC_CONFIG[mkey]
            s = system_series(df, mkey)
            if s.empty:
                cols[i].metric(cfg["label"], "N/A")
                continue
            val_now = s.iloc[-1]["value"]
            val_prev = s.iloc[-2]["value"] if len(s) > 1 else val_now
            delta_val = (val_now - val_prev) * cfg["scale"]
            cols[i].metric(
                label=cfg["label"],
                value=fmt(val_now, cfg),
                delta=f"{delta_val:+.2f}{cfg['unit']}",
                delta_color=delta_color(val_now, val_prev, cfg["higher_is_worse"]),
                help=cfg["description"],
            )

        st.divider()

        # 2×3 time-series grid
        grid_metrics = [
            ("npl_ratio_gross",   "Gross NPL Ratio"),
            ("provision_coverage","Provision Coverage"),
            ("equity_to_assets",  "Capital Ratio — Equity / Assets"),
            ("cost_to_income_c",  "Cost-to-Income — Fiscal Year"),
            ("roa_cumul",         "Return on Assets — Fiscal Year"),
            ("roe_cumul",         "Return on Equity — Fiscal Year"),
        ]
        for row_idx in range(0, len(grid_metrics), 2):
            left, right = st.columns(2)
            for col_widget, (mkey, title) in zip(
                [left, right], grid_metrics[row_idx: row_idx + 2]
            ):
                cfg = METRIC_CONFIG[mkey]
                s = system_series(df, mkey)
                col_widget.markdown(f"**{title}** ({cfg['unit']})")
                if s.empty:
                    col_widget.info("No data")
                else:
                    col_widget.plotly_chart(
                        line_chart(s, cfg), use_container_width=True
                    )

    # ── Tab 2: Bank Comparison ────────────────────────────────────────────────

    with tab_banks:
        st.subheader("Bank-Level Comparison")

        metric_choice = st.selectbox(
            "Select metric",
            options=list(METRIC_CONFIG.keys()),
            format_func=lambda k: METRIC_CONFIG[k]["label"],
        )
        cfg = METRIC_CONFIG[metric_choice]

        # Latest cross-section — horizontal bar chart
        lb = latest_by_bank(df, metric_choice)
        if lb.empty:
            st.info("No bank-level data available for this metric.")
        else:
            lb["display"] = lb["value"] * cfg["scale"]
            lb = lb.sort_values("display")

            # Colour bars red if metric is above warning threshold (for worse-is-higher)
            if cfg["higher_is_worse"] and cfg.get("warning_line"):
                lb["color"] = lb["display"].apply(
                    lambda v: "#e74c3c" if v > cfg["warning_line"] else "#3498db"
                )
            else:
                lb["color"] = "#3498db"

            fig = go.Figure(go.Bar(
                x=lb["display"],
                y=lb["bank"],
                orientation="h",
                marker_color=lb["color"],
                text=lb["display"].apply(
                    lambda v: f"{v:.1f}{cfg['unit']}" if cfg["unit"] == "%" else f"{v:.2f}"
                ),
                textposition="outside",
            ))
            fig.update_layout(
                title=f"{cfg['label']} — {latest_date.strftime('%B %Y')}",
                height=350,
                template=PLOTLY_TEMPLATE,
                margin=dict(l=80, r=80, t=40, b=20),
                xaxis_title=cfg["unit"],
                yaxis_title="",
            )
            if cfg.get("warning_line") is not None:
                fig.add_vline(
                    x=cfg["warning_line"],
                    line_dash="dash",
                    line_color="#e74c3c",
                    annotation_text=cfg.get("warning_label", ""),
                )
            st.plotly_chart(fig, use_container_width=True)

        st.divider()

        # Time series by bank
        st.markdown(f"**{cfg['label']} over time — by bank**")
        bank_ts = df[
            (df["metric"] == metric_choice) & (df["bank"].isin(INDIVIDUAL_BANKS))
        ].dropna(subset=["value"]).copy()
        bank_ts["display"] = bank_ts["value"] * cfg["scale"]

        # For readability, cap extreme outliers in the chart (provision coverage can hit 19x+)
        if metric_choice == "provision_coverage":
            bank_ts["display"] = bank_ts["display"].clip(upper=5)
            st.caption("Note: Values capped at 5x for readability. Individual banks with near-zero NPLs may have extremely high coverage ratios.")

        fig2 = px.line(
            bank_ts,
            x="date", y="display", color="bank",
            markers=False,
            labels={"display": cfg["unit"], "date": "", "bank": ""},
            template=PLOTLY_TEMPLATE,
            height=380,
        )
        fig2.update_layout(margin=dict(t=8, b=8))
        st.plotly_chart(fig2, use_container_width=True)

    # ── Tab 3: Profitability Detail ───────────────────────────────────────────

    with tab_profitability:
        st.subheader("Profitability & Efficiency — System Total")

        prof_pairs = [
            ("roa_cumul",        "ROA — Fiscal Year (%)"),
            ("roe_cumul",        "ROE — Fiscal Year (%)"),
            ("nim_cumul",        "Net Interest Margin (%)"),
            ("cost_to_income_c", "Cost-to-Income (%)"),
            ("avg_loan_yield_c", "Avg Loan Yield (%)"),
            ("avg_deposit_rate_c","Avg Deposit Rate (%)"),
        ]

        fig = make_subplots(
            rows=3, cols=2,
            subplot_titles=[label for _, label in prof_pairs],
            vertical_spacing=0.10,
            horizontal_spacing=0.08,
        )

        for i, (mkey, title) in enumerate(prof_pairs):
            row, col = divmod(i, 2)
            s = system_series(df, mkey)
            if s.empty:
                continue
            cfg = METRIC_CONFIG[mkey]
            fig.add_trace(
                go.Scatter(
                    x=s["date"],
                    y=s["value"] * cfg["scale"],
                    mode="lines+markers",
                    name=title,
                    showlegend=False,
                    line_color="#2c7bb6",
                ),
                row=row + 1, col=col + 1,
            )

        fig.update_layout(
            height=760,
            template=PLOTLY_TEMPLATE,
            margin=dict(t=40, b=20),
        )
        st.plotly_chart(fig, use_container_width=True)

        # Interest rate spread
        st.divider()
        st.markdown("**Interest Rate Spread (Avg Loan Yield − Avg Deposit Rate)**")
        loan_s  = system_series(df, "avg_loan_yield_c").set_index("date")["value"]
        dep_s   = system_series(df, "avg_deposit_rate_c").set_index("date")["value"]
        spread  = (loan_s - dep_s).dropna() * 100
        if not spread.empty:
            fig_sp = px.area(
                spread.reset_index(),
                x="date", y="value",
                labels={"value": "%", "date": ""},
                template=PLOTLY_TEMPLATE,
                height=280,
            )
            fig_sp.update_traces(fillcolor="rgba(44,123,182,0.15)", line_color="#2c7bb6")
            fig_sp.update_layout(margin=dict(t=8, b=8))
            st.plotly_chart(fig_sp, use_container_width=True)

    # ── Tab 4: FX Positions ───────────────────────────────────────────────────

    with tab_fx:
        st.subheader("Net FX Open Positions — Circulaire 81-3 / 81-6")

        with st.expander("Regulatory framework", expanded=False):
            st.markdown(
                """
**Regulatory limit:** Under **Circulaire 81-6** (issued 29 May 2019, in force 10 June 2019 — the
current regime), the cumulative net FX open position (long + short, balance sheet only; off-balance
sheet excluded) must not exceed **0.50% of accounting equity** at any point in time.
The *position cambiste* (intraday trading desk position) must be zero at end of each business day.

**Evolution of the regulatory framework:**
- **Circulaire 81-2** (pre-2004): original framework
- **Circulaire 81-3** (~2004–2010): stricter daily monitoring regime
- **Circulaire 81-4** (~2010–2017): intermediate revision
- **Circulaire 81-5** (April 2017–June 2019): further tightening
- **Circulaire 81-6** (from June 2019): current 0.50%-of-equity structural limit

**Metric definitions:**
- *Position* (end-of-month): net structural FX open position as a % of accounting equity,
  measured on the last day of the month. End-of-month compliance does not imply intra-month
  compliance — a bank can overshoot intraday and close back within the limit by month-end.
- *Days exceeded*: calendar days in the month during which the bank was non-compliant with
  the applicable circulaire (structural position > 0.50% of equity, or *position cambiste* ≠ 0
  at end of day). Zero = fully compliant; 31 = violated every single day.
  Note: some banks show 0.00% end-of-month position but non-zero days exceeded — this reflects
  intraday cambiste violations that were closed out before month-end.

CBNA = Citibank National Association (ceased operations in Haiti ~2024).
                """
            )
        st.caption(
            "Position = end-of-month net FX open position as % of accounting equity.  "
            "Days exceeded = calendar days the bank was non-compliant with the applicable circulaire.  "
            "Red dashed line = 0.50% structural limit (Circulaire 81-6, June 2019)."
        )

        if not FX_DATA_FILE.exists():
            st.error(
                f"**FX data not found:** `{FX_DATA_FILE}`\n\n"
                "Run: `python scripts/parse_brh_fx.py`"
            )
        else:
            df_fx = load_fx_data()
            latest_fx_date = df_fx["date"].max()
            latest_fx = df_fx[df_fx["date"] == latest_fx_date].copy()

            # ── KPI row ───────────────────────────────────────────────────────
            n_banks_latest  = latest_fx["bank"].nunique()
            n_breach_latest = (latest_fx["days_exceeded"] > 0).sum()
            total_breach_days = int(df_fx["days_exceeded"].sum())
            worst_bank = (
                df_fx.groupby("bank")["days_exceeded"].sum().idxmax()
            )

            k1, k2, k3, k4 = st.columns(4)
            k1.metric(
                "Banks reporting (latest month)",
                n_banks_latest,
                help=f"Latest data: {latest_fx_date.strftime('%B %Y')}",
            )
            k2.metric(
                "Banks with breach — latest month",
                n_breach_latest,
                help="Banks that exceeded the Circulaire limit ≥1 day in the latest month",
            )
            k3.metric(
                "Total breach-days (all history)",
                f"{total_breach_days:,}",
                help="Sum of all days_exceeded across all banks and months since Oct 1999",
            )
            k4.metric(
                "Most violations (all time)",
                worst_bank,
                help="Bank with the highest cumulative days_exceeded since 1999",
            )

            st.divider()

            # ── Latest month snapshot ──────────────────────────────────────
            col_l, col_r = st.columns(2)

            with col_l:
                st.markdown(
                    f"**End-of-month FX position — {latest_fx_date.strftime('%B %Y')}**  "
                    f"(% of equity, red = limit breached that month)"
                )
                snap = (
                    latest_fx
                    .dropna(subset=["fx_position"])
                    .assign(pct=lambda d: d["fx_position"])   # values are already % of equity
                    .sort_values("pct", ascending=True)
                )
                snap["color"] = snap["days_exceeded"].apply(
                    lambda d: "#e74c3c" if d > 0 else "#27ae60"
                )
                snap["label"] = snap.apply(
                    lambda r: f"{r['pct']:.1f}%  ({int(r['days_exceeded'])}d)"
                    if r["days_exceeded"] > 0 else f"{r['pct']:.1f}%",
                    axis=1,
                )
                fig_snap = go.Figure(go.Bar(
                    x=snap["pct"],
                    y=snap["bank"],
                    orientation="h",
                    marker_color=snap["color"],
                    text=snap["label"],
                    textposition="outside",
                ))
                fig_snap.update_layout(
                    height=300,
                    template=PLOTLY_TEMPLATE,
                    margin=dict(t=8, b=8, l=80, r=120),
                    xaxis_title="% of equity",
                )
                fig_snap.add_vline(
                    x=0.50, line_dash="dash", line_color="#e74c3c",
                    annotation_text="0.50% limit", annotation_position="top right",
                )
                st.plotly_chart(fig_snap, use_container_width=True)

            with col_r:
                st.markdown(
                    "**Breach days — latest month**  "
                    "(calendar days the intra-day limit was exceeded)"
                )
                snap2 = latest_fx.sort_values("days_exceeded", ascending=True)
                snap2["color"] = snap2["days_exceeded"].apply(
                    lambda d: "#e74c3c" if d > 10 else ("#e67e22" if d > 0 else "#27ae60")
                )
                fig_days = go.Figure(go.Bar(
                    x=snap2["days_exceeded"],
                    y=snap2["bank"],
                    orientation="h",
                    marker_color=snap2["color"],
                    text=snap2["days_exceeded"].astype(int),
                    textposition="outside",
                ))
                fig_days.update_layout(
                    height=300,
                    template=PLOTLY_TEMPLATE,
                    margin=dict(t=8, b=8, l=80, r=60),
                    xaxis_title="Days",
                )
                st.plotly_chart(fig_days, use_container_width=True)

            st.divider()

            # ── Quarterly breach heatmap ───────────────────────────────────
            st.markdown(
                "**Quarterly breach days by bank — full history**  "
                "(green = compliant; yellow/orange/red = escalating violations; max ~92 days/quarter)"
            )

            heatmap_view = st.radio(
                "Show:",
                options=["All violations", "Structural only (position > 0.50%)",
                         "Cambiste only (position ≤ 0.50%)"],
                horizontal=True,
                key="fx_heatmap_view",
            )
            st.caption(
                "**Structural**: balance-sheet FX position exceeded the 0.50% limit at month-end.  "
                "**Cambiste**: intraday trading position was non-zero at end of some business days "
                "(structural position within limits by month-end).  "
                "⚠️ Pre-2019 cases with position 0.10–0.50% may be misclassified as cambiste "
                "if the then-applicable limit was below 0.50%."
            )

            if heatmap_view == "Structural only (position > 0.50%)":
                df_heat = df_fx[df_fx["violation_type"] == "structural"]
            elif heatmap_view == "Cambiste only (position ≤ 0.50%)":
                df_heat = df_fx[df_fx["violation_type"] == "cambiste"]
            else:
                df_heat = df_fx[df_fx["days_exceeded"] > 0]

            def _quarter_end(d: pd.Timestamp) -> pd.Timestamp:
                """Map a monthly date to the last day of its BRH fiscal quarter."""
                m = d.month
                if m in [10, 11, 12]:
                    return pd.Timestamp(year=d.year, month=12, day=31)
                elif m in [1, 2, 3]:
                    return pd.Timestamp(year=d.year, month=3, day=31)
                elif m in [4, 5, 6]:
                    return pd.Timestamp(year=d.year, month=6, day=30)
                else:
                    return pd.Timestamp(year=d.year, month=9, day=30)

            def _qlabel(d: pd.Timestamp) -> str:
                m = d.month
                fy = d.year + 1 if m >= 10 else d.year
                q = {12: 1, 3: 2, 6: 3, 9: 4}[m]
                return f"Q{q} FY{str(fy)[2:]}"

            # Build the quarterly pivot from the filtered data
            # Start from ALL banks/quarters so compliant cells show as 0
            df_fx_q = df_fx.copy()
            df_fx_q["qend"] = df_fx_q["date"].apply(_quarter_end)
            all_bank_q = (
                df_fx_q.groupby(["bank", "qend"])["days_exceeded"]
                .sum().unstack(fill_value=0)
            )
            if not df_heat.empty:
                df_heat_q = df_heat.copy()
                df_heat_q["qend"] = df_heat_q["date"].apply(_quarter_end)
                filtered_pivot = (
                    df_heat_q.groupby(["bank", "qend"])["days_exceeded"]
                    .sum().unstack(fill_value=0)
                )
                # Reindex to full bank × quarter grid, fill missing with 0
                pivot = filtered_pivot.reindex(
                    index=all_bank_q.index,
                    columns=all_bank_q.columns,
                    fill_value=0,
                )
            else:
                pivot = all_bank_q * 0   # all zeros if no matching data

            pivot.columns = [_qlabel(c) for c in pivot.columns]

            # Custom colorscale: 0 → pale green; chronic → dark red
            fx_colorscale = [
                [0.000, "#d5f5e3"],
                [0.015, "#f9e79f"],
                [0.100, "#f0b27a"],
                [0.400, "#e74c3c"],
                [1.000, "#7b241c"],
            ]
            fig_heat = px.imshow(
                pivot,
                labels=dict(x="Quarter", y="Bank", color="Breach Days"),
                color_continuous_scale=fx_colorscale,
                aspect="auto",
                template=PLOTLY_TEMPLATE,
            )
            fig_heat.update_layout(
                height=max(300, len(pivot) * 30 + 80),
                margin=dict(t=20, b=60, l=80, r=20),
                xaxis=dict(tickangle=45, tickfont_size=9),
            )
            st.plotly_chart(fig_heat, use_container_width=True)

            # ── Structural vs. cambiste breakdown bar ──────────────────────
            st.markdown(
                "**Breach days by type — all-time totals per bank**  "
                "(structural = balance-sheet position > 0.50%; cambiste = intraday position only)"
            )
            breakdown = (
                df_fx[df_fx["violation_type"] != "compliant"]
                .groupby(["bank", "violation_type"])["days_exceeded"]
                .sum()
                .reset_index()
            )
            # Sort banks by total breach days descending
            bank_order = (
                breakdown.groupby("bank")["days_exceeded"].sum()
                .sort_values(ascending=True).index.tolist()
            )
            fig_breakdown = px.bar(
                breakdown,
                x="days_exceeded",
                y="bank",
                color="violation_type",
                orientation="h",
                color_discrete_map={
                    "structural": "#e74c3c",
                    "cambiste":   "#f39c12",
                },
                category_orders={"bank": bank_order},
                labels={
                    "days_exceeded": "Total breach days",
                    "bank": "",
                    "violation_type": "Type",
                },
                template=PLOTLY_TEMPLATE,
                height=max(280, len(bank_order) * 32 + 80),
            )
            fig_breakdown.update_layout(margin=dict(t=8, b=8, l=80, r=20))
            st.plotly_chart(fig_breakdown, use_container_width=True)

            st.divider()

            # ── Time-series charts (user-selected banks) ───────────────────
            all_fx_banks = sorted(df_fx["bank"].dropna().unique())
            default_banks = [
                b for b in ["BPH", "BUH", "CAPITALBK", "BNC", "UNIBK", "SOGEBK"]
                if b in all_fx_banks
            ]
            selected_fx = st.multiselect(
                "Select banks for trend charts",
                options=all_fx_banks,
                default=default_banks,
                key="fx_bank_select",
            )

            if selected_fx:
                ts_sub = df_fx[df_fx["bank"].isin(selected_fx)].copy()
                ts_sub["pct"] = ts_sub["fx_position"]   # values are already % of equity

                left_ts, right_ts = st.columns(2)

                with left_ts:
                    st.markdown("**End-of-month FX position over time (% of equity)**")
                    fig_pos = px.line(
                        ts_sub.dropna(subset=["pct"]),
                        x="date", y="pct", color="bank",
                        labels={"pct": "% of equity", "date": "", "bank": ""},
                        template=PLOTLY_TEMPLATE,
                        height=340,
                    )
                    fig_pos.add_hline(
                        y=0.50, line_dash="dash", line_color="#e74c3c", line_width=1.5,
                        annotation_text="0.50% limit (Circ. 81-6)",
                        annotation_position="bottom right",
                    )
                    fig_pos.update_layout(margin=dict(t=8, b=8))
                    st.plotly_chart(fig_pos, use_container_width=True)

                with right_ts:
                    st.markdown("**Breach days per month over time**")
                    fig_br = px.line(
                        ts_sub,
                        x="date", y="days_exceeded", color="bank",
                        labels={"days_exceeded": "Days exceeded", "date": "", "bank": ""},
                        template=PLOTLY_TEMPLATE,
                        height=340,
                    )
                    fig_br.update_layout(margin=dict(t=8, b=8))
                    # Add a faint reference line at 0 (compliance threshold)
                    fig_br.add_hline(
                        y=0, line_color="#aaaaaa", line_dash="dot", line_width=1
                    )
                    st.plotly_chart(fig_br, use_container_width=True)

                st.divider()

                # Cumulative breach days per bank (all-time ranking)
                st.markdown("**Cumulative breach days — all time** (total days limit was exceeded since Oct 1999)")
                cum = (
                    df_fx[df_fx["bank"].isin(selected_fx)]
                    .groupby("bank")["days_exceeded"]
                    .sum()
                    .reset_index()
                    .sort_values("days_exceeded", ascending=True)
                )
                cum["color"] = cum["days_exceeded"].apply(
                    lambda d: "#e74c3c" if d > 500 else ("#e67e22" if d > 100 else "#27ae60")
                )
                fig_cum = go.Figure(go.Bar(
                    x=cum["days_exceeded"],
                    y=cum["bank"],
                    orientation="h",
                    marker_color=cum["color"],
                    text=cum["days_exceeded"].apply(lambda v: f"{v:,}"),
                    textposition="outside",
                ))
                fig_cum.update_layout(
                    height=max(220, len(cum) * 38 + 60),
                    template=PLOTLY_TEMPLATE,
                    margin=dict(t=8, b=8, l=80, r=80),
                    xaxis_title="Total days exceeded",
                )
                st.plotly_chart(fig_cum, use_container_width=True)

    # ── Tab 5: Raw Data ───────────────────────────────────────────────────────

    with tab_data:
        st.subheader("Underlying Data")
        metric_filter = st.multiselect(
            "Filter by metric",
            options=df["metric"].unique(),
            default=["npl_ratio_gross", "equity_to_assets", "roa_cumul"],
        )
        bank_filter = st.multiselect(
            "Filter by bank",
            options=df["bank"].unique(),
            default=["SYSTÈME"],
        )
        filtered = df[
            df["metric"].isin(metric_filter) & df["bank"].isin(bank_filter)
        ].dropna(subset=["value"]).copy()
        filtered["value_pct"] = filtered.apply(
            lambda r: r["value"] * 100 if r["unit"] == "ratio" else r["value"], axis=1
        )
        st.dataframe(
            filtered[["date", "bank", "metric", "label", "period_type", "unit", "value_pct"]]
            .rename(columns={"value_pct": "value"})
            .sort_values(["metric", "bank", "date"])
            .reset_index(drop=True),
            use_container_width=True,
            hide_index=True,
        )
        st.download_button(
            "Download filtered data as CSV",
            data=filtered.to_csv(index=False),
            file_name="brh_ratios_filtered.csv",
            mime="text/csv",
        )


if __name__ == "__main__":
    main()
