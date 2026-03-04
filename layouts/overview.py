"""
layouts/overview.py — Page 1: Overview dashboard.
KPI cards + CSI distribution + 7 drill-down charts.
"""
import plotly.graph_objects as go
import plotly.express as px
import dash_bootstrap_components as dbc
from dash import html, dcc, Input, Output, State, callback, ctx, no_update, ALL
import pandas as pd

from config import CSI_COLORS, CSI_CATEGORIES
from layouts.components import (
    kpi_card, section_header, date_filter_bar, drill_breadcrumb
)
import data_service as ds


# ── Plotly dark config shared across all charts ───────────────────────────────
CHART_LAYOUT = dict(
    paper_bgcolor="rgba(255,255,255,0)",
    plot_bgcolor="rgba(255,255,255,0)",
    font=dict(color="#374151", family="Inter, sans-serif", size=12),
    margin=dict(l=10, r=10, t=30, b=10),
    legend=dict(bgcolor="rgba(255,255,255,0)", font=dict(color="#374151")),
    xaxis=dict(gridcolor="#f3f4f6", linecolor="#e5e7eb"),
    yaxis=dict(gridcolor="#f3f4f6", linecolor="#e5e7eb"),
)

DONUT_COLORS = [CSI_COLORS[c] for c in CSI_CATEGORIES if c in CSI_COLORS]


# ── Layout builder ────────────────────────────────────────────────────────────

def layout() -> html.Div:
    return html.Div([
        # Stores for drill-down state
        dcc.Store(id="store-selected-category", data="Very Poor"),
        dcc.Store(id="store-city-drill",   data=[]),   # [city, area]
        dcc.Store(id="store-bng-drill",    data=[]),   # [bng, olt]
        dcc.Store(id="store-service-drill",data=[]),   # [service, master_fault]

        # Top date/filter bar
        date_filter_bar(),

        html.Br(),

        # ── KPI CARDS ────────────────────────────────────────────────────────
        html.Div(id="kpi-row"),

        html.Br(),

        # ── DISTRIBUTION ROW: donut + trend ──────────────────────────────────
        section_header("CSI Distribution", "Click a segment to drill into that category across all charts"),
        dbc.Row([
            dbc.Col(
                dbc.Card(dbc.CardBody([
                    html.H6("Overall CSI Distribution", className="chart-title"),
                    dcc.Loading(dcc.Graph(id="donut-chart", config={"displayModeBar": False},
                                         style={"height": "320px"})),
                ])),
                xs=12, md=6,
            ),
            dbc.Col(
                dbc.Card(dbc.CardBody([
                    html.H6("CSI Trend Over Time", className="chart-title"),
                    dcc.Loading(dcc.Graph(id="trend-chart", config={"displayModeBar": False},
                                         style={"height": "320px"})),
                ])),
                xs=12, md=6,
            ),
        ], className="mb-3"),

        # ── 7 DRILL-DOWN CHARTS ───────────────────────────────────────────────
        section_header("Drill-Down Analysis",
                        "All charts reflect the selected CSI category. Click segments to drill deeper."),

        dbc.Tabs([
            dbc.Tab(label="📅 Occurrence", tab_id="tab-occurrence"),
            dbc.Tab(label="🌐 Services",   tab_id="tab-services"),
            dbc.Tab(label="🏙️ City",      tab_id="tab-city"),
            dbc.Tab(label="📡 OSP / BNG", tab_id="tab-bng"),
            dbc.Tab(label="📦 Packages",  tab_id="tab-packages"),
            dbc.Tab(label="📟 Hardware",  tab_id="tab-hardware"),
            dbc.Tab(label="📅 Install Year", tab_id="tab-install"),
        ], id="drilldown-tabs", active_tab="tab-occurrence", className="dash-tabs mb-2"),

        dcc.Loading(html.Div(id="drilldown-panel"), type="dot", color="#3b82f6"),

        html.Br(),

    ], className="page-content")


# ── CALLBACKS ─────────────────────────────────────────────────────────────────

def register_callbacks(app):

    # KPI Cards
    @app.callback(
        Output("kpi-row", "children"),
        Input("global-date-range", "start_date"),
        Input("global-date-range", "end_date"),
    )
    def update_kpis(d1, d2):
        summary = ds.get_csi_summary(d1, d2)
        total   = summary["total"]
        avg     = summary["avg_score"]

        cards = [
            kpi_card("Total Customers", f"{total:,}", f"Avg Score: {avg}", "#2563eb", "bi-people-fill", "kpi-total"),
        ]
        icon_map = {
            "Excellent": ("bi-star-fill",       "#22c55e"),
            "High":      ("bi-arrow-up-circle-fill", "#3b82f6"),
            "Medium":    ("bi-dash-circle-fill", "#f59e0b"),
            "Low":       ("bi-exclamation-circle-fill", "#f97316"),
            "Very Poor": ("bi-x-circle-fill",   "#ef4444"),
        }
        for cat in CSI_CATEGORIES:
            info = summary["by_category"].get(cat, {"count": 0, "pct": 0})
            icon, color = icon_map.get(cat, ("bi-circle", "#94a3b8"))
            cards.append(kpi_card(
                cat, f"{info['count']:,}",
                f"{info['pct']}% of base",
                color, icon,
            ))
        return dbc.Row(cards, className="kpi-row g-2")

    # Donut chart
    @app.callback(
        Output("donut-chart", "figure"),
        Input("global-date-range", "start_date"),
        Input("global-date-range", "end_date"),
    )
    def update_donut(d1, d2):
        summary = ds.get_csi_summary(d1, d2)
        cats    = list(summary["by_category"].keys())
        counts  = [summary["by_category"][c]["count"] for c in cats]
        colors  = [CSI_COLORS.get(c, "#94a3b8") for c in cats]

        fig = go.Figure(go.Pie(
            labels=cats, values=counts,
            hole=0.55,
            marker=dict(colors=colors, line=dict(color="#0f1117", width=2)),
            textinfo="label+percent",
            hovertemplate="<b>%{label}</b><br>%{value:,} customers (%{percent})<extra></extra>",
            textfont=dict(color="#e2e8f0", size=11),
        ))
        total = summary["total"]
        fig.add_annotation(text=f"<b>{total:,}</b><br>Total",
                           font=dict(size=14, color="#374151"), showarrow=False)
        fig.update_layout(**CHART_LAYOUT)
        return fig

    # Trend chart
    @app.callback(
        Output("trend-chart", "figure"),
        Input("global-date-range", "start_date"),
        Input("global-date-range", "end_date"),
        Input("granularity", "value"),
    )
    def update_trend(d1, d2, gran):
        df = ds.get_csi_trend(d1, d2, gran)
        if df.empty:
            return go.Figure().update_layout(**CHART_LAYOUT)

        fig = go.Figure()
        for cat in CSI_CATEGORIES:
            sub = df[df["csi_category"] == cat]
            if sub.empty:
                continue
            fig.add_trace(go.Scatter(
                x=sub["period"], y=sub["cnt"],
                mode="lines+markers",
                name=cat,
                line=dict(color=CSI_COLORS.get(cat, "#94a3b8"), width=2),
                marker=dict(size=5),
                hovertemplate=f"<b>{cat}</b><br>%{{x}}<br>%{{y:,}} customers<extra></extra>",
            ))
        fig.update_layout(
            xaxis_title=None, yaxis_title="Customers",
            hovermode="x unified",
            **CHART_LAYOUT,
        )
        return fig

    # Selected category from donut or category-filter dropdown
    @app.callback(
        Output("store-selected-category", "data"),
        Input("donut-chart", "clickData"),
        Input("category-filter", "value"),
        prevent_initial_call=True,
    )
    def update_selected_category(click, dropdown_val):
        triggered = ctx.triggered_id
        if triggered == "donut-chart" and click:
            return click["points"][0]["label"]
        return dropdown_val

    # Sync dropdown with store
    @app.callback(
        Output("category-filter", "value"),
        Input("store-selected-category", "data"),
        prevent_initial_call=True,
    )
    def sync_dropdown(cat):
        return cat

    # Drill-down interaction state machine
    @app.callback(
        Output("store-city-drill",    "data"),
        Output("store-bng-drill",     "data"),
        Output("store-service-drill", "data"),
        Input({"type": "drill-chart", "index": ALL}, "clickData"),
        Input("drilldown-tabs",        "active_tab"),
        Input("store-selected-category", "data"),
        Input("global-date-range", "start_date"),
        Input("global-date-range", "end_date"),
        State("store-city-drill",      "data"),
        State("store-bng-drill",       "data"),
        State("store-service-drill",   "data"),
        prevent_initial_call=True,
    )
    def update_drill_stores(chart_clicks, tab, category, d1, d2, city_drill, bng_drill, svc_drill):
        triggered = ctx.triggered_id or ""

        # Reset states explicitly back to top level of chart hierarchy
        if isinstance(triggered, str) and triggered in (
            "drilldown-tabs", "store-selected-category",
            "global-date-range.start_date", "global-date-range.end_date"
        ):
            return [], [], []

        # Handle drill-down clicks inside dynamic charts
        if isinstance(triggered, dict) and triggered.get("type") == "drill-chart":
            triggered_idx = triggered.get("index")
            click_data = None
            if chart_clicks and any(chart_clicks):
                click_data = next((c for c in chart_clicks if c is not None), None)

            if click_data and "points" in click_data:
                label = click_data["points"][0]["label"]
                if triggered_idx == "city":
                    if len(city_drill) == 0: return [label], bng_drill, svc_drill
                    elif len(city_drill) == 1: return [city_drill[0], label], bng_drill, svc_drill
                elif triggered_idx == "bng":
                    if len(bng_drill) == 0: return city_drill, [label], svc_drill
                    elif len(bng_drill) == 1: return city_drill, [bng_drill[0], label], svc_drill
                elif triggered_idx == "services":
                    if len(svc_drill) == 0: return city_drill, bng_drill, [label]
                    elif len(svc_drill) == 1: return city_drill, bng_drill, [svc_drill[0], label]

        # Prevent circular logic loops by only returning if exactly required
        return no_update, no_update, no_update

    # Drill-down UI renderer logic
    @app.callback(
        Output("drilldown-panel", "children"),
        Input("drilldown-tabs",        "active_tab"),
        Input("store-selected-category", "data"),
        Input("global-date-range", "start_date"),
        Input("global-date-range", "end_date"),
        Input("store-city-drill",      "data"),
        Input("store-bng-drill",       "data"),
        Input("store-service-drill",   "data"),
        prevent_initial_call=False,
    )
    def render_drilldown_panel(tab, category, d1, d2, city_drill, bng_drill, svc_drill):
        # Render panel conditionally based on updated drilldown store contexts
        return _render_drilldown(tab, category, d1, d2, city_drill, bng_drill, svc_drill)

    # (AI toggle & generate are handled at root level in app.py)


# ── Private render helper ─────────────────────────────────────────────────────

def _render_drilldown(tab, category, d1, d2, city_drill, bng_drill, svc_drill):

    def _bar(df, x_col, y_col, title, color="#7c3aed"):
        if df.empty:
            fig = go.Figure()
        else:
            fig = px.bar(df, x=x_col, y=y_col, text=y_col,
                         color_discrete_sequence=[color])
            fig.update_traces(texttemplate="%{text:,}", textposition="outside")
        fig.update_layout(title=title, xaxis_tickangle=-30, **CHART_LAYOUT)
        return dcc.Graph(figure=fig, config={"displayModeBar": False}, style={"height": "380px"})

    def _donut(df, label_col, value_col, title, colors=None):
        if df.empty:
            fig = go.Figure()
        else:
            fig = go.Figure(go.Pie(
                labels=df[label_col], values=df[value_col],
                hole=0.5,
                marker=dict(colors=colors or px.colors.qualitative.Set3,
                            line=dict(color="#ffffff", width=2)),
                textinfo="label+percent",
                hovertemplate="<b>%{label}</b><br>%{value:,}<extra></extra>",
                textfont=dict(color="#374151", size=10),
            ))
        fig.update_layout(title=title, **CHART_LAYOUT)
        return dcc.Graph(figure=fig, config={"displayModeBar": False}, style={"height": "380px"})

    # ── Tab: Occurrence ───────────────────────────────────────────────────────
    if tab == "tab-occurrence":
        df = ds.get_occurrence_by_period(d1, d2, category)
        if not df.empty:
            df["period"] = df["period"].astype(str).str[:10]
            df["pct"] = (df["selected_count"] / df["total_count"].replace(0, 1) * 100).round(1)
        fig = go.Figure()
        if not df.empty:
            fig.add_trace(go.Bar(
                x=df["period"], y=df["selected_count"],
                name=category,
                marker_color=CSI_COLORS.get(category, "#3b82f6"),
                text=[f"{c:,} ({p}%)" for c, p in zip(df["selected_count"], df["pct"])],
                textposition="outside",
                hovertemplate="<b>%{x}</b><br>%{y:,} customers<extra></extra>",
            ))
        fig.update_layout(
            title=f"{category} Customer Occurrence — highest first",
            xaxis_title=None, yaxis_title="Customers",
            **CHART_LAYOUT,
        )
        return dbc.Card(dbc.CardBody([
            dcc.Graph(id={"type": "drill-chart", "index": "occurrence"}, figure=fig,
                      config={"displayModeBar": False}, style={"height": "400px"}),
        ]))

    # ── Tab: Services ─────────────────────────────────────────────────────────
    elif tab == "tab-services":
        breadcrumb = drill_breadcrumb(["Services"] + svc_drill)
        if len(svc_drill) == 0:
            df = ds.get_service_breakdown(category, d1, d2)
            chart = _donut(df, "service", "cnt", f"{category} by Service",
                           colors=["#3b82f6", "#22c55e", "#f59e0b", "#a855f7"])
            chart_id = "services-chart"
        elif len(svc_drill) == 1:
            df = ds.get_fault_types(category, d1, d2, service_filter=svc_drill[0])
            chart = _donut(df, "fault_type", "cnt",
                           f"{category} – {svc_drill[0]} Master Fault Types")
            chart_id = "services-chart"
        else:
            df = ds.get_sub_fault_types(category, svc_drill[1], d1, d2)
            chart = _donut(df, "sub_fault_type", "cnt",
                           f"{category} – {svc_drill[-1]} Sub Fault Types")
            chart_id = "services-chart"

        # Patch the graph id
        if hasattr(chart, "id"):
            chart.id = {"type": "drill-chart", "index": "services"}
        return dbc.Card(dbc.CardBody([breadcrumb, html.Br(), chart]))

    # ── Tab: City ─────────────────────────────────────────────────────────────
    elif tab == "tab-city":
        breadcrumb = drill_breadcrumb(["All Cities"] + city_drill)
        city   = city_drill[0] if city_drill else None
        area   = city_drill[1] if len(city_drill) > 1 else None
        df = ds.get_city_breakdown(category, city, area, d1, d2)
        title = (f"{category} in {area}" if area
                 else f"{category} in {city}" if city
                 else f"{category} by City")
        chart = _donut(df, "label", "selected_cnt", title)
        chart.id = {"type": "drill-chart", "index": "city"}
        return dbc.Card(dbc.CardBody([breadcrumb, html.Br(), chart]))

    # ── Tab: BNG / OSP ────────────────────────────────────────────────────────
    elif tab == "tab-bng":
        breadcrumb = drill_breadcrumb(["All BNGs"] + bng_drill)
        df = ds.get_bng_breakdown(category, d1, d2)
        chart = _donut(df, "bng", "selected_cnt", f"{category} by BNG")
        chart.id = {"type": "drill-chart", "index": "bng"}
        return dbc.Card(dbc.CardBody([breadcrumb, html.Br(), chart]))

    # ── Tab: Packages ─────────────────────────────────────────────────────────
    elif tab == "tab-packages":
        df = ds.get_package_breakdown(category, d1, d2)
        return dbc.Card(dbc.CardBody([
            _bar(df, "package", "cnt", f"{category} by Package",
                 CSI_COLORS.get(category, "#3b82f6"))
        ]))

    # ── Tab: Hardware ─────────────────────────────────────────────────────────
    elif tab == "tab-hardware":
        df = ds.get_hardware_breakdown(category, d1, d2)
        return dbc.Card(dbc.CardBody([
            _bar(df, "hardware", "cnt", f"{category} by ONT Hardware",
                 "#a855f7")
        ]))

    # ── Tab: Install Year ─────────────────────────────────────────────────────
    elif tab == "tab-install":
        df = ds.get_install_year_breakdown(category, d1, d2)
        return dbc.Card(dbc.CardBody([
            _bar(df, "install_year", "cnt", f"{category} by Installation Year",
                 "#06b6d4")
        ]))

    return html.Div()
