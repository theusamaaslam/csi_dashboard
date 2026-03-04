"""
layouts/components.py  — Reusable UI components for the CSI Dashboard.
"""
import dash_bootstrap_components as dbc
from dash import html, dcc
from config import CSI_COLORS, CSI_CATEGORIES


# ─────────────────────────────────────────────────────────────────────────────
# COLOUR HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def cat_color(category: str) -> str:
    return CSI_COLORS.get(category, "#94a3b8")


# ─────────────────────────────────────────────────────────────────────────────
# KPI CARD
# ─────────────────────────────────────────────────────────────────────────────

def kpi_card(title: str, value: str, sub: str = "", color: str = "#3b82f6",
             icon: str = "bi-graph-up", card_id: str = "") -> dbc.Col:
    return dbc.Col(
        dbc.Card(
            dbc.CardBody([
                html.Div([
                    html.Div(
                        html.I(className=f"bi {icon} kpi-icon-svg"),
                        className="kpi-icon",
                        style={"color": color},
                    ),
                    html.Div([
                        html.P(title, className="kpi-title"),
                        html.H3(value, className="kpi-value", style={"color": color}),
                        html.P(sub, className="kpi-sub") if sub else html.Span(),
                    ], className="kpi-text"),
                ], className="kpi-inner"),
            ]),
            className="kpi-card h-100",
            id=card_id if card_id else f"kpi-{title.lower().replace(' ', '-')}",
            style={"border-left": f"4px solid {color}"},
        ),
        xs=12, sm=6, md=4, lg=4,
        className="d-flex",
    )


# ─────────────────────────────────────────────────────────────────────────────
# SECTION HEADER
# ─────────────────────────────────────────────────────────────────────────────

def section_header(title: str, subtitle: str = "") -> html.Div:
    return html.Div([
        html.H5(title, className="section-title"),
        html.P(subtitle, className="section-sub") if subtitle else html.Span(),
        html.Hr(className="section-hr"),
    ], className="section-header")


# ─────────────────────────────────────────────────────────────────────────────
# DATE RANGE PICKER
# ─────────────────────────────────────────────────────────────────────────────

def date_filter_bar() -> dbc.Card:
    from datetime import datetime, timedelta
    end   = datetime.now().strftime("%Y-%m-%d")
    start = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")

    return dbc.Card(
        dbc.CardBody([
            dbc.Row([
                dbc.Col([
                    html.Label("Date Range", className="filter-label"),
                    dcc.DatePickerRange(
                        id="global-date-range",
                        start_date=start,
                        end_date=end,
                        display_format="DD MMM YYYY",
                        className="date-picker",
                    ),
                ], xs=12, md=5),
                dbc.Col([
                    html.Label("Granularity", className="filter-label"),
                    dcc.RadioItems(
                        id="granularity",
                        options=[
                            {"label": "Day",   "value": "day"},
                            {"label": "Week",  "value": "week"},
                            {"label": "Month", "value": "month"},
                        ],
                        value="month",
                        inline=True,
                        className="granularity-radio",
                    ),
                ], xs=12, md=4),
                dbc.Col([
                    html.Label("Category Filter", className="filter-label"),
                    dcc.Dropdown(
                        id="category-filter",
                        options=[{"label": c, "value": c} for c in CSI_CATEGORIES],
                        value="Very Poor",
                        clearable=False,
                        className="dash-dropdown",
                    ),
                ], xs=12, md=3),
            ], align="center"),
        ]),
        className="filter-card",
    )


# ─────────────────────────────────────────────────────────────────────────────
# BREADCRUMB (drill-down path)
# ─────────────────────────────────────────────────────────────────────────────

def drill_breadcrumb(levels: list[str]) -> html.Div:
    items = []
    for i, lvl in enumerate(levels):
        items.append(html.Span(lvl, className="breadcrumb-item"))
        if i < len(levels) - 1:
            items.append(html.Span(" → ", className="breadcrumb-sep"))
    return html.Div(items, className="drill-breadcrumb") if items else html.Span()


# ─────────────────────────────────────────────────────────────────────────────
# AI ANALYSIS PANEL
# ─────────────────────────────────────────────────────────────────────────────

def ai_panel_offcanvas() -> dbc.Offcanvas:
    return dbc.Offcanvas(
        [
            html.Div([
                html.Div([
                    html.I(className="bi bi-stars", style={"font-size": "1.5rem", "color": "#3d6dbf"}),
                    html.H5("AI Analysis", className="ms-2 mb-0"),
                ], className="d-flex align-items-center mb-3"),
                html.P(
                    "Get an AI-powered insight on the current view. The analysis uses your "
                    "selected filters and live data from the database.",
                    className="text-muted small",
                ),
                dbc.Button(
                    [html.I(className="bi bi-stars me-2"), "Generate Analysis"],
                    id="btn-generate-ai",
                    color="primary",
                    className="w-100 mb-3",
                    n_clicks=0,
                ),
                dcc.Loading(
                    id="ai-loading",
                    type="dot",
                    color="#3b82f6",
                    children=html.Div(id="ai-output", className="ai-output"),
                ),
            ]),
        ],
        id="ai-offcanvas",
        title="",
        is_open=False,
        placement="end",
        style={"width": "480px"},
    )


# ─────────────────────────────────────────────────────────────────────────────
# NAVBAR / SIDEBAR
# ─────────────────────────────────────────────────────────────────────────────

def sidebar() -> html.Div:
    return html.Div([
        html.Div([
            html.Img(
                src="/assets/nayatel-logo.webp",
                height="38px",
                className="me-2",
                style={"object-fit": "contain"},
            ),
        ], className="sidebar-logo"),

        html.Hr(className="sidebar-hr"),

        dbc.Nav(
            [
                dbc.NavLink(
                    [html.I(className="bi bi-speedometer2 sidebar-icon"), " Overview"],
                    href="/",
                    active="exact",
                    className="sidebar-link",
                ),
                dbc.NavLink(
                    [html.I(className="bi bi-people sidebar-icon"), " Customers"],
                    href="/customers",
                    active="exact",
                    className="sidebar-link",
                ),
            ],
            vertical=True,
            pills=True,
            className="sidebar-nav",
        ),

        html.Div([
            html.P("Nayatel CSI Platform", className="sidebar-footer-text"),
            html.P("v2.0 — 2026", className="sidebar-footer-sub"),
        ], className="sidebar-footer"),
    ], className="sidebar", id="sidebar")
