"""
layouts/customers.py — Page 2: Customer list with filters.
"""
import dash_bootstrap_components as dbc
from dash import html, dcc, Input, Output, State, callback, ctx, dash_table
import pandas as pd

from config import CSI_CATEGORIES, CSI_COLORS
from layouts.components import section_header
import data_service as ds


PAGE_SIZE = 50


def layout() -> html.Div:
    return html.Div([

        # ── FILTER BAR ────────────────────────────────────────────────────────
        dbc.Card(dbc.CardBody([
            dbc.Row([
                dbc.Col([
                    html.Label("Search User ID", className="filter-label"),
                    dbc.Input(id="cust-userid-filter", placeholder="Enter User ID…",
                              className="dash-input", debounce=True),
                ], xs=12, md=3),
                dbc.Col([
                    html.Label("CSI Category", className="filter-label"),
                    dcc.Dropdown(
                        id="cust-cat-filter",
                        options=[{"label": "All", "value": ""}] +
                                [{"label": c, "value": c} for c in CSI_CATEGORIES],
                        value="",
                        clearable=False,
                        className="dash-dropdown",
                    ),
                ], xs=12, md=2),
                dbc.Col([
                    html.Label("date Range", className="filter-label"),
                    dcc.DatePickerRange(
                        id="cust-date-range",
                        display_format="DD MMM YYYY",
                        className="date-picker",
                    ),
                ], xs=12, md=4),
                dbc.Col([
                    html.Label("\u00a0", className="filter-label d-block"),
                    dbc.Button([html.I(className="bi bi-search me-1"), "Search"],
                               id="cust-search-btn", color="primary",
                               n_clicks=0, className="w-100"),
                ], xs=12, md=2),
                dbc.Col([
                    html.Label("\u00a0", className="filter-label d-block"),
                    dbc.Button([html.I(className="bi bi-stars me-1"), "AI Analysis"],
                               id="cust-ai-btn", color="secondary",
                               outline=True, n_clicks=0, className="w-100"),
                ], xs=12, md=1),
            ], align="end", className="g-2"),
        ]), className="filter-card mb-3"),

        # ── SUMMARY LINE ─────────────────────────────────────────────────────
        html.Div(id="cust-summary", className="mb-2 text-muted small"),

        # ── TABLE ─────────────────────────────────────────────────────────────
        dcc.Loading(
            dash_table.DataTable(
                id="customer-table",
                columns=[
                    {"name": "User ID",       "id": "userid",         "type": "text"},
                    {"name": "CSI Score",     "id": "predicted_csi",  "type": "numeric",
                     "format": dash_table.Format.Format(precision=1, scheme=dash_table.Format.Scheme.fixed)},
                    {"name": "Category",      "id": "csi_category",   "type": "text"},
                    {"name": "Calls",         "id": "total_calls",    "type": "numeric"},
                    {"name": "Tickets",       "id": "total_tickets",  "type": "numeric"},
                    {"name": "Outages",       "id": "total_outages",  "type": "numeric"},
                    {"name": "Activities",    "id": "total_activities","type": "numeric"},
                    {"name": "Run Date",      "id": "run_date",       "type": "text"},
                ],
                data=[],
                page_size=PAGE_SIZE,
                page_action="custom",
                page_current=0,
                sort_action="custom",
                sort_mode="single",
                filter_action="none",
                row_selectable="single",
                selected_rows=[],
                style_table={"overflowX": "auto"},
                style_header={
                    "backgroundColor": "#f4f6f9",
                    "color": "#5c7291",
                    "fontWeight": "600",
                    "fontSize": "12px",
                    "border": "1px solid #e4e8ef",
                    "textTransform": "uppercase",
                    "letterSpacing": "0.05em",
                },
                style_cell={
                    "backgroundColor": "#ffffff",
                    "color": "#1e2a3a",
                    "fontSize": "13px",
                    "padding": "10px 14px",
                    "border": "1px solid #e4e8ef",
                    "fontFamily": "Inter, sans-serif",
                },
                style_data_conditional=[
                    {
                        "if": {"filter_query": "{csi_category} = 'Very Poor'"},
                        "backgroundColor": "rgba(239,68,68,0.06)",
                        "color": "#ef4444",
                        "fontWeight": "600",
                    },
                    {
                        "if": {"filter_query": "{csi_category} = 'Low'"},
                        "backgroundColor": "rgba(249,115,22,0.06)",
                        "color": "#f97316",
                    },
                    {
                        "if": {"filter_query": "{csi_category} = 'Excellent'"},
                        "color": "#16a34a",
                        "fontWeight": "600",
                    },
                    {
                        "if": {"filter_query": "{csi_category} = 'High'"},
                        "color": "#2563eb",
                    },
                    {
                        "if": {"row_index": "odd"},
                        "backgroundColor": "#f8fafc",
                    },
                ],
                css=[{"selector": ".dash-table-container", "rule": "border-radius: 8px; overflow: hidden;"}],
            ),
            type="dot", color="#3d6dbf",
        ),

        # ── PAGINATION INFO ───────────────────────────────────────────────────
        html.Div(id="cust-page-info", className="mt-2 text-muted small text-end"),

        # Store
        dcc.Store(id="cust-total-rows", data=0),
        dcc.Store(id="cust-selected-userid", data=""),

        # AI journey offcanvas output
        html.Div(id="cust-ai-journey-output"),

    ], className="page-content")


def register_callbacks(app):

    @app.callback(
        Output("customer-table",   "data"),
        Output("cust-summary",     "children"),
        Output("cust-total-rows",  "data"),
        Output("cust-page-info",   "children"),
        Input("cust-search-btn",   "n_clicks"),
        Input("customer-table",    "page_current"),
        Input("customer-table",    "sort_by"),
        State("cust-userid-filter","value"),
        State("cust-cat-filter",   "value"),
        State("cust-date-range",   "start_date"),
        State("cust-date-range",   "end_date"),
        prevent_initial_call=False,
    )
    def update_table(n, page, sort_by, uid_filter, cat_filter, d1, d2):
        df, total = ds.get_customer_list(
            category=cat_filter or None,
            userid_filter=uid_filter or None,
            date_from=d1,
            date_to=d2,
            page=(page or 0) + 1,
            page_size=PAGE_SIZE,
        )
        if df.empty:
            return [], "No customers found.", 0, ""

        # Format run_date
        if "run_date" in df.columns:
            df["run_date"] = df["run_date"].astype(str).str[:10]

        page_start = (page or 0) * PAGE_SIZE + 1
        page_end   = min(page_start + len(df) - 1, total)
        page_info  = f"Showing {page_start:,}–{page_end:,} of {total:,} customers"

        cat_counts = df["csi_category"].value_counts().to_dict() if "csi_category" in df.columns else {}
        summary_parts = [f"Total: {total:,}"] + [
            f"{cat}: {cnt}" for cat, cnt in cat_counts.items()
        ]
        summary = " | ".join(summary_parts)

        return df.to_dict("records"), summary, total, page_info

    @app.callback(
        Output("cust-selected-userid", "data"),
        Input("customer-table", "selected_rows"),
        State("customer-table", "data"),
        prevent_initial_call=True,
    )
    def track_selected(selected, data):
        if selected and data:
            return data[selected[0]].get("userid", "")
        return ""
    # (AI callbacks are at root level in app.py)
