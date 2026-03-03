"""
app.py — Dash application entry point.
AI offcanvas lives here (root level) to avoid duplicate callback outputs.
"""
import dash
import dash_bootstrap_components as dbc
from dash import html, dcc, Input, Output, State, ctx

from config import DASHBOARD_HOST, DASHBOARD_PORT, DASHBOARD_DEBUG
import layouts.overview  as overview_page
import layouts.customers as customers_page
from layouts.components import sidebar, ai_panel_offcanvas

# ── App init ──────────────────────────────────────────────────────────────────
app = dash.Dash(
    __name__,
    external_stylesheets=[
        dbc.themes.BOOTSTRAP,
        "https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap",
        "https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.min.css",
    ],
    suppress_callback_exceptions=True,
    meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}],
    title="Nayatel CSI Dashboard",
)
server = app.server

# ── Root layout ───────────────────────────────────────────────────────────────
# AI offcanvas at root so it's available on every page with a single callback
app.layout = html.Div([
    dcc.Location(id="url", refresh=False),
    sidebar(),
    ai_panel_offcanvas(),
    html.Div(id="page-content", className="main-content"),
], className="app-wrapper")


# ── Page routing ──────────────────────────────────────────────────────────────
@app.callback(Output("page-content", "children"), Input("url", "pathname"))
def route(pathname):
    if pathname == "/customers":
        return customers_page.layout()
    return overview_page.layout()


# ── AI offcanvas toggle (single callback at root level) ───────────────────────
# NOTE: btn-open-ai (overview FAB) and nav-ai-btn (sidebar) always exist in root.
# cust-ai-btn only exists when on /customers page — handled via dcc.Store signal.
@app.callback(
    Output("ai-offcanvas", "is_open"),
    Input("btn-open-ai",   "n_clicks"),
    Input("nav-ai-btn",    "n_clicks"),
    State("ai-offcanvas",  "is_open"),
    prevent_initial_call=True,
)
def toggle_ai(n1, n2, is_open):
    return not is_open


# ── AI analysis generation (single callback at root level) ────────────────────
@app.callback(
    Output("ai-output", "children"),
    Input("btn-generate-ai", "n_clicks"),
    State("store-selected-category", "data"),
    State("global-date-range", "start_date"),
    State("global-date-range", "end_date"),
    State("cust-selected-userid", "data"),
    State("url", "pathname"),
    prevent_initial_call=True,
)
def generate_ai(n, category, d1, d2, userid, pathname):
    if not n:
        return ""
    from ai_service import analyze_segment, analyze_customer
    import data_service as ds

    if pathname == "/customers" and userid:
        journey = ds.get_customer_journey(userid)
        text = analyze_customer(journey)
    else:
        summary = ds.get_csi_summary(d1, d2)
        filters = {"date_from": d1, "date_to": d2}
        text = analyze_segment(category or "Very Poor", summary, filters)

    from dash import dcc
    return dcc.Markdown(text, className="ai-markdown")


# ── Register page-specific callbacks (no AI-related outputs) ───────────────────
overview_page.register_callbacks(app)
customers_page.register_callbacks(app)

# ── Run ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    app.run(
        host=DASHBOARD_HOST,
        port=DASHBOARD_PORT,
        debug=DASHBOARD_DEBUG,
    )
