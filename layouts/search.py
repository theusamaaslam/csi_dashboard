"""
Global customer search modal component + data query for app-wide search.
Accessible from the sidebar on any page.
"""
import dash_bootstrap_components as dbc
from dash import html, dcc, Input, Output, State
import data_service as ds
from config import CSI_COLORS


def search_modal() -> dbc.Modal:
    """Full-screen search modal, rendered once at root in app.layout."""
    return dbc.Modal(
        [
            dbc.ModalHeader(
                html.Div([
                    html.Span("🔍", className="me-2"),
                    dbc.Input(
                        id="global-search-input",
                        placeholder="Search by User ID, e.g. USR-001234…",
                        type="text",
                        debounce=True,
                        autofocus=True,
                        className="search-modal-input",
                    ),
                ], className="d-flex align-items-center w-100"),
                close_button=True,
            ),
            dbc.ModalBody([
                dcc.Loading(
                    html.Div(id="global-search-results", className="search-results"),
                    type="dot",
                    color="#2563eb",
                ),
            ]),
        ],
        id="global-search-modal",
        is_open=False,
        size="xl",
        className="search-modal",
        keyboard=True,
    )


def register_search_callbacks(app):
    """Register the search modal open/close and results callbacks."""

    # Open modal from sidebar search button
    @app.callback(
        Output("global-search-modal", "is_open"),
        Input("btn-search-sidebar", "n_clicks"),
        State("global-search-modal", "is_open"),
        prevent_initial_call=True,
    )
    def toggle_search(n, is_open):
        return not is_open

    # Perform search when user types
    @app.callback(
        Output("global-search-results", "children"),
        Input("global-search-input", "value"),
        prevent_initial_call=True,
    )
    def do_search(query):
        if not query or len(query.strip()) < 2:
            return html.P("Type at least 2 characters to search…",
                          className="text-muted text-center py-4")

        df, total = ds.get_customer_list(userid_filter=query.strip(), page_size=100)

        if df.empty:
            return html.P(f"No customers found matching '{query}'",
                          className="text-muted text-center py-4")

        rows = []
        for _, row in df.iterrows():
            cat   = row.get("csi_category", "")
            score = row.get("predicted_csi", 0)
            color = CSI_COLORS.get(cat, "#94a3b8")
            rows.append(
                html.Div([
                    html.Div([
                        html.Span(row.get("userid", ""), className="search-uid"),
                        html.Span(cat, className="search-cat-badge",
                                  style={"background": color + "20",
                                         "color": color,
                                         "border": f"1px solid {color}40"}),
                    ], className="d-flex align-items-center gap-2"),
                    html.Div([
                        html.Span(f"Score: {score:.1f}", className="search-meta"),
                        html.Span(f"Calls: {int(row.get('total_calls', 0))}",  className="search-meta"),
                        html.Span(f"Tickets: {int(row.get('total_tickets', 0))}",  className="search-meta"),
                        html.Span(f"Outages: {int(row.get('total_outages', 0))}",  className="search-meta"),
                    ], className="d-flex gap-3 mt-1"),
                ], className="search-result-row")
            )

        header = html.P(
            f"Found {total:,} customer(s) matching '{query}'" +
            (" — showing first 100" if total > 100 else ""),
            className="search-count mb-3",
        )
        return [header, html.Div(rows, className="search-result-list")]
