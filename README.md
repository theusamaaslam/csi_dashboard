# Nayatel CSI Dashboard

A premium Dash/Plotly web dashboard for Nayatel's **Customer Service Index (CSI)** system — visualises customer satisfaction scores, provides interactive drill-downs, and generates on-demand AI analysis using Groq.

---

## Prerequisites

- Python 3.10+
- Access to Nayatel's databases (`csi_db`, `ai`, `dwh`)
- Groq API key

---

## Setup

### 1. Clone / Copy the project

```
cd /path/to/csi_dashboard
```

### 2. Create the virtual environment (first time only)

```bash
python -m venv venv
```

### 3. Install dependencies (first time only)

```bash
# Windows
.\venv\Scripts\pip install -r requirements.txt

# Linux / Mac
venv/bin/pip install -r requirements.txt
```

### 4. Configure environment variables

Edit `.env` in the project root — all required fields are already present:

| Variable | Description |
|---|---|
| `CSI_DB_HOST` | Localhost PostgreSQL host (csi_scores table) |
| `CSI_DB_NAME` | Local DB name (`ai`) |
| `AI_DB_*` | Remote AI database credentials |
| `DWH_DB_*` | Remote DWH database credentials |
| `GROQ_API_KEY` | Your Groq API key |
| `GROQ_MODEL` | Model name e.g. `llama3-70b-8192` |
| `DASHBOARD_PORT` | Port to serve on (default `8050`) |

---

## Running the Dashboard

### Development

```bash
# Windows
.\venv\Scripts\python app.py

# Linux / Mac
venv/bin/python app.py
```

Then open **http://localhost:8050** (or the server IP on the configured port).

### Production (multi-worker)

```bash
.\venv\Scripts\gunicorn app:server -b 0.0.0.0:8050 -w 4
# or on Linux:
venv/bin/gunicorn app:server -b 0.0.0.0:8050 -w 4
```

---

## Data Pipeline

The dashboard **reads only** — it never writes to any database.  
CSI scores must be pre-calculated by running:

```bash
.\venv\Scripts\python run_csi.py
```

This should be scheduled daily (e.g. via Task Scheduler / cron) to keep scores fresh.

---

## Pages

| Page | URL | Description |
|---|---|---|
| Overview | `/` | KPI cards, CSI donut, trend chart, 7 drill-down tabs |
| Customers | `/customers` | Searchable/filterable customer DataTable |

**AI Analysis** is available on both pages via the **AI Analysis** button (sidebar or FAB).

---

## Project Structure

```
csi_dashboard/
├── app.py               # Dash entry point
├── config.py            # Environment variable loader
├── db.py                # SQLAlchemy DB engines
├── data_service.py      # All data query functions
├── ai_service.py        # Groq AI integration
├── csi_utils.py         # CSI scoring engine (unchanged)
├── run_csi.py           # Daily score calculation pipeline
├── csi_api.py           # FastAPI endpoint for per-user CSI
├── export_csi_tables.py # Exports ai-DB tables to CSV
├── requirements.txt
├── .env                 # Credentials (do not commit)
├── assets/
│   ├── style.css        # Dashboard CSS
│   └── nayatel-logo.webp
└── layouts/
    ├── components.py    # Sidebar, KPI card, AI panel
    ├── overview.py      # Page 1 — Overview
    └── customers.py     # Page 2 — Customers
```
