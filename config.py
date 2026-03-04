"""
config.py — Central configuration loader.
Reads all values from .env file.
"""
import os
from dotenv import load_dotenv

load_dotenv()

# ── Local CSI DB (read/write) ─────────────────────────────────────────────────
LOCAL_DB_HOST     = os.getenv("CSI_DB_HOST", "localhost")
LOCAL_DB_PORT     = int(os.getenv("CSI_DB_PORT", 5432))
LOCAL_DB_NAME     = os.getenv("CSI_DB_NAME", "csi_db")
LOCAL_DB_USER     = os.getenv("CSI_DB_USER", "csi_admin")
LOCAL_DB_PASSWORD = os.getenv("CSI_DB_PASSWORD", "")
LOCAL_DB_TABLE    = os.getenv("CSI_DB_TABLE", "csi_scores")

LOCAL_DB_URL = (
    f"postgresql+psycopg2://{LOCAL_DB_USER}:{LOCAL_DB_PASSWORD}"
    f"@{LOCAL_DB_HOST}:{LOCAL_DB_PORT}/{LOCAL_DB_NAME}"
)

# ── Remote AI DB (read-only) ──────────────────────────────────────────────────
AI_DB_HOST     = os.getenv("AI_DB_HOST", "dwhprimary.nayatel.com")
AI_DB_PORT     = int(os.getenv("AI_DB_PORT", 5432))
AI_DB_NAME     = os.getenv("AI_DB_NAME", "ai")
AI_DB_USER     = os.getenv("AI_DB_USER", "tacusama")
AI_DB_PASSWORD = os.getenv("AI_DB_PASSWORD", "")

AI_DB_URL = (
    f"postgresql+psycopg2://{AI_DB_USER}:{AI_DB_PASSWORD}"
    f"@{AI_DB_HOST}:{AI_DB_PORT}/{AI_DB_NAME}"
)

# ── Remote DWH DB (read-only) ─────────────────────────────────────────────────
DWH_DB_HOST     = os.getenv("DWH_DB_HOST", "dwhprimary.nayatel.com")
DWH_DB_PORT     = int(os.getenv("DWH_DB_PORT", 5432))
DWH_DB_NAME     = os.getenv("DWH_DB_NAME", "dwh")
DWH_DB_USER     = os.getenv("DWH_DB_USER", "tacusama")
DWH_DB_PASSWORD = os.getenv("DWH_DB_PASSWORD", "")

DWH_DB_URL = (
    f"postgresql+psycopg2://{DWH_DB_USER}:{DWH_DB_PASSWORD}"
    f"@{DWH_DB_HOST}:{DWH_DB_PORT}/{DWH_DB_NAME}"
)

# ── Groq (LLM) ────────────────────────────────────────────────────────────────
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL = os.getenv("GROQ_MODEL", "openai/gpt-oss-120b")

# ── Dashboard ─────────────────────────────────────────────────────────────────
DASHBOARD_HOST  = os.getenv("DASHBOARD_HOST", "0.0.0.0")
DASHBOARD_PORT  = int(os.getenv("DASHBOARD_PORT", 8050))
DASHBOARD_DEBUG = os.getenv("DASHBOARD_DEBUG", "false").lower() == "true"

# ── CSI Category config ───────────────────────────────────────────────────────
CSI_CATEGORIES  = ["Excellent", "High", "Medium", "Low", "Very Poor"]
CSI_COLORS = {
    "Excellent": "#22c55e",
    "High":      "#3b82f6",
    "Medium":    "#f59e0b",
    "Low":       "#f97316",
    "Very Poor": "#ef4444",
}

# BNG → Router mapping from requirements doc
BNG_ROUTERS = {
    "Islamabad BNG":         ["F11-MX480-R1", "F11-MX480-R2", "FPOP-MX-480", "I9-BNG"],
    "Faisalabad BNG":        ["FSD MX480 R1", "FSD MX480 R2"],
    "Gujranwala/Sialkot BNG":["GRW NE40 R2", "SKT NE 40"],
    "Peshawar BNG":          ["Huawei BNG Psh (172.16.86.45)", "Huawei-BNG-Psh (172.16.86.46)"],
    "Lahore BNG":            ["LHR Router NE-40", "LHR_MX104"],
    "Rawalpindi BNG":        ["MT Router1 (172.16.86.40)", "MT Router1 (172.16.86.41)", "SADDAR BRAS", "SDR-MX-480"],
    "Multan/Muzaffargarh":   ["MTN MX480", "MTN-MX-480-R2"],
    "Sargodha BNG":          ["SGD BRAS-1", "SGD BRAS-2"],
}
