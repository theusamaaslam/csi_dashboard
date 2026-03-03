"""
ai_service.py — Groq LLM integration for on-demand CSI analysis.
"""
from groq import Groq
from config import GROQ_API_KEY, GROQ_MODEL

_client = Groq(api_key=GROQ_API_KEY)

FALLBACK_MODEL = "llama3-70b-8192"


def _call_groq(prompt: str, model: str = GROQ_MODEL) -> str:
    """Call Groq with a prompt and return the response text."""
    try:
        resp = _client.chat.completions.create(
            model=model,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a customer experience analyst for Nayatel, a telecom company in Pakistan. "
                        "You analyse Customer Service Index (CSI) data and provide clear, concise, actionable insights. "
                        "Format your response using markdown with clear headings. Keep responses focused and professional."
                    ),
                },
                {"role": "user", "content": prompt},
            ],
            max_tokens=1024,
            temperature=0.4,
        )
        return resp.choices[0].message.content or "No response generated."
    except Exception as e:
        if model != FALLBACK_MODEL:
            return _call_groq(prompt, FALLBACK_MODEL)
        return f"⚠️ AI analysis unavailable: {e}"


def analyze_segment(
    category: str,
    summary: dict,
    filters: dict | None = None,
) -> str:
    """
    Generate an insight for a selected CSI category segment.

    Args:
        category: e.g. 'Very Poor'
        summary: dict from data_service.get_csi_summary()
        filters: {'date_from': ..., 'date_to': ..., 'city': ..., 'service': ...}
    """
    filters = filters or {}
    by_cat = summary.get("by_category", {})
    total  = summary.get("total", 0)

    cat_info = by_cat.get(category, {})
    count    = cat_info.get("count", 0)
    pct      = cat_info.get("pct",   0)

    # Build per-category breakdown string
    breakdown = "\n".join(
        f"  - {c}: {info.get('count', 0):,} customers ({info.get('pct', 0)}%)"
        for c, info in by_cat.items()
    )

    date_range = (
        f"from {filters.get('date_from', 'N/A')} to {filters.get('date_to', 'N/A')}"
    )
    city_note    = f"City filter: {filters['city']}"     if filters.get("city")    else ""
    service_note = f"Service filter: {filters['service']}" if filters.get("service") else ""

    prompt = f"""
## CSI Dashboard Segment Analysis Request

**Selected Category:** {category}
**Date Range:** {date_range}
{city_note}
{service_note}

### Overall CSI Distribution ({total:,} total customers):
{breakdown}

### Focus: {category} Customers
- Count: {count:,}
- Percentage: {pct}% of total base

---

Please provide:
1. **Root Cause Analysis** – What typically drives customers to the '{category}' category?
2. **Key Risk Indicators** – What patterns should TAC Level-1 agents watch for?
3. **Immediate Actions** – 3–5 concrete steps to improve this segment's experience.
4. **Trends** – Any notable observations from the data above.
"""
    return _call_groq(prompt)


def analyze_customer(journey: dict) -> str:
    """
    Generate a per-customer journey summary for TAC agents.

    Args:
        journey: dict from data_service.get_customer_journey(userid)
    """
    uid  = journey.get("userid", "Unknown")
    csi  = journey.get("csi", {})
    score    = csi.get("predicted_csi", "N/A")
    category = csi.get("csi_category", "N/A")

    def _fmt_list(records: list, fields: list[str]) -> str:
        if not records:
            return "  None on record."
        lines = []
        for r in records[:10]:
            parts = [f"{f}: {r.get(f, '')}" for f in fields if r.get(f)]
            lines.append("  - " + " | ".join(parts))
        return "\n".join(lines)

    tickets_str  = _fmt_list(journey.get("tickets", []),
                              ["ticket_type", "fault_types", "sub_fault_types", "creation_time"])
    calls_str    = _fmt_list(journey.get("calls", []),
                              ["master_fault_type", "sub_fault_type", "entry_time"])
    outages_str  = _fmt_list(journey.get("outages", []),
                              ["event_type", "duration", "occurrence_time"])
    activities_str = _fmt_list(journey.get("activities", []),
                                ["activity_name", "services", "status", "occurrence_time"])

    prompt = f"""
## Customer Journey Analysis for TAC Level-1

**Customer ID:** {uid}
**CSI Score:** {score}  |  **Category:** {category}

### Recent Trouble Tickets (last 20):
{tickets_str}

### Recent CTI Calls (last 20):
{calls_str}

### Recent Outages (last 10):
{outages_str}

### Recent Activities (last 10):
{activities_str}

---

Please provide a TAC Level-1 agent with:
1. **Customer Experience Summary** – A brief narrative of this customer's service history.
2. **Recurring Issues** – Identify any repeated fault types or patterns.
3. **Root Cause Hypothesis** – What is most likely causing the poor experience?
4. **Recommended Action** – What should the agent do/say right now to help this customer?
5. **Escalation Needed?** – Yes or No, and why.
"""
    return _call_groq(prompt)


def analyze_fleet_trend(summary: dict, trend_data: list[dict] | None = None) -> str:
    """Generate a fleet-wide insight for the management view."""
    by_cat = summary.get("by_category", {})
    total  = summary.get("total", 0)
    avg    = summary.get("avg_score", 0)

    breakdown = "\n".join(
        f"  - {c}: {info.get('count', 0):,} ({info.get('pct', 0)}%)"
        for c, info in by_cat.items()
    )

    prompt = f"""
## Fleet-Wide CSI Analysis

**Total Customers Scored:** {total:,}
**Average CSI Score:** {avg}

### Distribution:
{breakdown}

Please provide an executive summary covering:
1. **Overall Health Assessment** – Is the customer base healthy?
2. **Top Concerns** – Which segments need urgent attention?
3. **Positive Highlights** – What is working well?
4. **Strategic Recommendations** – 3 data-driven actions for management.
"""
    return _call_groq(prompt)
