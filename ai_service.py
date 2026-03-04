"""
ai_service.py — Groq LLM integration for on-demand CSI analysis.
"""
from groq import Groq
from config import GROQ_API_KEY, GROQ_MODEL

_client = Groq(api_key=GROQ_API_KEY)

FALLBACK_MODEL = "openai/gpt-oss-120b"


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

Based on the Nayatel Telecom CSI data above, please provide a highly detailed, analytical response for the Management/NOC team:
1. **Root Cause Analysis** – What technical or operational factors typically drive customers into the '{category}' segment? Refer to specific data points if available.
2. **Key Risk Indicators** – What specific patterns (in calls, tickets, services, or locations) should TAC Level-1 and Level-2 agents watch for to intercept these customers before churn?
3. **Immediate Actions** – Provide 3–5 concrete, actionable steps to improve this specific segment's experience immediately.
4. **Strategic Trends** – Any notable observations from the distribution above, and how it impacts overall network health.

*Keep the response highly professional, data-centric, and formatted nicely with markdown headers and bullet points.*
"""
    return _call_groq(prompt)


def analyze_customer(journey: dict) -> str:
    """
    Generate a per-customer journey summary for TAC agents.
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
                              ["creation_time", "ticket_type", "fault_types", "sub_fault_types", "location", "duration"])
    calls_str    = _fmt_list(journey.get("calls", []),
                              ["entry_time", "call_detail_log_group", "master_fault_type", "sub_fault_type", "comments", "location", "call_duration"])
    outages_str  = _fmt_list(journey.get("outages", []),
                              ["occurrence_time", "event_type", "description", "location", "duration"])
    activities_str = _fmt_list(journey.get("activities", []),
                                ["occurrence_time", "activity_name", "services", "status", "location", "customer_downtime_hours"])

    prompt = f"""
## Detailed Customer Journey Analysis Request

**Customer ID:** {uid}
**Predicted CSI Score:** {score}  |  **Current Category:** {category}

### Recent Trouble Tickets (Last 20):
{tickets_str}

### Recent CTI Calls & Comments (Last 20):
{calls_str}

### Recent Network Outages (Last 10):
{outages_str}

### Recent Maintenance/Activities (Last 10):
{activities_str}

---

Please act as a Senior Technical Analyst at Nayatel. Analyze this customer's exact history from the past 3 months based *strictly* on the rich data provided above (including specific agent comments, fault types, sub-faults, location outages, etc.).

Provide a highly detailed, comprehensive report formatted using Markdown. It must include:

1. **Why they are in this CSI Category** – Analytically explain what exact sequence of events (e.g., repeated specific faults, excessive downtime, poor resolution on calls) caused their CSI score to reach '{category}'. Reference their actual data.
2. **Detailed Date-wise Timeline (Past 3 Months)** – Provide a meticulous chronological breakdown, date by date, of exactly what this customer has experienced. Do not skip dates where multiple things happened. Detail every ticket, call, and outage chronologically to paint a picture of their friction. Explain the technical issues they likely faced on each day.
3. **Root Cause Analysis** – Dive into the technical fault types and sub-faults. Is there an ongoing localized issue (referencing the location/node)? Is there bad hardware at the premises? Is it a configuration issue?
4. **Actionable Resolution Plan** – Provide exact, step-by-step instructions on how the agent handling this account should resolve their underlying friction, and specifically what engineering team they should route it to if necessary.
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
