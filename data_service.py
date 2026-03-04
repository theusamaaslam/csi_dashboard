"""
data_service.py — All query functions for the CSI Dashboard.
Each function returns a pandas DataFrame or dict ready for Plotly charts.
"""
import pandas as pd
from datetime import datetime, timedelta
from db import query_df, local_engine, ai_engine, dwh_engine
from config import LOCAL_DB_TABLE, CSI_CATEGORIES


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _default_dates():
    end   = datetime.now()
    start = end - timedelta(days=90)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


# ─────────────────────────────────────────────────────────────────────────────
# 1. KPI SUMMARY
# ─────────────────────────────────────────────────────────────────────────────

def get_csi_summary(date_from: str = None, date_to: str = None) -> dict:
    """
    Returns total counts and percentages for each CSI category.
    Returns safe empty dict on any DB error.
    """
    if not date_from or not date_to:
        date_from, date_to = _default_dates()

    try:
        sql = f"""
            SELECT csi_category, COUNT(*) AS cnt, AVG(predicted_csi) AS avg_score
            FROM {LOCAL_DB_TABLE}
            WHERE run_date::date BETWEEN :d1 AND :d2
            GROUP BY csi_category
        """
        df = query_df(local_engine, sql, {"d1": date_from, "d2": date_to})
        total = int(df["cnt"].sum()) if not df.empty else 0
        result = {"total": total, "by_category": {}, "avg_score": 0.0}
        if total > 0:
            result["avg_score"] = round(float(df["avg_score"].mean()), 1)
            for _, row in df.iterrows():
                cat = row["csi_category"]
                cnt = int(row["cnt"])
                result["by_category"][cat] = {
                    "count": cnt,
                    "pct": round(cnt / total * 100, 1),
                }
        return result
    except Exception as e:
        print(f"[data_service] get_csi_summary error: {e}")
        return {"total": 0, "by_category": {}, "avg_score": 0.0}


# ─────────────────────────────────────────────────────────────────────────────
# 2. CSI TREND (line / bar)
# ─────────────────────────────────────────────────────────────────────────────

def get_csi_trend(date_from: str, date_to: str, granularity: str = "day") -> pd.DataFrame:
    """
    Returns counts per CSI category grouped by date/week/month.
    Returns empty DataFrame on any DB error.
    """
    trunc_map = {"day": "day", "week": "week", "month": "month"}
    trunc = trunc_map.get(granularity, "day")
    try:
        sql = f"""
            SELECT
                DATE_TRUNC('{trunc}', run_date) AS period,
                csi_category,
                COUNT(*) AS cnt
            FROM {LOCAL_DB_TABLE}
            WHERE run_date::date BETWEEN :d1 AND :d2
            GROUP BY 1, 2
            ORDER BY 1
        """
        return query_df(local_engine, sql, {"d1": date_from, "d2": date_to})
    except Exception as e:
        print(f"[data_service] get_csi_trend error: {e}")
        return pd.DataFrame(columns=["period", "csi_category", "cnt"])


# ─────────────────────────────────────────────────────────────────────────────
# 3. OCCURRENCE DISTRIBUTION (bar, descending by very poor %)
# ─────────────────────────────────────────────────────────────────────────────

def get_occurrence_by_period(date_from: str, date_to: str,
                              category: str = "Very Poor",
                              granularity: str = "month") -> pd.DataFrame:
    trunc_map = {"day": "day", "week": "week", "month": "month"}
    trunc = trunc_map.get(granularity, "month")
    try:
        sql = f"""
            SELECT
                DATE_TRUNC('{trunc}', run_date) AS period,
                COUNT(*) FILTER (WHERE csi_category = :cat) AS selected_count,
                COUNT(*) AS total_count
            FROM {LOCAL_DB_TABLE}
            WHERE run_date::date BETWEEN :d1 AND :d2
            GROUP BY 1
            ORDER BY selected_count DESC
        """
        return query_df(local_engine, sql, {"d1": date_from, "d2": date_to, "cat": category})
    except Exception as e:
        print(f"[data_service] get_occurrence_by_period error: {e}")
        return pd.DataFrame(columns=["period", "selected_count", "total_count"])


# ─────────────────────────────────────────────────────────────────────────────
# 4. SERVICES BREAKDOWN
# ─────────────────────────────────────────────────────────────────────────────

def get_service_breakdown(category: str = "Very Poor",
                           date_from: str = None, date_to: str = None) -> pd.DataFrame:
    """
    Returns count/% of selected category per service.
    5 categories: Internet, VOIP, Video, VAS, Network Issue.
    - Internet/Video/VAS/VOIP sourced from ai.activity (services column) and dwh for VOIP.
    - Network Issue = CSI users with NO record in ai.activity (no known service).
    All 5 labels are always present (zero-filled if absent).
    """
    if not date_from or not date_to:
        date_from, date_to = _default_dates()

    ALL_SERVICES = ["Internet", "VOIP", "Video", "VAS", "Network Issue"]

    try:
        from sqlalchemy import text
        from db import ai_engine, dwh_engine

        # ── Step 1: get CSI user IDs for the chosen category ───────────────
        id_sql = f"""
            SELECT DISTINCT userid FROM {LOCAL_DB_TABLE}
            WHERE csi_category = :cat
              AND run_date::date BETWEEN :d1 AND :d2
        """
        ids_df = query_df(local_engine, id_sql,
                          {"cat": category, "d1": date_from, "d2": date_to})
        if ids_df.empty:
            empty = pd.DataFrame({"service": ALL_SERVICES, "cnt": [0] * len(ALL_SERVICES)})
            return empty

        all_ids = set(str(x) for x in ids_df["userid"].tolist())
        in_clause = ", ".join(f"'{x}'" for x in all_ids)

        # ── Step 2: query ai.activity for service per user ──────────────────
        with ai_engine.connect() as conn:
            result = conn.execute(
                text(f"SELECT userid::text AS userid, UPPER(services) AS service "
                     f"FROM ai.activity WHERE userid IN ({in_clause}) "
                     f"AND services IS NOT NULL")
            )
            act_rows = result.fetchall()

        act_df = pd.DataFrame(act_rows, columns=["userid", "service"]) if act_rows else \
                 pd.DataFrame(columns=["userid", "service"])

        # ── Step 3: Map raw service strings → canonical 5 categories ───────
        # Comprehensive map built from actual DB values
        def _map_service(raw: str) -> str:
            raw = str(raw).upper().strip()
            # Multi-value entries — check for dominant token
            if not raw or raw in ("ALL", "NONE", ""):
                return "Network Issue"   # multi/unknown → Network Issue
            tokens = [t.strip() for t in raw.replace(",", "|").split("|")]
            # Priority order: if any token matches, use that service
            for token in tokens:
                if token in ("INTERNET", "PREMIUM-INTERNET", "CVLAS_INTERNET",
                             "CVAS_INTERNET", "UNLIMITED_BUNDLE"):
                    return "Internet"
                if token in ("POTS", "VOIP", "TELEPHONY", "PHONE"):
                    return "VOIP"
                if token in ("VIDEO", "BASIC-CABLE-TV", "DIGITAL_SIGNAGE",
                             "TV", "CABLE"):
                    return "Video"
                if token in ("NAYATV", "NAYATEL_TV", "JOYBOX", "EVIEW",
                             "HOSTEX", "NAYATEL_CLOUD", "NAYATEL CLOUD",
                             "NWATCH", "NAYATEL_VPN", "WHATSAPP-AUTO-CHATBOT",
                             "VAS"):
                    return "VAS"
            # Contains keyword checks for unrecognised compound values
            if "INTERNET" in raw or "PREMIUM" in raw:
                return "Internet"
            if "VIDEO" in raw or "CABLE" in raw or "TV" in raw:
                return "Video"
            if "POTS" in raw or "VOIP" in raw:
                return "VOIP"
            # Anything else with no match → Network Issue
            return "Network Issue"

        if not act_df.empty:
            act_df["mapped"] = act_df["service"].map(_map_service)
            # One user can appear multiple times; take the most common service per user
            user_service = (
                act_df.groupby(["userid", "mapped"])
                .size().reset_index(name="n")
                .sort_values("n", ascending=False)
                .drop_duplicates(subset="userid", keep="first")[["userid", "mapped"]]
            )
            service_counts = user_service["mapped"].value_counts().to_dict()
            known_ids = set(user_service["userid"].tolist())
        else:
            service_counts = {}
            known_ids = set()

        # ── Step 4: Users with NO activity record → Network Issue ───────────
        missing_ids = all_ids - known_ids

        # ── Step 5: Try dwh.customers to rescue VOIP users ─────────────────
        # Some VOIP users won't appear in ai.activity at all.
        # If we find service-type columns in dwh.customers, use them.
        voip_extra = 0
        try:
            if missing_ids:
                miss_clause = ", ".join(f"'{x}'" for x in missing_ids)
                with dwh_engine.connect() as conn:
                    # Try to find VOIP/POTS customers in dwh.customers
                    r = conn.execute(
                        text(
                            f"SELECT COUNT(DISTINCT customer_id) AS cnt "
                            f"FROM dwh.customers "
                            f"WHERE customer_id IN ({miss_clause}) "
                            f"AND (LOWER(type) LIKE '%voip%' OR LOWER(type) LIKE '%pots%' "
                            f"     OR LOWER(type) LIKE '%phone%' OR LOWER(type) LIKE '%telephony%')"
                        )
                    )
                    row = r.fetchone()
                    if row and row[0]:
                        voip_extra = int(row[0])
        except Exception as ve:
            print(f"[data_service] VOIP dwh fallback: {ve}")

        # Assign Network Issue count (minus VOIP rescued from dwh)
        network_issue_cnt = max(0, len(missing_ids) - voip_extra)
        if voip_extra > 0:
            service_counts["VOIP"] = service_counts.get("VOIP", 0) + voip_extra
        service_counts["Network Issue"] = service_counts.get("Network Issue", 0) + network_issue_cnt

        # ── Step 6: Build final DataFrame with all 5 categories ─────────────
        rows = [{"service": svc, "cnt": service_counts.get(svc, 0)} for svc in ALL_SERVICES]
        df = pd.DataFrame(rows).sort_values("cnt", ascending=False)
        return df

    except Exception as e:
        print(f"[data_service] get_service_breakdown error: {e}")
        empty = pd.DataFrame({"service": ALL_SERVICES, "cnt": [0] * len(ALL_SERVICES)})
        return empty


# ─────────────────────────────────────────────────────────────────────────────
# 5. FAULT BREAKDOWN (Master Fault → Sub Fault)
# ─────────────────────────────────────────────────────────────────────────────

def get_fault_types(category: str = "Very Poor",
                     date_from: str = None, date_to: str = None,
                     service_filter: str = None) -> pd.DataFrame:
    """
    Master fault type counts for selected category.
    When service_filter is provided (e.g. 'Internet'), only CTI records
    belonging to users of THAT service (via ai.activity) are counted.
    """
    if not date_from or not date_to:
        date_from, date_to = _default_dates()

    id_sql = f"""
        SELECT DISTINCT userid FROM {LOCAL_DB_TABLE}
        WHERE csi_category = :cat
          AND run_date::date BETWEEN :d1 AND :d2
    """
    ids_df = query_df(local_engine, id_sql, {"cat": category, "d1": date_from, "d2": date_to})
    if ids_df.empty:
        return pd.DataFrame(columns=["fault_type", "cnt"])

    ids = [str(x) for x in ids_df["userid"].tolist()]
    from db import ai_engine
    from sqlalchemy import text
    if not ids:
        return pd.DataFrame(columns=["fault_type", "cnt"])

    in_clause = ", ".join(f"'{x}'" for x in ids)

    # ── Service filter: narrow IDs to only those whose activity service
    #    matches the selected service (Internet, VOIP, Video, VAS, Network Issue)
    if service_filter and service_filter.lower() != "all":
        REVERSE_MAP = {
            "Internet":       ["INTERNET", "PREMIUM-INTERNET", "CVLAS_INTERNET",
                               "CVAS_INTERNET", "UNLIMITED_BUNDLE"],
            "VOIP":           ["POTS", "VOIP", "TELEPHONY", "PHONE"],
            "Video":          ["VIDEO", "BASIC-CABLE-TV", "DIGITAL_SIGNAGE"],
            "VAS":            ["NAYATV", "NAYATEL_TV", "JOYBOX", "EVIEW",
                               "HOSTEX", "NAYATEL_CLOUD", "NAYATEL CLOUD",
                               "NWATCH", "NAYATEL_VPN", "WHATSAPP-AUTO-CHATBOT"],
            "Network Issue":  [],   # users with no activity record
        }
        with ai_engine.connect() as conn:
            if service_filter == "Network Issue":
                # Network Issue users = those with NO activity record
                r = conn.execute(
                    text(f"SELECT DISTINCT userid::text FROM ai.activity "
                         f"WHERE userid IN ({in_clause})")
                )
                act_ids = set(row[0] for row in r.fetchall())
                filtered = [x for x in ids if x not in act_ids]
            else:
                raw_svcs = REVERSE_MAP.get(service_filter, [])
                if raw_svcs:
                    svc_clause = ", ".join(f"'{s}'" for s in raw_svcs)
                    r = conn.execute(
                        text(f"SELECT DISTINCT userid::text FROM ai.activity "
                             f"WHERE userid IN ({in_clause}) "
                             f"AND UPPER(services) IN ({svc_clause})")
                    )
                else:
                    # Generic LIKE fallback for partial matches
                    r = conn.execute(
                        text(f"SELECT DISTINCT userid::text FROM ai.activity "
                             f"WHERE userid IN ({in_clause}) "
                             f"AND UPPER(services) LIKE '%{service_filter.upper()}%'")
                    )
                filtered = [row[0] for row in r.fetchall()]

        if not filtered:
            return pd.DataFrame(columns=["fault_type", "cnt"])
        in_clause = ", ".join(f"'{x}'" for x in filtered)

    with ai_engine.connect() as conn:
        result = conn.execute(
            text(f"SELECT master_fault_type AS fault_type, COUNT(*) AS cnt "
                 f"FROM ai.cti WHERE userid IN ({in_clause}) "
                 f"AND master_fault_type IS NOT NULL AND master_fault_type != 'nan' "
                 f"GROUP BY 1 ORDER BY cnt DESC LIMIT 20")
        )
        rows = result.fetchall()
        cols = list(result.keys())
    return pd.DataFrame(rows, columns=cols)


def get_sub_fault_types(category: str = "Very Poor",
                         master_fault: str = None,
                         date_from: str = None, date_to: str = None) -> pd.DataFrame:
    """Sub fault type counts for a given master fault."""
    if not date_from or not date_to:
        date_from, date_to = _default_dates()
    if not master_fault:
        return pd.DataFrame(columns=["sub_fault_type", "cnt"])

    id_sql = f"""
        SELECT DISTINCT userid FROM {LOCAL_DB_TABLE}
        WHERE csi_category = :cat
          AND run_date::date BETWEEN :d1 AND :d2
    """
    ids_df = query_df(local_engine, id_sql, {"cat": category, "d1": date_from, "d2": date_to})
    if ids_df.empty:
        return pd.DataFrame(columns=["sub_fault_type", "cnt"])

    ids = [str(x) for x in ids_df["userid"].tolist()]
    from db import ai_engine
    from sqlalchemy import text
    if not ids:
        return pd.DataFrame(columns=["sub_fault_type", "cnt"])
    in_clause = ", ".join(f"'{x}'" for x in ids)
    with ai_engine.connect() as conn:
        result = conn.execute(
            text(f"SELECT sub_fault_type, COUNT(*) AS cnt FROM ai.cti "
                 f"WHERE userid IN ({in_clause}) AND master_fault_type = :mft "
                 f"AND sub_fault_type IS NOT NULL AND sub_fault_type != 'nan' "
                 f"GROUP BY 1 ORDER BY cnt DESC LIMIT 20"),
            {"mft": master_fault}
        )
        rows = result.fetchall()
        cols = list(result.keys())
    return pd.DataFrame(rows, columns=cols)


# ─────────────────────────────────────────────────────────────────────────────
# 6. CITY BREAKDOWN (City → Area → Sub-Area)
# ─────────────────────────────────────────────────────────────────────────────

def get_city_breakdown(category: str = "Very Poor",
                        city: str = None, area: str = None,
                        date_from: str = None, date_to: str = None) -> pd.DataFrame:
    """
    Returns drill-down data: if no city → city counts, if city → areas, if area → sub-areas.
    Uses dwh.customers for city/area/sub-area.
    """
    if not date_from or not date_to:
        date_from, date_to = _default_dates()

    id_sql = f"""
        SELECT DISTINCT userid FROM {LOCAL_DB_TABLE}
        WHERE csi_category = :cat
          AND run_date::date BETWEEN :d1 AND :d2
    """
    ids_df = query_df(local_engine, id_sql, {"cat": category, "d1": date_from, "d2": date_to})
    selected_ids = [str(x) for x in ids_df["userid"].tolist()] if not ids_df.empty else []

    # Total active customers (denominator)
    from db import dwh_engine
    from sqlalchemy import text

    try:
        if city is None:
            # City level
            if not selected_ids:
                return pd.DataFrame(columns=["label", "selected_cnt", "total_cnt", "pct"])
            in_clause = ", ".join(f"'{x}'" for x in selected_ids)
            with dwh_engine.connect() as conn:
                # Selected category count per city
                r_sel = conn.execute(
                    text(f"SELECT city, COUNT(*) AS selected_cnt "
                         f"FROM dwh.customers WHERE customer_id IN ({in_clause}) "
                         f"AND city IS NOT NULL GROUP BY city ORDER BY selected_cnt DESC")
                )
                sel_df = pd.DataFrame(r_sel.fetchall(), columns=list(r_sel.keys()))
                # Total per city
                r_tot = conn.execute(
                    text("SELECT city, COUNT(*) AS total_cnt FROM dwh.customers "
                         "WHERE status='ACTIVE' AND city IS NOT NULL GROUP BY city")
                )
                tot_df = pd.DataFrame(r_tot.fetchall(), columns=list(r_tot.keys()))
            df = sel_df.merge(tot_df, on="city", how="left").fillna(0)
            df["pct"] = (df["selected_cnt"] / df["total_cnt"].replace(0, 1) * 100).round(1)
            return df.rename(columns={"city": "label"})

        elif area is None:
            # Area level within city
            if not selected_ids:
                return pd.DataFrame(columns=["label", "selected_cnt", "total_cnt", "pct"])
            in_clause = ", ".join(f"'{x}'" for x in selected_ids)
            with dwh_engine.connect() as conn:
                r_sel = conn.execute(
                    text(f"SELECT subdept AS area, COUNT(*) AS selected_cnt "
                         f"FROM dwh.customers WHERE customer_id IN ({in_clause}) "
                         f"AND city = :city AND subdept IS NOT NULL "
                         f"GROUP BY subdept ORDER BY selected_cnt DESC"),
                    {"city": city}
                )
                sel_df = pd.DataFrame(r_sel.fetchall(), columns=list(r_sel.keys()))
                r_tot = conn.execute(
                    text("SELECT subdept AS area, COUNT(*) AS total_cnt "
                         "FROM dwh.customers WHERE status='ACTIVE' AND city = :city "
                         "AND subdept IS NOT NULL GROUP BY subdept"),
                    {"city": city}
                )
                tot_df = pd.DataFrame(r_tot.fetchall(), columns=list(r_tot.keys()))
            df = sel_df.merge(tot_df, on="area", how="left").fillna(0)
            df["pct"] = (df["selected_cnt"] / df["total_cnt"].replace(0, 1) * 100).round(1)
            return df.rename(columns={"area": "label"})

        else:
            # Sub-area level (using location from cti as proxy)
            if not selected_ids:
                return pd.DataFrame(columns=["label", "selected_cnt", "total_cnt", "pct"])
            in_clause = ", ".join(f"'{x}'" for x in selected_ids)
            with ai_engine.connect() as conn:
                r_sel = conn.execute(
                    text(f"SELECT location AS sublabel, COUNT(DISTINCT userid) AS selected_cnt "
                         f"FROM ai.cti WHERE userid IN ({in_clause}) "
                         f"AND location IS NOT NULL AND location != 'nan' "
                         f"GROUP BY location ORDER BY selected_cnt DESC LIMIT 20")
                )
                df = pd.DataFrame(r_sel.fetchall(), columns=list(r_sel.keys()))
            df["total_cnt"] = df["selected_cnt"]
            df["pct"] = 100.0
            return df.rename(columns={"sublabel": "label"})
    except Exception as e:
        print(f"[data_service] get_city_breakdown error: {e}")
        return pd.DataFrame(columns=["label", "selected_cnt", "total_cnt", "pct"])


# ─────────────────────────────────────────────────────────────────────────────
# 7. BNG / OSP BREAKDOWN
# ─────────────────────────────────────────────────────────────────────────────

def get_bng_breakdown(category: str = "Very Poor",
                       date_from: str = None, date_to: str = None) -> pd.DataFrame:
    """
    Returns BNG-level counts using the BNG_ROUTERS mapping from config.
    Because DWH may or may not have BNG, we return the static BNG list with counts
    approximated from city distribution.
    """
    from config import BNG_ROUTERS
    city_df = get_city_breakdown(category=category, date_from=date_from, date_to=date_to)

    CITY_BNG_MAP = {
        "Islamabad": "Islamabad BNG",
        "Rawalpindi": "Rawalpindi BNG",
        "Lahore": "Lahore BNG",
        "Faisalabad": "Faisalabad BNG",
        "Gujranwala": "Gujranwala/Sialkot BNG",
        "Sialkot": "Gujranwala/Sialkot BNG",
        "Peshawar": "Peshawar BNG",
        "Multan": "Multan/Muzaffargarh",
        "Muzaffargarh": "Multan/Muzaffargarh",
        "Sargodha": "Sargodha BNG",
    }
    if city_df.empty:
        return pd.DataFrame(columns=["bng", "selected_cnt", "total_cnt", "pct"])

    rows = []
    for _, r in city_df.iterrows():
        city_name = r["label"]
        bng = next((v for k, v in CITY_BNG_MAP.items() if k.lower() in city_name.lower()), "Other BNG")
        rows.append({"bng": bng,
                      "selected_cnt": r.get("selected_cnt", 0),
                      "total_cnt": r.get("total_cnt", 1),
                      "pct": r.get("pct", 0)})

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    return df.groupby("bng", as_index=False).agg(
        selected_cnt=("selected_cnt", "sum"),
        total_cnt=("total_cnt", "sum")
    ).assign(pct=lambda d: (d["selected_cnt"] / d["total_cnt"].replace(0, 1) * 100).round(1)) \
     .sort_values("selected_cnt", ascending=False)


# ─────────────────────────────────────────────────────────────────────────────
# 8. PACKAGE BREAKDOWN
# ─────────────────────────────────────────────────────────────────────────────

def get_package_breakdown(category: str = "Very Poor",
                           date_from: str = None, date_to: str = None) -> pd.DataFrame:
    if not date_from or not date_to:
        date_from, date_to = _default_dates()

    id_sql = f"""
        SELECT DISTINCT userid FROM {LOCAL_DB_TABLE}
        WHERE csi_category = :cat AND run_date::date BETWEEN :d1 AND :d2
    """
    ids_df = query_df(local_engine, id_sql, {"cat": category, "d1": date_from, "d2": date_to})
    if ids_df.empty:
        return pd.DataFrame(columns=["package", "cnt"])

    ids = [str(x) for x in ids_df["userid"].tolist()]
    try:
        from db import ai_engine
        import pandas as pd
        if not ids:
            return pd.DataFrame(columns=["package", "cnt"])
        
        # Determine the total number of IDs in the category
        total_ids = len(ids)

        in_clause = ", ".join(f"'{x}'" for x in ids)
        with ai_engine.connect() as conn:
            from sqlalchemy import text
            result = conn.execute(
                text(f"SELECT planname AS package, COUNT(DISTINCT userid) AS cnt "
                     f"FROM ai.plans WHERE userid IN ({in_clause}) "
                     f"AND planname IS NOT NULL GROUP BY planname ORDER BY cnt DESC LIMIT 24")
            )
            df = pd.DataFrame(result.fetchall(), columns=list(result.keys()))
            
        # Calculate how many IDs are missing from the ai.plans query
        found_cnt = df['cnt'].sum() if not df.empty else 0
        missing_cnt = total_ids - found_cnt
        
        if missing_cnt > 0:
            unknown_df = pd.DataFrame([{"package": "Unknown", "cnt": missing_cnt}])
            df = pd.concat([df, unknown_df], ignore_index=True)
            
        return df
    except Exception as e:
        print(f"[data_service] get_package_breakdown error: {e}")
        return pd.DataFrame(columns=["package", "cnt"])


# ─────────────────────────────────────────────────────────────────────────────
# 9. HARDWARE (ONT) BREAKDOWN
# ─────────────────────────────────────────────────────────────────────────────

def get_hardware_breakdown(category: str = "Very Poor",
                            date_from: str = None, date_to: str = None) -> pd.DataFrame:
    if not date_from or not date_to:
        date_from, date_to = _default_dates()

    id_sql = f"""
        SELECT DISTINCT userid FROM {LOCAL_DB_TABLE}
        WHERE csi_category = :cat AND run_date::date BETWEEN :d1 AND :d2
    """
    ids_df = query_df(local_engine, id_sql, {"cat": category, "d1": date_from, "d2": date_to})
    if ids_df.empty:
        return pd.DataFrame(columns=["hardware", "cnt"])

    ids = [str(x) for x in ids_df["userid"].tolist()]
    try:
        from db import dwh_engine
        import pandas as pd
        if not ids:
            return pd.DataFrame(columns=["hardware", "cnt"])
            
        total_ids = len(ids)
        in_clause = ", ".join(f"'{x}'" for x in ids)
        
        with dwh_engine.connect() as conn:
            from sqlalchemy import text
            result = conn.execute(
                text(f"SELECT hardware_name AS hardware, COUNT(*) AS cnt "
                     f"FROM dwh.customers_equipment WHERE customer_id IN ({in_clause}) "
                     f"AND hardware_category ILIKE '%ONT%' "
                     f"AND hardware_name IS NOT NULL GROUP BY hardware_name ORDER BY cnt DESC LIMIT 19")
            )
            df = pd.DataFrame(result.fetchall(), columns=list(result.keys()))
            
        # Calculate how many IDs are missing from the dwh.customers_equipment query
        found_cnt = df['cnt'].sum() if not df.empty else 0
        missing_cnt = total_ids - found_cnt
        
        if missing_cnt > 0:
            unknown_df = pd.DataFrame([{"hardware": "Unknown", "cnt": missing_cnt}])
            df = pd.concat([df, unknown_df], ignore_index=True)
            
        return df
    except Exception as e:
        print(f"[data_service] get_hardware_breakdown error: {e}")
        return pd.DataFrame(columns=["hardware", "cnt"])


# ─────────────────────────────────────────────────────────────────────────────
# 10. INSTALLATION YEAR BREAKDOWN
# ─────────────────────────────────────────────────────────────────────────────

def get_install_year_breakdown(category: str = "Very Poor",
                                date_from: str = None, date_to: str = None) -> pd.DataFrame:
    if not date_from or not date_to:
        date_from, date_to = _default_dates()

    id_sql = f"""
        SELECT DISTINCT userid FROM {LOCAL_DB_TABLE}
        WHERE csi_category = :cat AND run_date::date BETWEEN :d1 AND :d2
    """
    ids_df = query_df(local_engine, id_sql, {"cat": category, "d1": date_from, "d2": date_to})
    if ids_df.empty:
        return pd.DataFrame(columns=["install_year", "cnt"])

    ids = [str(x) for x in ids_df["userid"].tolist()]
    try:
        from db import dwh_engine
        import pandas as pd
        if not ids:
            return pd.DataFrame(columns=["install_year", "cnt"])
            
        total_ids = len(ids)
        in_clause = ", ".join(f"'{x}'" for x in ids)
        
        with dwh_engine.connect() as conn:
            from sqlalchemy import text
            result = conn.execute(
                text(f"SELECT EXTRACT(YEAR FROM activation_datetime)::int AS install_year, COUNT(DISTINCT customer_id) AS cnt "
                     f"FROM dwh.lifecycle WHERE customer_id IN ({in_clause}) "
                     f"AND activation_type = 'New Customer' "
                     f"AND activation_datetime IS NOT NULL "
                     f"GROUP BY 1 ORDER BY 1")
            )
            df = pd.DataFrame(result.fetchall(), columns=list(result.keys()))
            
        # Calculate how many IDs are missing from the query
        found_cnt = df['cnt'].sum() if not df.empty else 0
        missing_cnt = total_ids - found_cnt
        
        if missing_cnt > 0:
            unknown_df = pd.DataFrame([{"install_year": "Unknown", "cnt": missing_cnt}])
            df = pd.concat([df, unknown_df], ignore_index=True)
            
        return df
    except Exception as e:
        print(f"[data_service] get_install_year_breakdown error: {e}")
        return pd.DataFrame(columns=["install_year", "cnt"])


# ─────────────────────────────────────────────────────────────────────────────
# 11. CUSTOMER LIST (paginated)
# ─────────────────────────────────────────────────────────────────────────────

def get_customer_list(
    category: str = None,
    userid_filter: str = None,
    city_filter: str = None,
    date_from: str = None,
    date_to: str = None,
    page: int = 1,
    page_size: int = 50,
) -> tuple[pd.DataFrame, int]:
    """Returns (dataframe, total_count). Safe on DB error."""
    if not date_from or not date_to:
        date_from, date_to = _default_dates()

    try:
        conditions = ["cs.run_date::date BETWEEN :d1 AND :d2"]
        params: dict = {"d1": date_from, "d2": date_to}

        if category:
            conditions.append("cs.csi_category = :cat")
            params["cat"] = category
        if userid_filter:
            conditions.append("cs.userid ILIKE :uid")
            params["uid"] = f"%{userid_filter}%"

        where_clause = "WHERE " + " AND ".join(conditions)
        offset = (page - 1) * page_size

        count_sql = f"SELECT COUNT(*) FROM {LOCAL_DB_TABLE} cs {where_clause}"
        count_df = query_df(local_engine, count_sql, params)
        total = int(count_df.iloc[0, 0]) if not count_df.empty else 0

        data_sql = f"""
            SELECT
                cs.userid,
                cs.total_calls,
                cs.total_tickets,
                cs.total_outages,
                cs.total_activities,
                cs.predicted_csi,
                cs.csi_category,
                cs.run_date
            FROM {LOCAL_DB_TABLE} cs
            {where_clause}
            ORDER BY cs.predicted_csi ASC
            LIMIT :lim OFFSET :off
        """
        params["lim"] = page_size
        params["off"] = offset
        df = query_df(local_engine, data_sql, params)
        return df, total
    except Exception as e:
        print(f"[data_service] get_customer_list error: {e}")
        return pd.DataFrame(), 0


# ─────────────────────────────────────────────────────────────────────────────
# 12. CUSTOMER JOURNEY (for AI analysis)
# ─────────────────────────────────────────────────────────────────────────────

def get_customer_journey(userid: str) -> dict:
    """
    Returns a rich dict with all recent interactions for a customer,
    ready to be formatted into a Groq prompt.
    """
    uid = str(userid)

    # CSI score
    csi_df = query_df(
        local_engine,
        f"SELECT * FROM {LOCAL_DB_TABLE} WHERE userid = :uid ORDER BY run_date DESC LIMIT 1",
        {"uid": uid}
    )

    # Trouble tickets (last 20)
    try:
        from db import ai_engine
        from sqlalchemy import text
        with ai_engine.connect() as conn:
            r = conn.execute(
                text("SELECT ticket_type, fault_types, sub_fault_types, duration, creation_time, location "
                     "FROM ai.trouble_tickets WHERE userid = :uid ORDER BY creation_time DESC LIMIT 20"),
                {"uid": uid}
            )
            tickets = pd.DataFrame(r.fetchall(), columns=list(r.keys()))
    except:
        tickets = pd.DataFrame()

    # CTI calls (last 20)
    try:
        with ai_engine.connect() as conn:
            r = conn.execute(
                text("SELECT call_detail_log_group, master_fault_type, sub_fault_type, call_duration, entry_time, comments, location "
                     "FROM ai.cti WHERE userid = :uid ORDER BY entry_time DESC LIMIT 20"),
                {"uid": uid}
            )
            calls = pd.DataFrame(r.fetchall(), columns=list(r.keys()))
    except:
        calls = pd.DataFrame()

    # Outages (last 10)
    try:
        with ai_engine.connect() as conn:
            r = conn.execute(
                text("SELECT event_type, duration, occurrence_time, description, location "
                     "FROM ai.outages WHERE userid = :uid ORDER BY occurrence_time DESC LIMIT 10"),
                {"uid": uid}
            )
            outages = pd.DataFrame(r.fetchall(), columns=list(r.keys()))
    except:
        outages = pd.DataFrame()

    # Activities (last 10)
    try:
        with ai_engine.connect() as conn:
            r = conn.execute(
                text("SELECT activity_name, services, status, customer_downtime_hours, occurrence_time, location "
                     "FROM ai.activity WHERE userid = :uid ORDER BY occurrence_time DESC LIMIT 10"),
                {"uid": uid}
            )
            activities = pd.DataFrame(r.fetchall(), columns=list(r.keys()))
    except:
        activities = pd.DataFrame()

    return {
        "userid": uid,
        "csi": csi_df.to_dict("records")[0] if not csi_df.empty else {},
        "tickets": tickets.to_dict("records"),
        "calls": calls.to_dict("records"),
        "outages": outages.to_dict("records"),
        "activities": activities.to_dict("records"),
    }
