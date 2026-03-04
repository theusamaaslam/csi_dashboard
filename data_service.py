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
    Returns count/% of selected category per service (Internet, VOIP, Video, VAS).
    Joins csi_scores → dwh.customers (via customer_id) to get service type.
    """
    if not date_from or not date_to:
        date_from, date_to = _default_dates()

    sql = """
        SELECT
            UPPER(las.service_type) AS service,
            COUNT(*) AS cnt
        FROM dwh.latest_active_services las
        JOIN dwh.customers c ON c.customer_id = las.customer_id
        WHERE las.customer_id IN (
            SELECT userid FROM csi_db.public.csi_scores
            WHERE csi_category = :cat
              AND run_date::date BETWEEN :d1 AND :d2
        )
        GROUP BY 1
        ORDER BY cnt DESC
    """
    # We query dwh_engine here (cross-db via app-layer join if needed)
    # Fallback: use ai.activity services column
    try:
        sql2 = """
            SELECT
                UPPER(a.services) AS service,
                COUNT(DISTINCT a.userid) AS cnt
            FROM ai.activity a
            WHERE a.userid IN (
                SELECT userid::text FROM (VALUES {placeholders}) AS t(userid)
            )
            GROUP BY 1
            ORDER BY cnt DESC
        """
        # Get userids of selected category from local DB
        id_sql = f"""
            SELECT DISTINCT userid FROM {LOCAL_DB_TABLE}
            WHERE csi_category = :cat
              AND run_date::date BETWEEN :d1 AND :d2
            LIMIT 5000
        """
        ids_df = query_df(local_engine, id_sql, {"cat": category, "d1": date_from, "d2": date_to})
        if ids_df.empty:
            return pd.DataFrame(columns=["service", "cnt"])

        ids = tuple(str(x) for x in ids_df["userid"].tolist())
        # Build service mapping from activity table
        params = {"cat": category, "d1": date_from, "d2": date_to}
        act_sql = f"""
            SELECT UPPER(services) AS service, COUNT(DISTINCT userid) AS cnt
            FROM ai.activity
            WHERE userid IN :ids
            GROUP BY 1
            ORDER BY cnt DESC
        """
        from sqlalchemy import text
        from db import ai_engine

        in_clause = ", ".join(f"'{x}'" for x in ids)
        with ai_engine.connect() as conn:
            result = conn.execute(
                text(f"SELECT UPPER(services) AS service, COUNT(DISTINCT userid) AS cnt "
                     f"FROM ai.activity WHERE userid IN ({in_clause}) GROUP BY 1 ORDER BY cnt DESC")
            )
            rows = result.fetchall()
            cols = list(result.keys())
        df = pd.DataFrame(rows, columns=cols)

        # Map to standard 4 services
        service_map = {
            "INTERNET": "Internet", "BASIC-CABLE-TV": "Video", "VIDEO": "Video",
            "POTS": "VOIP", "NAYATV": "VAS", "JOYBOX": "VAS", "EVIEW": "VAS",
            "HOSTEX": "VAS", "NAYATEL CLOUD": "VAS", "NWATCH": "VAS",
            "NAYATEL_VPN": "VAS", "ALL": "Internet",
        }
        df["service"] = df["service"].map(lambda x: service_map.get(x, "VAS"))
        df = df.groupby("service", as_index=False)["cnt"].sum().sort_values("cnt", ascending=False)
        return df
    except Exception as e:
        print(f"[data_service] get_service_breakdown error: {e}")
        return pd.DataFrame(columns=["service", "cnt"])


# ─────────────────────────────────────────────────────────────────────────────
# 5. FAULT BREAKDOWN (Master Fault → Sub Fault)
# ─────────────────────────────────────────────────────────────────────────────

def get_fault_types(category: str = "Very Poor",
                     date_from: str = None, date_to: str = None,
                     service_filter: str = None) -> pd.DataFrame:
    """Master fault type counts for selected category."""
    if not date_from or not date_to:
        date_from, date_to = _default_dates()

    id_sql = f"""
        SELECT DISTINCT userid FROM {LOCAL_DB_TABLE}
        WHERE csi_category = :cat
          AND run_date::date BETWEEN :d1 AND :d2
        LIMIT 5000
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
        LIMIT 5000
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
        LIMIT 5000
    """
    ids_df = query_df(local_engine, id_sql, {"cat": category, "d1": date_from, "d2": date_to})
    if ids_df.empty:
        return pd.DataFrame(columns=["package", "cnt"])

    ids = [str(x) for x in ids_df["userid"].tolist()]
    try:
        from db import ai_engine
        from sqlalchemy import text
        if not ids:
            return pd.DataFrame(columns=["package", "cnt"])
        in_clause = ", ".join(f"'{x}'" for x in ids)
        with ai_engine.connect() as conn:
            result = conn.execute(
                text(f"SELECT planname AS package, COUNT(DISTINCT userid) AS cnt "
                     f"FROM ai.plans WHERE userid IN ({in_clause}) "
                     f"AND planname IS NOT NULL GROUP BY planname ORDER BY cnt DESC LIMIT 25")
            )
            return pd.DataFrame(result.fetchall(), columns=list(result.keys()))
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
        LIMIT 5000
    """
    ids_df = query_df(local_engine, id_sql, {"cat": category, "d1": date_from, "d2": date_to})
    if ids_df.empty:
        return pd.DataFrame(columns=["hardware", "cnt"])

    ids = [str(x) for x in ids_df["userid"].tolist()]
    try:
        from db import dwh_engine
        from sqlalchemy import text
        if not ids:
            return pd.DataFrame(columns=["hardware", "cnt"])
        in_clause = ", ".join(f"'{x}'" for x in ids)
        with dwh_engine.connect() as conn:
            result = conn.execute(
                text(f"SELECT hardware_name AS hardware, COUNT(*) AS cnt "
                     f"FROM dwh.customers_equipment WHERE customer_id IN ({in_clause}) "
                     f"AND hardware_category ILIKE '%ONT%' "
                     f"AND hardware_name IS NOT NULL GROUP BY hardware_name ORDER BY cnt DESC LIMIT 20")
            )
            return pd.DataFrame(result.fetchall(), columns=list(result.keys()))
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
        from sqlalchemy import text
        if not ids:
            return pd.DataFrame(columns=["install_year", "cnt"])
        in_clause = ", ".join(f"'{x}'" for x in ids)
        with dwh_engine.connect() as conn:
            result = conn.execute(
                text(f"SELECT EXTRACT(YEAR FROM start_time)::int AS install_year, COUNT(*) AS cnt "
                     f"FROM dwh.lifecycle WHERE customer_id IN ({in_clause}) "
                     f"AND activation_type = 'ACTIVATION' "
                     f"GROUP BY 1 ORDER BY 1")
            )
            return pd.DataFrame(result.fetchall(), columns=list(result.keys()))
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
                text("SELECT ticket_type, fault_types, sub_fault_types, duration, creation_time "
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
                text("SELECT call_detail_log_group, master_fault_type, sub_fault_type, call_duration, entry_time "
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
                text("SELECT event_type, duration, occurrence_time "
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
                text("SELECT activity_name, services, status, customer_downtime_hours, occurrence_time "
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
