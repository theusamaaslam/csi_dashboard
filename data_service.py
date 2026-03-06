"""
data_service.py — All query functions for the CSI Dashboard.
Each function returns a pandas DataFrame or dict ready for Plotly charts.
"""
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import inspect
from db import query_df, local_engine, ai_engine, dwh_engine
from config import LOCAL_DB_TABLE, CSI_CATEGORIES


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _get_history_tables(limit=5):
    """Returns the base table + up to `limit` historical rotated tables."""
    try:
        inspector = inspect(local_engine)
        all_tables = inspector.get_table_names()
        history = [t for t in all_tables if t.startswith(f"{LOCAL_DB_TABLE}_")]
        history.sort(reverse=True)
        return [LOCAL_DB_TABLE] + history[:limit]
    except Exception as e:
        print(f"Error fetching history tables: {e}")
        return [LOCAL_DB_TABLE]

def _default_dates():
    end   = datetime.now()
    start = end - timedelta(days=90)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")

def _map_fault_to_service(fault_name: str) -> str:
    """Classifies a master fault into one of 5 service buckets: Internet, VOIP, Video, VAS, Network Issue"""
    if not fault_name or str(fault_name).lower() == 'nan':
        return "Unknown"
        
    f = str(fault_name).upper()
    
    # Network Issue keywords
    network_keywords = ["POWER", "OPTICAL", "LINK", "OLT", "SWITCH", "OUTAGE", "FIBER", "CUT", "SPLICE", "DOWN", "NODE", "GPON"]
    if any(k in f for k in network_keywords):
        return "Network Issue"
        
    # VOIP keywords
    voip_keywords = ["VOICE", "VOIP", "CALL", "PHONE", "RING", "DIAL"]
    if any(k in f for k in voip_keywords):
        return "VOIP"
        
    # Video keywords
    video_keywords = ["VIDEO", "TV", "CHANNEL", "CABLE", "STB", "BOX", "JOYBOX", "BROADCAST"]
    if any(k in f for k in video_keywords):
        return "Video"
        
    # VAS keywords
    vas_keywords = ["VAS", "CLOUD", "VPN", "HOSTEX", "CAMERA", "NWATCH", "DOMAIN", "EMAIL"]
    if any(k in f for k in vas_keywords):
        return "VAS"
        
    # Default everything else to Internet (Routing, Browsing, Speed, Disconnect, Config etc)
    return "Internet"



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
    trunc = "day" # Force daily grouping. 
    try:
        tables = _get_history_tables(limit=5)
        union_sq = " UNION ALL ".join([f'SELECT csi_category, run_date FROM "{t}" WHERE run_date::date <= :d2' for t in tables])
        
        sql = f"""
            WITH all_data AS (
                {union_sq}
            ),
            daily_latest AS (
                -- Find the exact latest timestamp for each calendar day
                SELECT 
                    run_date::date as d_date, 
                    MAX(run_date) as max_run_date
                FROM all_data
                WHERE run_date::date <= :d2
                GROUP BY 1
                ORDER BY 1 DESC
                LIMIT 5
            )
            SELECT
                DATE_TRUNC('{trunc}', c.run_date) AS period,
                c.csi_category,
                COUNT(*) AS cnt
            FROM all_data c
            INNER JOIN daily_latest dl ON c.run_date = dl.max_run_date
            GROUP BY 1, 2
            ORDER BY 1
        """
        return query_df(local_engine, sql, {"d2": date_to})
    except Exception as e:
        print(f"[data_service] get_csi_trend error: {e}")
        return pd.DataFrame(columns=["period", "csi_category", "cnt"])


# ─────────────────────────────────────────────────────────────────────────────
# 3. OCCURRENCE DISTRIBUTION (bar, descending by very poor %)
# ─────────────────────────────────────────────────────────────────────────────

def get_occurrence_by_period(date_from: str, date_to: str,
                              category: str = "Very Poor",
                              granularity: str = "month") -> pd.DataFrame:
    trunc = "day" # Force daily grouping for distinct points instead of monthly rollover
    try:
        tables = _get_history_tables(limit=5)
        union_sq = " UNION ALL ".join([f'SELECT csi_category, run_date FROM "{t}" WHERE run_date::date <= :d2' for t in tables])
        
        sql = f"""
            WITH all_data AS (
                {union_sq}
            ),
            daily_latest AS (
                -- Find the exact latest timestamp for each calendar day
                SELECT 
                    run_date::date as d_date, 
                    MAX(run_date) as max_run_date
                FROM all_data
                WHERE run_date::date <= :d2
                GROUP BY 1
                ORDER BY 1 DESC
                LIMIT 5
            )
            SELECT
                DATE_TRUNC('{trunc}', c.run_date) AS period,
                COUNT(*) FILTER (WHERE c.csi_category = :cat) AS selected_count,
                COUNT(*) AS total_count
            FROM all_data c
            INNER JOIN daily_latest dl ON c.run_date = dl.max_run_date
            GROUP BY 1
            ORDER BY 1
        """
        return query_df(local_engine, sql, {"d2": date_to, "cat": category})
    except Exception as e:
        print(f"[data_service] get_occurrence_by_period error: {e}")
        return pd.DataFrame(columns=["period", "selected_count", "total_count"])


# ─────────────────────────────────────────────────────────────────────────────
# 4. SERVICES BREAKDOWN
# ─────────────────────────────────────────────────────────────────────────────

def get_service_breakdown(category: str = "Very Poor",
                           date_from: str = None, date_to: str = None) -> pd.DataFrame:
    """
    Returns count/% of selected category per service (Internet, VOIP, Video, VAS, Network Issue).
    Combines CTI and Tickets data to determine the service issues customers faced.
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
        return pd.DataFrame(columns=["service", "cnt"])

    ids = [str(x) for x in ids_df["userid"].tolist()]
    total_ids = len(ids)

    try:
        from db import ai_engine
        from sqlalchemy import text
        in_clause = ", ".join(f"'{x}'" for x in ids)
        
        # 1. Get CTI Faults
        cti_sql = f"""
            SELECT userid, master_fault_type AS fault_type 
            FROM ai.cti 
            WHERE userid IN ({in_clause}) 
              AND master_fault_type IS NOT NULL AND master_fault_type != 'nan'
        """
        # 2. Get Trouble Tickets Faults
        tt_sql = f"""
            SELECT userid, fault_types AS fault_type 
            FROM ai.trouble_tickets 
            WHERE userid IN ({in_clause}) 
              AND fault_types IS NOT NULL AND fault_types != 'nan'
        """
        
        with ai_engine.connect() as conn:
            cti_res = conn.execute(text(cti_sql))
            tt_res = conn.execute(text(tt_sql))
            cti_rows = cti_res.fetchall()
            tt_rows = tt_res.fetchall()
            
        df_cti = pd.DataFrame(cti_rows, columns=["userid", "fault_type"]) if cti_rows else pd.DataFrame(columns=["userid", "fault_type"])
        df_tt = pd.DataFrame(tt_rows, columns=["userid", "fault_type"]) if tt_rows else pd.DataFrame(columns=["userid", "fault_type"])
        
        # Combine all faults
        all_faults = pd.concat([df_cti, df_tt], ignore_index=True)
        
        if all_faults.empty:
            return pd.DataFrame([{"service": "Unknown", "cnt": total_ids}])
            
        # Map faults to the 5 services
        all_faults["service"] = all_faults["fault_type"].apply(_map_fault_to_service)
        
        # Get primary service for each user (taking the mode or first service they faced)
        user_grouped = all_faults.groupby("userid")["service"].agg(lambda x: x.mode()[0] if not x.mode().empty else "Unknown").reset_index()
        
        df = user_grouped.groupby("service", as_index=False)["userid"].count().rename(columns={"userid": "cnt"}).sort_values("cnt", ascending=False)
        
        found_cnt = df["cnt"].sum()
        missing_cnt = total_ids - found_cnt
        if missing_cnt > 0:
            unknown_df = pd.DataFrame([{"service": "Unknown", "cnt": missing_cnt}])
            df = pd.concat([df, unknown_df], ignore_index=True)
            
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
    """Master fault type counts for selected category, correctly filtered by 5 explicitly mapped Services."""
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
        
    # We want to know how many actual users we are attributing faults to (for the 'Unknown' logic).
    # Since this drilldown is theoretically filtered by service, we first find how many users
    # naturally belong to this service_filter, so we can calculate missing ones.
    
    in_clause = ", ".join(f"'{x}'" for x in ids)
    
    cti_sql = f"""
        SELECT userid, master_fault_type AS fault_type 
        FROM ai.cti 
        WHERE userid IN ({in_clause}) 
          AND master_fault_type IS NOT NULL AND master_fault_type != 'nan'
    """
    tt_sql = f"""
        SELECT userid, fault_types AS fault_type 
        FROM ai.trouble_tickets 
        WHERE userid IN ({in_clause}) 
          AND fault_types IS NOT NULL AND fault_types != 'nan'
    """
    
    try:
        with ai_engine.connect() as conn:
            cti_res = conn.execute(text(cti_sql))
            tt_res = conn.execute(text(tt_sql))
            cti_rows = cti_res.fetchall()
            tt_rows = tt_res.fetchall()
            
        df_cti = pd.DataFrame(cti_rows, columns=["userid", "fault_type"]) if cti_rows else pd.DataFrame(columns=["userid", "fault_type"])
        df_tt = pd.DataFrame(tt_rows, columns=["userid", "fault_type"]) if tt_rows else pd.DataFrame(columns=["userid", "fault_type"])
        
        all_faults = pd.concat([df_cti, df_tt], ignore_index=True)
        
        if all_faults.empty:
             return pd.DataFrame([{"fault_type": "Unknown", "cnt": len(ids)}])
             
        # Map master faults to their generic service bucket
        all_faults["service"] = all_faults["fault_type"].apply(_map_fault_to_service)
        
        # Determine the users belonging to the selected service to calculate the correct Unknown gap
        # Similar logic as get_service_breakdown to align counts perfectly.
        user_primary_service = all_faults.groupby("userid")["service"].agg(lambda x: x.mode()[0] if not x.mode().empty else "Unknown").reset_index()
        target_service_users = user_primary_service[user_primary_service["service"] == service_filter]["userid"].tolist()
        expected_total_for_service = len(target_service_users)
        
        if service_filter:
            # Drop faults not matching the filtered service bucket
            all_faults = all_faults[all_faults["service"] == service_filter]
            
            # Restrict mapping only to users whose primary service is this one 
            # (to avoid counting secondary faults of users whose primary fault is different)
            all_faults = all_faults[all_faults["userid"].isin(target_service_users)]
            
        # Group by user to just take their top master fault for this category to ensure 1-to-1 customer matching
        if not all_faults.empty:
            df_dedup = all_faults.groupby("userid")["fault_type"].agg(lambda x: x.mode()[0] if not x.mode().empty else "Unknown").reset_index()
            df = df_dedup.groupby("fault_type", as_index=False)["userid"].count().rename(columns={"userid": "cnt"}).sort_values("cnt", ascending=False)
        else:
            df = pd.DataFrame(columns=["fault_type", "cnt"])
            
        found_cnt = df["cnt"].sum() if not df.empty else 0
        missing_cnt = expected_total_for_service - found_cnt
        if missing_cnt > 0:
            unknown_df = pd.DataFrame([{"fault_type": "Unknown", "cnt": missing_cnt}])
            df = pd.concat([df, unknown_df], ignore_index=True)
            
        return df.head(20) # Top 20 for pie chart sanity
    except Exception as e:
        print(f"[data_service] get_fault_types error: {e}")
        return pd.DataFrame(columns=["fault_type", "cnt"])


def get_sub_fault_types(category: str = "Very Poor",
                         master_fault: str = None,
                         date_from: str = None, date_to: str = None,
                         service_filter: str = None) -> pd.DataFrame:
    """Sub fault type counts for a given master fault, using both CTI and Tickets."""
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
    
    cti_sql = f"""
        SELECT userid, sub_fault_type 
        FROM ai.cti 
        WHERE userid IN ({in_clause}) 
          AND master_fault_type = :mft
          AND sub_fault_type IS NOT NULL AND sub_fault_type != 'nan'
    """
    tt_sql = f"""
        SELECT userid, sub_fault_types AS sub_fault_type 
        FROM ai.trouble_tickets 
        WHERE userid IN ({in_clause}) 
          AND fault_types = :mft
          AND sub_fault_types IS NOT NULL AND sub_fault_types != 'nan'
    """
    
    # We need to know how many users actually had this master_fault_type as their primary fault
    # to calculate the exact subset gap for the 'Unknown' bucket.
    # Because doing the full primary deduction again is expensive, we'll approximate the gap:
    # Gap = (Users with this master fault) - (Users with this master fault who also have a sub fault)
    base_sql_cti = f"SELECT userid FROM ai.cti WHERE userid IN ({in_clause}) AND master_fault_type = :mft"
    base_sql_tt = f"SELECT userid FROM ai.trouble_tickets WHERE userid IN ({in_clause}) AND fault_types = :mft"
    
    try:
        with ai_engine.connect() as conn:
            cti_res = conn.execute(text(cti_sql), {"mft": master_fault})
            tt_res = conn.execute(text(tt_sql), {"mft": master_fault})
            all_sub_faults = pd.concat([
                pd.DataFrame(cti_res.fetchall(), columns=["userid", "sub_fault_type"]) if cti_res.rowcount else pd.DataFrame(columns=["userid", "sub_fault_type"]),
                pd.DataFrame(tt_res.fetchall(), columns=["userid", "sub_fault_type"]) if tt_res.rowcount else pd.DataFrame(columns=["userid", "sub_fault_type"])
            ], ignore_index=True)
            
            # Fetch base users for gap calculation
            b_cti = conn.execute(text(base_sql_cti), {"mft": master_fault})
            b_tt = conn.execute(text(base_sql_tt), {"mft": master_fault})
            all_base_users = pd.concat([
                pd.DataFrame(b_cti.fetchall(), columns=["userid"]) if b_cti.rowcount else pd.DataFrame(columns=["userid"]),
                pd.DataFrame(b_tt.fetchall(), columns=["userid"]) if b_tt.rowcount else pd.DataFrame(columns=["userid"])
            ], ignore_index=True)

        expected_total = all_base_users["userid"].nunique() if not all_base_users.empty else 0
        
        if all_sub_faults.empty:
            return pd.DataFrame([{"sub_fault_type": "Unknown", "cnt": expected_total}]) if expected_total > 0 else pd.DataFrame(columns=["sub_fault_type", "cnt"])

        # De-duplicate by user to keep 1-to-1 customer counts
        df_dedup = all_sub_faults.groupby("userid")["sub_fault_type"].agg(lambda x: x.mode()[0] if not x.mode().empty else "Unknown").reset_index()
        df = df_dedup.groupby("sub_fault_type", as_index=False)["userid"].count().rename(columns={"userid": "cnt"}).sort_values("cnt", ascending=False)
        
        found_cnt = df["cnt"].sum()
        missing_cnt = expected_total - found_cnt
        if missing_cnt > 0:
            unknown_df = pd.DataFrame([{"sub_fault_type": "Unknown", "cnt": missing_cnt}])
            df = pd.concat([df, unknown_df], ignore_index=True)
            
        return df.head(20)
    except Exception as e:
        print(f"[data_service] get_sub_fault_types error: {e}")
        return pd.DataFrame(columns=["sub_fault_type", "cnt"])

def get_fault_details(category: str = "Very Poor",
                      service_filter: str = None,
                      master_fault: str = None,
                      sub_fault: str = None,
                      date_from: str = None, date_to: str = None) -> pd.DataFrame:
    """Returns the raw row records for users matching the exact 3-layer fault drilldown."""
    if not date_from or not date_to:
        date_from, date_to = _default_dates()
    
    empty_df = pd.DataFrame(columns=["Customer ID", "Service Group", "Master Fault", "Sub Fault"])
    
    # 1. Fetch valid users in this category timeframe
    id_sql = f"""
        SELECT DISTINCT userid FROM {LOCAL_DB_TABLE}
        WHERE csi_category = :cat
          AND run_date::date BETWEEN :d1 AND :d2
    """
    ids_df = query_df(local_engine, id_sql, {"cat": category, "d1": date_from, "d2": date_to})
    if ids_df.empty: return empty_df
    ids = [str(x) for x in ids_df["userid"].tolist()]
    if not ids: return empty_df
    
    # 2. Extract CTI/Ticket faults matching exactly
    from db import ai_engine
    from sqlalchemy import text
    in_clause = ", ".join(f"'{x}'" for x in ids)
    
    # Filter conditions
    mft_cti_cond = f" AND master_fault_type = :mft" if master_fault else ""
    mft_tt_cond = f" AND fault_types = :mft" if master_fault else ""
    
    sft_cti_cond = " AND (sub_fault_type = :sft OR sub_fault_type IS NULL)" if sub_fault == "Unknown" else (f" AND sub_fault_type = :sft" if sub_fault else "")
    sft_tt_cond = " AND (sub_fault_types = :sft OR sub_fault_types IS NULL)" if sub_fault == "Unknown" else (f" AND sub_fault_types = :sft" if sub_fault else "")
    
    cti_sql = f"""
        SELECT userid, master_fault_type, sub_fault_type
        FROM ai.cti 
        WHERE userid IN ({in_clause}){mft_cti_cond}{sft_cti_cond}
    """
    tt_sql = f"""
        SELECT userid, fault_types AS master_fault_type, sub_fault_types AS sub_fault_type
        FROM ai.trouble_tickets 
        WHERE userid IN ({in_clause}){mft_tt_cond}{sft_tt_cond}
    """
    
    try:
        with ai_engine.connect() as conn:
            params = {}
            if master_fault: params["mft"] = master_fault
            if sub_fault and sub_fault != "Unknown": params["sft"] = sub_fault
            
            cti_res = conn.execute(text(cti_sql), params)
            tt_res = conn.execute(text(tt_sql), params)
            
            df_cti = pd.DataFrame(cti_res.fetchall(), columns=["Customer ID", "Master Fault", "Sub Fault"]) if cti_res.rowcount else empty_df[["Customer ID", "Master Fault", "Sub Fault"]]
            df_tt = pd.DataFrame(tt_res.fetchall(), columns=["Customer ID", "Master Fault", "Sub Fault"]) if tt_res.rowcount else empty_df[["Customer ID", "Master Fault", "Sub Fault"]]
            
            all_faults = pd.concat([df_cti, df_tt], ignore_index=True)
            if all_faults.empty: return empty_df
            
            # Reconstruct the Service Filter mapping to ensure we don't grab faults belonging to a different service
            all_faults["Service Group"] = all_faults["Master Fault"].apply(_map_fault_to_service)
            if service_filter:
                all_faults = all_faults[all_faults["Service Group"] == service_filter]
                
            # De-duplicate by User ID
            return all_faults.drop_duplicates(subset=["Customer ID"]).reset_index(drop=True)
            
    except Exception as e:
        print(f"[data_service] get_fault_details error: {e}")
        return empty_df

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
        expected_total = len(selected_ids)
        if expected_total == 0:
            return pd.DataFrame(columns=["label", "selected_cnt", "total_cnt", "pct"])
            
        in_clause = ", ".join(f"'{x}'" for x in selected_ids)
        
        if city is None:
            # City level
            with dwh_engine.connect() as conn:
                r_sel = conn.execute(
                    text(f"SELECT city, COUNT(*) AS selected_cnt "
                         f"FROM dwh.customers WHERE customer_id IN ({in_clause}) "
                         f"AND city IS NOT NULL GROUP BY city ORDER BY selected_cnt DESC")
                )
                sel_df = pd.DataFrame(r_sel.fetchall(), columns=list(r_sel.keys()))
                r_tot = conn.execute(
                    text("SELECT city, COUNT(*) AS total_cnt FROM dwh.customers "
                         "WHERE status='ACTIVE' AND city IS NOT NULL GROUP BY city")
                )
                tot_df = pd.DataFrame(r_tot.fetchall(), columns=list(r_tot.keys()))
                
            df = sel_df.merge(tot_df, on="city", how="left").fillna(0)
            df = df.rename(columns={"city": "label"})
            
            # Check for Unknowns
            found_cnt = df["selected_cnt"].sum() if not df.empty else 0
            if found_cnt < expected_total:
                df = pd.concat([df, pd.DataFrame([{
                    "label": "Unknown", "selected_cnt": expected_total - found_cnt, 
                    "total_cnt": expected_total - found_cnt, "pct": 0
                }])], ignore_index=True)
                
            df["pct"] = (df["selected_cnt"] / df["total_cnt"].replace(0, 1) * 100).round(1)
            return df.sort_values("selected_cnt", ascending=False)

        elif area is None:
            # Area level within city
            with dwh_engine.connect() as conn:
                # Find exactly how many selected users exist in this specific city to calculate a localized 'Unknown' gap
                r_base = conn.execute(text(f"SELECT COUNT(*) AS c FROM dwh.customers WHERE customer_id IN ({in_clause}) AND city = :city"), {"city": city})
                local_expected = r_base.scalar() or 0
                
                r_sel = conn.execute(
                    text(f"SELECT sector AS area, COUNT(*) AS selected_cnt "
                         f"FROM dwh.customers WHERE customer_id IN ({in_clause}) "
                         f"AND city = :city AND sector IS NOT NULL "
                         f"GROUP BY sector ORDER BY selected_cnt DESC"),
                    {"city": city}
                )
                sel_df = pd.DataFrame(r_sel.fetchall(), columns=list(r_sel.keys()))
                r_tot = conn.execute(
                    text("SELECT sector AS area, COUNT(*) AS total_cnt "
                         "FROM dwh.customers WHERE status='ACTIVE' AND city = :city "
                         "AND sector IS NOT NULL GROUP BY sector"),
                    {"city": city}
                )
                tot_df = pd.DataFrame(r_tot.fetchall(), columns=list(r_tot.keys()))
                
            df = sel_df.merge(tot_df, on="area", how="left").fillna(0)
            df = df.rename(columns={"area": "label"})
            
            # Check for Unknowns at Area level
            found_cnt = df["selected_cnt"].sum() if not df.empty else 0
            if found_cnt < local_expected:
                df = pd.concat([df, pd.DataFrame([{
                    "label": "Unknown", "selected_cnt": local_expected - found_cnt, 
                    "total_cnt": local_expected - found_cnt, "pct": 0
                }])], ignore_index=True)
                
            df["pct"] = (df["selected_cnt"] / df["total_cnt"].replace(0, 1) * 100).round(1)
            return df.sort_values("selected_cnt", ascending=False)

        else:
            # Sub-area level
            try:
                with dwh_engine.connect() as conn:
                    # Find exactly how many selected users exist in this specific city+area to calculate a localized 'Unknown' gap
                    r_base = conn.execute(
                        text(f"SELECT COUNT(*) AS c FROM dwh.customers WHERE customer_id IN ({in_clause}) AND city = :city AND sector = :area"), 
                        {"city": city, "area": area}
                    )
                    local_expected = r_base.scalar() or 0
                    
                    r_sel = conn.execute(
                        text(f"SELECT subsector AS sublabel, COUNT(*) AS selected_cnt "
                             f"FROM dwh.customers WHERE customer_id IN ({in_clause}) "
                             f"AND city = :city AND sector = :area AND subsector IS NOT NULL "
                             f"GROUP BY subsector ORDER BY selected_cnt DESC LIMIT 20"),
                        {"city": city, "area": area}
                    )
                    sel_df = pd.DataFrame(r_sel.fetchall(), columns=list(r_sel.keys()))
                    r_tot = conn.execute(
                        text("SELECT subsector AS sublabel, COUNT(*) AS total_cnt "
                             "FROM dwh.customers WHERE status='ACTIVE' AND city = :city "
                             "AND sector = :area AND subsector IS NOT NULL GROUP BY subsector"),
                        {"city": city, "area": area}
                    )
                    tot_df = pd.DataFrame(r_tot.fetchall(), columns=list(r_tot.keys()))
                    
                df = sel_df.merge(tot_df, on="sublabel", how="left").fillna(0)
                df = df.rename(columns={"sublabel": "label"})
                
                # Check for Unknowns at Sub-Area level
                found_cnt = df["selected_cnt"].sum() if not df.empty else 0
                if found_cnt < local_expected:
                    df = pd.concat([df, pd.DataFrame([{
                        "label": "Unknown", "selected_cnt": local_expected - found_cnt, 
                        "total_cnt": local_expected - found_cnt, "pct": 0
                    }])], ignore_index=True)
                    
                df["pct"] = (df["selected_cnt"] / df["total_cnt"].replace(0, 1) * 100).round(1)
                return df.sort_values("selected_cnt", ascending=False)
                
            except Exception as e:
                print(f"[data_service] get_city_breakdown sub-area error: {e}")
                return pd.DataFrame(columns=["label", "selected_cnt", "total_cnt", "pct"])
                
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
