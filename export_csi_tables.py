import os
import psycopg2

# ==========================
# CONFIGURATION
# ==========================
DB_CONFIG = {
    "host": "dwhprimary.nayatel.com",
    "port": 5432,
    "dbname": "ai",                 # database name from your screenshot
    "user": "tacusama",         # <-- change this
    "password": "FGJrxURRHsESGWCXuDeS", # <-- change this
}

# Directory where CSV files will be stored
OUTPUT_DIR = "./data"  # e.g. "/var/data/ai_exports"


# ==========================
# EXPORT FUNCTION (uses COPY)
# ==========================
def export_query_to_csv(table_name: str, query: str):
    """
    Runs a SQL query and exports the result to OUTPUT_DIR/{table_name}.csv
    using PostgreSQL COPY for efficiency.
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(OUTPUT_DIR, f"{table_name}.csv")

    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)

        # autocommit is recommended for COPY
        conn.autocommit = True

        with conn.cursor() as cur, open(output_path, "w", encoding="utf-8", newline="") as f:
            copy_sql = f"COPY ({query}) TO STDOUT WITH CSV HEADER"
            cur.copy_expert(copy_sql, f)

        print(f"[OK] Exported {table_name} -> {output_path}")
    except Exception as e:
        print(f"[ERROR] Exporting {table_name}: {e}")
    finally:
        if conn is not None:
            conn.close()


# ==========================
# MAIN
# ==========================
def main():
    # Map: table_name -> query
    table_queries = {
        # filtered activity
        "activity": """
            SELECT *
            FROM ai.activity
            WHERE status IN ('COMPLETED', 'PENDING', 'SUBMITTED')
        """,

        # full tables
        "cti": """
            SELECT *
            FROM ai.cti
        """,
        "outages": """
            SELECT *
            FROM ai.outages
        """,
        "trouble_tickets": """
            SELECT *
            FROM ai.trouble_tickets
        """,
    }

    for table_name, query in table_queries.items():
        export_query_to_csv(table_name, query)


if __name__ == "__main__":
    main()