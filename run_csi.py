import pandas as pd
import os
import time
from datetime import datetime
from sqlalchemy import create_engine, text, inspect
import warnings

# Import existing logic from your utility file
from csi_utils import (
    CPTOptimizedCSIConfig,
    load_data_optimized,
    preprocess_data_optimized,
    create_features_batched
)

# --- DATABASE CONFIGURATION ---
# Configured for the user and DB you just created
DB_CONFIG = {
    'user': 'csi_admin',
    'password': 'cGD*A8hd*jhJ!PuC',  # <--- REPLACE THIS WITH THE PASSWORD YOU SET
    'host': 'localhost',
    'port': '5432',
    'dbname': 'csi_db',
    'table_name': 'csi_scores'
}

class DatabaseManager:
    def __init__(self, config):
        self.table_name = config['table_name']
        # Construct connection string for PostgreSQL
        # format: postgresql://user:password@host:port/dbname
        self.db_url = f"postgresql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['dbname']}"
        self.engine = create_engine(self.db_url)

    def rotate_table(self):
        """
        Renames the existing 'csi_scores' table to 'csi_scores_YYYYMMDD'.
        This preserves history before we write the new daily batch.
        """
        inspector = inspect(self.engine)
        
        # Check if the main table exists
        if self.table_name in inspector.get_table_names():
            # Create archive name based on today's date
            archive_suffix = datetime.now().strftime("%Y%m%d")
            archive_table_name = f"{self.table_name}_{archive_suffix}"
            
            # Handle edge case: if archive already exists (e.g. script ran twice today), append time
            if archive_table_name in inspector.get_table_names():
                archive_table_name = f"{self.table_name}_{datetime.now().strftime('%Y%m%d_%H%M')}"

            print(f"🔄 Archiving current table '{self.table_name}' to '{archive_table_name}'...")
            
            try:
                with self.engine.connect() as conn:
                    conn.execute(text("COMMIT")) # Ensure no open transaction blocks DDL
                    # PostgreSQL syntax for renaming
                    conn.execute(text(f'ALTER TABLE "{self.table_name}" RENAME TO "{archive_table_name}"'))
                print(f"✅ Table archived successfully.")
            except Exception as e:
                print(f"⚠️  Warning: Could not archive table. Error: {e}")
        else:
            print(f"ℹ️  No existing table '{self.table_name}' found. Skipping archive step.")

    def save_to_db(self, df):
        """
        Saves the dataframe to the database.
        """
        print(f"💾 Saving {len(df):,} records to DB table '{self.table_name}'...")
        try:
            # if_exists='replace' is safe here because we just renamed the old table 
            # in rotate_table(), so we are effectively creating a fresh table.
            df.to_sql(
                self.table_name, 
                self.engine, 
                if_exists='replace', 
                index=False,
                chunksize=5000 
            )
            print("✅ Data saved successfully.")
        except Exception as e:
            print(f"❌ Error saving to database: {e}")
            raise e

def run_calculator():
    print("🧮 Starting Customer Service Quality Calculator")
    print("=" * 60)
    
    start_total = time.time()
    
    # 1. Initialize Configuration
    config = CPTOptimizedCSIConfig()
    
    print(f"📂 Configuration loaded.")
    print(f"   Batch size: {config.processing_batch_size}")

    # 2. Load Data
    print(f"\n=== LOADING DATA ===")
    data_dict = load_data_optimized(config)
    
    if all(len(df) == 0 for df in data_dict.values()):
        print("❌ CRITICAL ERROR: No data loaded.")
        return

    # 3. Preprocess Data
    print(f"\n=== PREPROCESSING DATA ===")
    data_dict = preprocess_data_optimized(data_dict, config)

    # 4. Calculate Scores
    print(f"\n=== CALCULATING CSI SCORES ===")
    results_df = create_features_batched(data_dict, config)
    
    if results_df.empty:
        print("❌ No results generated.")
        return

    # 5. Format the Report (Exact same columns as before, minus the ML model column)
    print(f"\n=== GENERATING REPORT ===")
    
    # Define the exact column order
    column_order = [
        'userid', 'total_tickets', 'total_outages', 'total_activities', 'total_calls',
        'total_interactions', 'distress_duration', 'complaint_ratio',
        'customer_problem_indicator', 'customer_ticket_duration', 
        'outage_events', 'outage_duration', 'activity_count', 
        'activity_completion', 'activity_average', 
        'total_call_duration', 'avg_call_duration'
    ]
    
    # Add the recency window columns dynamically
    for d in config.recency_windows_days:
        column_order.extend([
            f'total_tickets_last_{d}d', f'total_outages_last_{d}d', f'total_calls_last_{d}d',
            f'total_activities_last_{d}d', f'distress_duration_last_{d}d', f'complaint_ratio_last_{d}d'
        ])
    
    # Add the Calculated Score and Category
    column_order.extend(['predicted_csi', 'csi_category'])

    # Filter to ensure we only select columns that actually exist in the dataframe
    available_columns = [col for col in column_order if col in results_df.columns]
    final_report = results_df[available_columns]

    # Add a timestamp column so we know when this run happened
    final_report['run_date'] = datetime.now()

    # 6. Database Operations
    print(f"\n=== DATABASE OPERATIONS ===")
    db_manager = DatabaseManager(DB_CONFIG)
    
    # Step A: Archive the old table (e.g. csi_scores -> csi_scores_20231027)
    db_manager.rotate_table()
    
    # Step B: Write the new table (creates new csi_scores)
    db_manager.save_to_db(final_report)
    
    # 7. Print Summary
    print(f"\n📊 CSI QUALITY SUMMARY:")
    if 'csi_category' in final_report.columns:
        distribution = final_report['csi_category'].value_counts()
        total_customers = len(final_report)
        for category, count in distribution.sort_index().items():
            percentage = (count / total_customers) * 100
            print(f"   {category:<10}: {count:,} customers ({percentage:.1f}%)")

    print(f"\n⏱️  Total calculation time: {time.time() - start_total:.2f} seconds")
    print("=" * 60)

if __name__ == "__main__":
    import multiprocessing
    multiprocessing.set_start_method("spawn", force=True)
    run_calculator()