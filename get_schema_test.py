import pandas as pd
from db import dwh_engine

try:
    df = pd.read_sql("SELECT column_name FROM information_schema.columns WHERE table_schema='dwh' AND table_name='customers'", dwh_engine)
    with open('schema_output.txt', 'w') as f:
        f.write("\n".join(df['column_name'].tolist()))
except Exception as e:
    with open('schema_output.txt', 'w') as f:
        f.write(f"Error: {e}")
