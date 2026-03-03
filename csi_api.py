import uvicorn
from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

# ================================
# CONFIGURATION
# ================================
DB_CONFIG = {
    'user': 'csi_admin',
    'password': 'cGD*A8hd*jhJ!PuC',  # <--- UPDATE THIS
    'host': 'localhost',
    'port': '5432',
    'dbname': 'csi_db',
    'table_name': 'csi_scores'
}

# Construct DB URL
DATABASE_URL = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"

# Initialize Database Engine
engine = create_engine(DATABASE_URL, pool_pre_ping=True)

# Initialize FastAPI App
app = FastAPI(
    title="Nayatel CSI API",
    description="API to retrieve Customer Service Index scores and categories.",
    version="1.0.0"
)

# ================================
# DATA MODELS (Response Schema)
# ================================
class CSIResponse(BaseModel):
    user_id: str
    csi_score: float
    csi_category: str
    last_updated: str

# ================================
# API ENDPOINTS
# ================================

@app.get("/")
def health_check():
    """Simple health check to ensure API is running."""
    return {"status": "online", "service": "CSI API"}

@app.get("/csi/{user_id}", response_model=CSIResponse)
def get_customer_csi(user_id: str):
    """
    Fetch the CSI Score and Category for a specific UserID.
    """
    query = text(f"""
        SELECT 
            userid, 
            predicted_csi, 
            csi_category, 
            run_date 
        FROM {DB_CONFIG['table_name']} 
        WHERE userid = :uid
        LIMIT 1
    """)

    try:
        with engine.connect() as conn:
            # Execute query safely using parameters (prevents SQL Injection)
            result = conn.execute(query, {"uid": user_id}).fetchone()

            if not result:
                raise HTTPException(status_code=404, detail=f"User ID '{user_id}' not found in CSI records.")

            # Map result to the Pydantic model
            # Note: accessing result by index or column name depends on driver, 
            # ._mapping is the safest SQLAlchemy 1.4+ way
            row = result._mapping
            
            return CSIResponse(
                user_id=str(row['userid']),
                csi_score=round(float(row['predicted_csi']), 2),
                csi_category=row['csi_category'],
                last_updated=str(row['run_date'])
            )

    except OperationalError:
        raise HTTPException(status_code=500, detail="Database connection failed.")
    except Exception as e:
        print(f"Error: {e}") # Log to console
        raise HTTPException(status_code=500, detail="Internal Server Error")

if __name__ == "__main__":
    # Run the server on port 8000
    uvicorn.run("csi_api:app", host="0.0.0.0", port=8000, reload=True)