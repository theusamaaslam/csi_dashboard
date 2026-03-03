import traceback
import db

try:
    print("Testing local db...")
    db.local_engine.connect()
    print("Local OK")
except Exception as e:
    print("Local Error:")
    print(e)

try:
    print("\nTesting ai db...")
    db.ai_engine.connect()
    print("AI OK")
except Exception as e:
    print("AI Error:")
    print(e)
