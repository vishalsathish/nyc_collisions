from sqlalchemy import create_engine
import os

password = os.getenv("DB_PASSWORD")

engine = create_engine(f"postgresql://postgres:{password}@localhost:5432/nyc_collisions")

conn = engine.connect()
print("Connected successfully!")
conn.close()