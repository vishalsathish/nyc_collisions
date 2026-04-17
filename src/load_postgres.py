import pandas as pd
from sqlalchemy import create_engine
import os

def load_to_postgres():

    # connection string (update password)
    password = os.getenv("DB_PASSWORD")
    engine = create_engine(f"postgresql://postgres:{password}@localhost:5432/nyc_collisions")

    # read gold tables
    crashes_by_borough = pd.read_parquet("data/gold/crashes_by_borough.parquet")
    monthly_trends = pd.read_parquet("data/gold/monthly_trends.parquet")
    crash_by_hour = pd.read_parquet("data/gold/crash_by_hour.parquet")
    top_factors = pd.read_parquet("data/gold/top_factors.parquet")

    # load into postgres
    crashes_by_borough.to_sql("crashes_by_borough", engine, if_exists="replace", index=False)
    monthly_trends.to_sql("monthly_trends", engine, if_exists="replace", index=False)
    crash_by_hour.to_sql("crash_by_hour", engine, if_exists="replace", index=False)
    top_factors.to_sql("top_factors", engine, if_exists="replace", index=False)

    print("Gold data loaded into PostgreSQL")