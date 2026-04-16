def gold_tables():

    import pandas as pd
    import os
    import pyarrow.parquet as pq

    table = pq.read_table("data/silver/")
    df = table.to_pandas()
    os.makedirs("data/gold/", exist_ok=True)

    # crashes by borough
    crashes_by_borough = df.groupby("borough", observed=True).agg(
        total_crashes=("collision_id", "count"),
        total_injuries=("total_injured", "sum"),
        total_deaths=("total_killed", "sum")
    ).reset_index()

    # monthly table
    monthly_trends = df.groupby(["year", "month"], observed=True).agg(
        total_crashes=("collision_id", "count"),
        total_injuries=("total_injured", "sum"),
        total_deaths=("total_killed", "sum")
    ).reset_index()

    # peak crash hours
    crash_by_hour = df.groupby("hour").agg(
        total_crashes=("collision_id", "count")
    ).reset_index().sort_values("total_crashes", ascending=False)

    # contributing factors (2 factors mostly)
    factor_cols = [
        "contributing_factor_vehicle_1",
        "contributing_factor_vehicle_2"
    ]

    factors = pd.concat([df[col] for col in factor_cols])

    top_factors = factors.value_counts().reset_index()
    top_factors.columns = ["factor", "count"]

    # saving tables
    crashes_by_borough.to_parquet("data/gold/crashes_by_borough.parquet", index=False)

    monthly_trends.to_parquet("data/gold/monthly_trends.parquet", index=False)

    crash_by_hour.to_parquet("data/gold/crash_by_hour.parquet", index=False)

    top_factors.to_parquet("data/gold/top_factors.parquet", index=False)

    print("Gold layer tables produced ............")