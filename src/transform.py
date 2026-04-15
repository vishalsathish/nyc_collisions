
def transform_data():

    import pandas as pd
    import os

    df = pd.read_parquet("data/bronze/")

    df.columns = (
        df.columns
        .str.lower()
        .str.replace(" ", "_")
    )

    # rename for ease
    df = df.rename(columns={
        "accident_date": "crash_date",
        "accident_time": "crash_time"
    })

    # ensuring date data is in right format
    df["crash_date"] = pd.to_datetime(df["crash_date"], errors="coerce")
    df["crash_time"] = pd.to_datetime(df["crash_time"], format="%H:%M", errors="coerce").dt.time

    # creating time columns
    df["year"] = df["crash_date"].dt.year.astype("Int64")
    df["month"] = df["crash_date"].dt.month.astype("Int64")
    df["day"] = df["crash_date"].dt.day
    df["crash_time"] = pd.to_datetime(df["crash_time"], format="%H:%M", errors="coerce")
    df["hour"] = df["crash_time"].dt.hour

    # ensuring numerical columns are numeric
    # instances with nulls for these columns are fillna with 0
    numeric_cols = [
        "number_of_persons_injured",
        "number_of_persons_killed",
        "number_of_pedestrians_injured",
        "number_of_pedestrians_killed",
        "number_of_cyclist_injured",
        "number_of_cyclist_killed",
        "number_of_motorist_injured",
        "number_of_motorist_killed"
    ]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)


    # missing borough
    df["borough"] = df["borough"].fillna("UNKNOWN")

    # ensure latitude and longitude data are numeric
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")

    # data with missing latitude and longitude are corrupted signficantly, thus drop
    df = df.dropna(subset=["latitude", "longitude"])
    # drop with missing id and date
    df = df.dropna(subset=["collision_id", "crash_date"])

    # rename for simplicity
    df["total_injured"] = df["number_of_persons_injured"]
    df["total_killed"] = df["number_of_persons_killed"]

    # consistent street naming convention
    text_cols = [
        "borough",
        "on_street_name",
        "cross_street_name",
        "off_street_name"
    ]

    for col in text_cols:
        df[col] = df[col].astype(str).str.upper().str.strip()

    # dropping duplicate id
    df = df.drop_duplicates(subset=["collision_id"])

    os.makedirs("data/silver", exist_ok=True)
    df.to_parquet(
        "data/silver/",
        partition_cols=["year", "month"]
    )

    print(f"Silver layer created with {len(df)} rows")