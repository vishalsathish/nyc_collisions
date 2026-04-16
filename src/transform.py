def transform_data():

    import pandas as pd
    import os

    df = pd.read_parquet("data/bronze/")

    # standardize column names
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

    # ensuring date/time data is in right format
    df["crash_date"] = pd.to_datetime(df["crash_date"], errors="coerce")
    df["crash_time"] = pd.to_datetime(df["crash_time"], format="%H:%M", errors="coerce")

    # drop rows with missing id and date (invalid records)
    df = df.dropna(subset=["collision_id", "crash_date"])

    # creating time columns for analysis
    df["year"] = df["crash_date"].dt.year.astype(int)
    df["month"] = df["crash_date"].dt.month.astype(int)
    df["day"] = df["crash_date"].dt.day
    df["hour"] = df["crash_time"].dt.hour

    # ensuring numerical columns are numeric
    # instances with nulls for these columns are filled with 0
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

    # missing borough values filled with UNKNOWN
    df["borough"] = df["borough"].fillna("UNKNOWN")

    # ensure latitude and longitude data are numeric
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")

    # data with missing latitude and longitude are significantly corrupted, thus drop
    df = df.dropna(subset=["latitude", "longitude"])

    # derived metrics for easier aggregation
    df["total_injured"] = df["number_of_persons_injured"]
    df["total_killed"] = df["number_of_persons_killed"]

    # consistent text formatting for categorical/location fields
    text_cols = [
        "borough",
        "on_street_name",
        "cross_street_name",
        "off_street_name"
    ]

    for col in text_cols:
        df[col] = df[col].fillna("UNKNOWN").str.upper().str.strip()

    # dropping duplicate collision records
    df = df.drop_duplicates(subset=["collision_id"])

    # save cleaned data to silver layer with partitioning
    os.makedirs("data/silver", exist_ok=True)

    df.to_parquet(
        "data/silver/",
        partition_cols=["year", "month"],
        engine="pyarrow",
        use_dictionary=False
    )

    print(f"Silver layer created with {len(df)} rows")