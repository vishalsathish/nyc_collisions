from datetime import datetime
import requests
import pandas as pd
import os

API_URL = "https://data.cityofnewyork.us/resource/h9gi-nx95.json"

def extract_data():
    limit = 1000
    offset = 0

    headers = {
        "User-Agent": "nyc-collisions-pipeline",
        "Accept": "application/json"
    }

    # Ensure folder exists
    os.makedirs("data/bronze", exist_ok=True)

    total_rows = 0

    while True:
        params = {
            "$limit": limit,
            "$offset": offset,
            "$where": "crash_date > '2026-04-01T00:00:00'"
        }

        response = requests.get(API_URL, params=params, headers=headers, timeout=10)
        response.raise_for_status()

        batch = response.json()

        if not batch:
            print("No more data. Finished pagination.")
            break

        save_batch(batch, offset)

        batch_size = len(batch)
        total_rows += batch_size

        print(f"Fetched batch: {batch_size} rows | Total so far: {total_rows}")

        offset += limit

    print(f"Final row count: {total_rows}")

    return total_rows  # return metadata instead of full dataset


def save_batch(batch, offset):
    df = pd.DataFrame(batch)

    today = datetime.now()
    year = today.strftime("%Y")
    month = today.strftime("%m")
    day = today.strftime("%d")

    path = f"data/bronze/year={year}/month={month}/day={day}"
    os.makedirs(path, exist_ok=True)

    file_path = f"{path}/batch_{offset}.parquet"
    df.to_parquet(file_path, index=False)