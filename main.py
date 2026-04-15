from src.extract import extract_data
from src.transform import transform_data

def run_pipeline():
    print("Starting Bronze ingestion......")
    total_rows = extract_data()
    print(f"Pipeline completed. Total rows ingested: {total_rows}")

    print("Starting Silver transformation........")
    transform_data()
    print("Transformation successfully completed")

if __name__ == "__main__":
    run_pipeline()