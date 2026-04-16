from src.extract import extract_data
from src.transform import transform_data
from src.load import gold_tables

def run_pipeline():
    print("Starting Bronze ingestion......")
    total_rows = extract_data()
    print(f"Pipeline completed. Total rows ingested: {total_rows}")

    print("Starting Silver transformation........")
    transform_data()
    print("Transformation successfully completed")

    print("Starting Gold tables creation........")
    gold_tables()
    print("Tables successfully created...........")

if __name__ == "__main__":
    run_pipeline()