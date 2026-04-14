from src.extract import extract_data

def run_pipeline():
    total_rows = extract_data()
    print(f"Pipeline completed. Total rows ingested: {total_rows}")

if __name__ == "__main__":
    run_pipeline()