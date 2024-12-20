from pymongo import MongoClient
import pandas as pd
import logging

# Setup logging
logging.basicConfig(
    filename="cleaning_process.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["fraud_detection"]
bronze_collection = db["bronze_layer"]
silver_collection = db["silver_layer"]

try:
    # Fetch Bronze Layer Data
    bronze_data = list(bronze_collection.find())
    print("Fetched Data:", bronze_data[:5])
    logging.info(f"Fetched {len(bronze_data)} records from Bronze Layer.")
    print(f"Fetched {len(bronze_data)} records from Bronze Layer.")

    if not bronze_data:
        raise ValueError("No records found in the Bronze Layer collection.")

    # Convert Bronze Layer Data to DataFrame
    bronze_df = pd.DataFrame(bronze_data)

    # Step 1: Remove records with missing critical fields
    required_fields = ["amount", "merchant_state", "zip"]
    if not all(field in bronze_df.columns for field in required_fields):
        raise ValueError(f"Missing required fields: {required_fields}")

    cleaned_df = bronze_df.dropna(subset=required_fields).copy()
    logging.info(f"Records after removing missing critical fields: {len(cleaned_df)}")
    print(f"Records after removing missing critical fields: {len(cleaned_df)}")

    # Step 2: Clean 'amount' field
    cleaned_df["amount"] = (
        cleaned_df["amount"]
        .str.replace("[\$,]", "", regex=True)  # Remove $ and commas
        .astype(float)
    )
    cleaned_df = cleaned_df[cleaned_df["amount"] > 0]  # Remove negative or zero amounts
    logging.info(f"Records after cleaning 'amount': {len(cleaned_df)}")
    print(f"Records after cleaning 'amount': {len(cleaned_df)}")

    # Step 3: Standardize 'date' field to ISO format
    cleaned_df["date"] = pd.to_datetime(cleaned_df["date"]).dt.strftime("%Y-%m-%dT%H:%M:%S")
    logging.info("Standardized 'date' field to ISO format.")

    # Step 4: Fill missing 'errors' field with 'No Errors'
    cleaned_df["errors"] = cleaned_df["errors"].fillna("No Errors")
    logging.info("Filled missing 'errors' field with 'No Errors'.")

    # Step 5: Handle 'zip' field: Convert to integer or fill with a default value
    cleaned_df["zip"] = cleaned_df["zip"].fillna(0).astype(int)
    logging.info("Cleaned 'zip' field by filling missing values.")

    # Insert Cleaned Data into the Silver Layer
    silver_collection.delete_many({})  # Optional: Clear existing Silver Layer data
    silver_collection.insert_many(cleaned_df.to_dict("records"))
    logging.info(f"Inserted {len(cleaned_df)} records into the Silver Layer.")
    print(f"Inserted {len(cleaned_df)} records into the Silver Layer.")

    # Sample Output
    print("Sample records from Silver Layer:")
    for record in silver_collection.find().limit(5):
        print(record)

except Exception as e:
    logging.error(f"An error occurred: {e}")
    print(f"An error occurred: {e}")
