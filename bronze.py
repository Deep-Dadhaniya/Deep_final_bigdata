from pymongo import MongoClient
import pandas as pd

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["fraud_detection"]
bronze_collection = db["bronze_layer"]

# Fetch data from Bronze Layer
bronze_data = list(bronze_collection.find())

# Check if data exists
if bronze_data:
    # Convert to a Pandas DataFrame for analysis
    bronze_df = pd.DataFrame(bronze_data)

    # Display the first 5 rows of the data
    print("Bronze Layer Data (First 5 Rows):")
    print(bronze_df.head())

    # Show the count of rows and columns
    rows, columns = bronze_df.shape
    print(f"\nBronze Layer has {rows} rows and {columns} columns.")
else:
    print("No data found in the Bronze Layer collection.")
