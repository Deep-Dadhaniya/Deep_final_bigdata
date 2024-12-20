from pymongo import MongoClient
import pandas as pd

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["fraud_detection"]
silver_collection = db["silver_layer"]
gold_total_avg_by_merchant = db["gold_total_avg_by_merchant"]
gold_transactions_by_state = db["gold_transactions_by_state"]
gold_daily_totals = db["gold_daily_totals"]
gold_high_value_transactions = db["gold_high_value_transactions"]
gold_top_merchants = db["gold_top_merchants"]
gold_client_spending_patterns = db["gold_client_spending_patterns"]


# Helper function to run aggregation and handle empty results
def run_aggregation(pipeline, target_collection, collection_name):
    # Clear the target collection to avoid duplicate key errors
    target_collection.delete_many({})

    # Run the aggregation pipeline
    results = list(silver_collection.aggregate(pipeline))
    if results:
        target_collection.insert_many(results)
        print(f"Inserted {len(results)} records into '{collection_name}'.")
    else:
        print(f"No records found for '{collection_name}'.")


# 1. Total and Average Transaction Amounts by Merchant
pipeline_total_avg_by_merchant = [
    {
        "$group": {
            "_id": "$merchant_id",
            "total_amount": {"$sum": "$amount"},
            "avg_amount": {"$avg": "$amount"},
            "transaction_count": {"$sum": 1}
        }
    },
    # Rename `_id` to `merchant_id` and remove `_id`
    {"$project": {"merchant_id": "$_id", "_id": 0, "total_amount": 1, "avg_amount": 1, "transaction_count": 1}},
    {"$sort": {"total_amount": -1}}
]
run_aggregation(pipeline_total_avg_by_merchant, gold_total_avg_by_merchant, "gold_total_avg_by_merchant")

# 2. Transactions by State
pipeline_transactions_by_state = [
    {
        "$group": {
            "_id": "$merchant_state",
            "total_amount": {"$sum": "$amount"},
            "transaction_count": {"$sum": 1}
        }
    },
    # Rename `_id` to `state` and remove `_id`
    {"$project": {"state": "$_id", "_id": 0, "total_amount": 1, "transaction_count": 1}},
    {"$sort": {"transaction_count": -1}}
]
run_aggregation(pipeline_transactions_by_state, gold_transactions_by_state, "gold_transactions_by_state")

# 3. Daily Total Transactions
pipeline_daily_totals = [
    {
        "$group": {
            "_id": {"date": {"$substr": ["$date", 0, 10]}},
            "total_daily_amount": {"$sum": "$amount"},
            "transaction_count": {"$sum": 1}
        }
    },
    # Flatten `_id` field and remove `_id`
    {"$project": {"date": "$_id.date", "_id": 0, "total_daily_amount": 1, "transaction_count": 1}},
    {"$sort": {"date": 1}}
]
run_aggregation(pipeline_daily_totals, gold_daily_totals, "gold_daily_totals")

# 4. High-Value Transactions
pipeline_high_value_transactions = [
    {"$match": {"amount": {"$gte": 1000}}},
    {"$project": {"_id": 0, "merchant_id": 1, "client_id": 1, "amount": 1, "date": 1, "merchant_state": 1}}
]
run_aggregation(pipeline_high_value_transactions, gold_high_value_transactions, "gold_high_value_transactions")

# 5. Top 10 Merchants by Transaction Count
pipeline_top_merchants = [
    {
        "$group": {
            "_id": "$merchant_id",
            "transaction_count": {"$sum": 1}
        }
    },
    {"$sort": {"transaction_count": -1}},
    {"$limit": 10},
    # Rename `_id` to `merchant_id` and remove `_id`
    {"$project": {"merchant_id": "$_id", "_id": 0, "transaction_count": 1}}
]
run_aggregation(pipeline_top_merchants, gold_top_merchants, "gold_top_merchants")

# 6. Client Spending Patterns
pipeline_client_spending_patterns = [
    {
        "$group": {
            "_id": "$client_id",
            "total_spent": {"$sum": "$amount"},
            "avg_spent": {"$avg": "$amount"},
            "transaction_count": {"$sum": 1}
        }
    },
    # Rename `_id` to `client_id` and remove `_id`
    {"$project": {"client_id": "$_id", "_id": 0, "total_spent": 1, "avg_spent": 1, "transaction_count": 1}},
    {"$sort": {"total_spent": -1}}
]
run_aggregation(pipeline_client_spending_patterns, gold_client_spending_patterns, "gold_client_spending_patterns")


# Export each Gold Layer collection to CSV
def export_collection_to_csv(collection, filename):
    data = list(collection.find())
    if data:
        df = pd.DataFrame(data)
        df.to_csv(filename, index=False)
        print(f"Exported {len(data)} records to '{filename}'.")
    else:
        print(f"No records to export for '{filename}'.")


# Export all Gold Layer collections
export_collection_to_csv(gold_total_avg_by_merchant, "gold_total_avg_by_merchant.csv")
export_collection_to_csv(gold_transactions_by_state, "gold_transactions_by_state.csv")
export_collection_to_csv(gold_daily_totals, "gold_daily_totals.csv")
export_collection_to_csv(gold_high_value_transactions, "gold_high_value_transactions.csv")
export_collection_to_csv(gold_top_merchants, "gold_top_merchants.csv")
export_collection_to_csv(gold_client_spending_patterns, "gold_client_spending_patterns.csv")

print("Gold Layer datasets created and exported.")
