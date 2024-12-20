from kafka import KafkaConsumer
from pymongo import MongoClient
import json

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['fraud_detection']
bronze_collection = db['bronze_layer']

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

# Consume messages and store in MongoDB
for message in consumer:
    row = message.value


    bronze_collection.update_one({'_id': row['id']}, {'$set': row}, upsert=True)
    print(f"Upserted: {row}")

print(f"Total documents in MongoDB: {bronze_collection.count_documents({})}")
