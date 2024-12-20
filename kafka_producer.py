from kafka import KafkaProducer
import pandas as pd
import json

# Load the dataset
data = pd.read_csv('./data/transactions_data.csv').head(150000)

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Send data in batches
batch_size = 1000
for i in range(0, len(data), batch_size):
    batch = data.iloc[i:i + batch_size].to_dict('records')
    for record in batch:
        producer.send('transactions', record)
    print(f"Sent batch {i // batch_size + 1}")

producer.flush()
print("Data streaming to Kafka completed.")
