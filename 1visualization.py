from pymongo import MongoClient
import pandas as pd

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["fraud_detection"]

# Load data from gold_daily_totals collection
gold_daily_totals = pd.DataFrame(list(db.gold_daily_totals.find()))
print(gold_daily_totals.head())

# Load data from gold_total_avg_by_merchant collection
gold_total_avg_by_merchant = pd.DataFrame(list(db.gold_total_avg_by_merchant.find()))
print(gold_total_avg_by_merchant.head())

# Load data from gold_transactions_by_state collection
gold_transactions_by_state = pd.DataFrame(list(db.gold_transactions_by_state.find()))
print(gold_transactions_by_state.head())

import matplotlib.pyplot as plt

# Plot daily totals (line chart)
plt.figure(figsize=(10, 6))
plt.plot(gold_daily_totals['date'], gold_daily_totals['total_daily_amount'], marker='o', label="Total Daily Amount")
plt.title("Daily Transaction Trends", fontsize=14)
plt.xlabel("Date", fontsize=12)
plt.ylabel("Total Amount", fontsize=12)
plt.xticks(rotation=45)
plt.legend()
plt.grid()
plt.show()


# Plot daily transaction count (bar chart)
plt.figure(figsize=(10, 6))
plt.bar(gold_daily_totals['date'], gold_daily_totals['transaction_count'], color='orange')
plt.title("Daily Transaction Volume", fontsize=14)
plt.xlabel("Date", fontsize=12)
plt.ylabel("Transaction Count", fontsize=12)
plt.xticks(rotation=45)
plt.show()


plt.xticks(rotation=45)
plt.show()


# Transaction distribution by state (pie chart)
state_data = gold_transactions_by_state.set_index('state')
plt.figure(figsize=(10, 8))
plt.pie(state_data['transaction_count'], labels=state_data.index, autopct='%1.1f%%', startangle=140)
plt.title("Transaction Distribution by State", fontsize=14)
plt.show()



# Top states by total transaction amount (bar chart)
top_states = gold_transactions_by_state.nlargest(10, 'total_amount')
plt.figure(figsize=(10, 6))
plt.bar(top_states['state'], top_states['total_amount'], color='purple')
plt.title("Top States by Total Transaction Amount", fontsize=14)
plt.xlabel("State", fontsize=12)
plt.ylabel("Total Amount", fontsize=12)
plt.xticks(rotation=45)
plt.show()


# Scatter plot of transaction count vs. total amount
plt.figure(figsize=(8, 6))
plt.scatter(gold_transactions_by_state['transaction_count'], gold_transactions_by_state['total_amount'], alpha=0.6)
plt.title("Transaction Volume vs. Total Amount by State", fontsize=14)
plt.xlabel("Transaction Count", fontsize=12)
plt.ylabel("Total Amount", fontsize=12)
plt.grid()
plt.show()
