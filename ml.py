import pandas as pd
from pymongo import MongoClient
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix, ConfusionMatrixDisplay
import matplotlib.pyplot as plt
import joblib

# Step 1: Connect to MongoDB and Load Bronze Layer Data
client = MongoClient("mongodb://localhost:27017/")
db = client["fraud_detection"]
bronze_layer = pd.DataFrame(list(db.bronze_layer.find()))

# Step 2: Data Cleaning
# Drop rows with missing critical fields
bronze_layer = bronze_layer.dropna(subset=["amount", "card_id", "client_id", "mcc", "merchant_id"])
bronze_layer["amount"] = bronze_layer["amount"].str.replace(r"[\$,]", "", regex=True).astype(float)

# Select Features and Target
X = bronze_layer[["card_id", "client_id", "mcc", "merchant_id"]]
y = (bronze_layer["amount"] < 0).astype(int)  # Fraud if amount < 0

# Convert Categorical Variables to Dummy Variables
X = pd.get_dummies(X, columns=["mcc"], drop_first=True)

# Step 3: Train-Test Split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Step 4: Train a Machine Learning Model
model = RandomForestClassifier(max_depth=10, random_state=42)
model.fit(X_train, y_train)

# Step 5: Evaluate the Model
y_pred = model.predict(X_test)
print("Classification Report:")
print(classification_report(y_test, y_pred))

# Confusion Matrix
cm = confusion_matrix(y_test, y_pred)
disp = ConfusionMatrixDisplay(confusion_matrix=cm, display_labels=["Non-Fraud", "Fraud"])
disp.plot(cmap="Blues")
plt.show()

# Step 6: Save the Model
joblib.dump(model, "bronze_layer_model.pkl")
print("Model saved as 'bronze_layer_model.pkl'.")
