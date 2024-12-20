import os
import zipfile
import pandas as pd


os.environ['KAGGLE_USERNAME'] = 'your_kaggle_username'
os.environ['KAGGLE_KEY'] = 'your_kaggle_api_key'

# Download dataset
os.system('kaggle datasets download -d computingvictor/transactions-fraud-datasets -p ./data')

# Extract the ZIP file
zip_file = './data/transactions-fraud-datasets.zip'
with zipfile.ZipFile(zip_file, 'r') as zip_ref:
    zip_ref.extractall('./data')

# Load dataset into a DataFrame
data = pd.read_csv('./data/transactions_data.csv')
print(f"Dataset contains {data.shape[0]} rows and {data.shape[1]} columns.")
