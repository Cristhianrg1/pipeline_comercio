import os
import sys
from dotenv import load_dotenv
from google.cloud import bigquery

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(PROJECT_ROOT)
load_dotenv()
FILE_PATH = os.path.join(PROJECT_ROOT, os.getenv("CREDENTIALS_PATH"))


def new_dataset_id(dataset_id):
    client = bigquery.Client.from_service_account_json(FILE_PATH)
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"
    dataset = client.create_dataset(dataset, exists_ok=True)
    print(f"Dataset {dataset_id} has been created")
