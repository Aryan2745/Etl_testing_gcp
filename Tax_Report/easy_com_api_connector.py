import time
from typing import List
import requests
import os

import pandas as pd
from google.cloud import bigquery
from sqlalchemy import create_engine, inspect
from airflow.models import Variable

from datetime import datetime

import os
import re
import zipfile
import io
import json


from google.oauth2 import service_account
from google.cloud import  storage

def get_gcs_client(credentials_info)->storage.Client:
    credentials_info = json.loads(credentials_info)
    
    SCOPES = ['https://www.googleapis.com/auth/cloud-platform']
    
    credentials = service_account.Credentials.from_service_account_info(credentials_info, scopes=SCOPES)
    client = storage.Client(credentials=credentials, project="shopify-pubsub-project")
    return client


BASE_URL = "https://api.easyecom.io"
TOKEN = ''

def generate_api_token():
    global TOKEN
    if not TOKEN:
        auth_api_end_point = BASE_URL + "/access/token"
        body = {
            "email": Variable.get("EASYCOM_EMAIL"),
            "password": Variable.get("EASYCOM_PASSWORD"),
            "location_key": Variable.get("EASYCOM_LOCATION_KEY")
        }
        response = requests.post(auth_api_end_point, json=body)
        response_json = response.json()
        TOKEN = response_json.get("data", {}).get("token", {}).get("jwt_token") or ''
    
    return TOKEN

def generate_location_key_token(location_key):
    auth_api_end_point = BASE_URL + "/access/token"
    body = {
        "email": Variable.get("EASYCOM_EMAIL"),
        "password": Variable.get("EASYCOM_PASSWORD"),
        "location_key": location_key
    }
    response = requests.post(auth_api_end_point, json=body)
    response_json = response.json()
    location_key_token = response_json.get("data", {}).get("token", {}).get("jwt_token") or ''
    
    return location_key_token




class EasyComApiConnector:
    data_download_bucket = "airflow-data-download"
    def __init__(self):
        token = generate_api_token()
        if not token:
            raise Exception("Invalid Token")
        self.token = token
        self.base_url = BASE_URL
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        self.client:bigquery.Client = None

    def send_get_request(self, url, params=None, auth_token = None, _retry=0, max_retry=3):

        if auth_token:
            headers = {
                "Authorization": f"Bearer {auth_token}",
                "Content-Type": "application/json"
            }
        else:
            headers = self.headers

        if params:
            response = requests.get(url, headers=headers, params=params)
        else:
            response = requests.get(url, headers=headers)
        # import pdb; pdb.set_trace()
        # print(";;;" + f"{response.status_code}" + ";"+response.content.decode())
        if(response.status_code!=200): 
            if _retry+1<max_retry: 
                mint = 60
                print(f"SLEEPING :: Error getting data retrying in {mint} min")
                time.sleep(60*mint)
                return self.send_get_request(url, params=params, auth_token=auth_token, _retry=_retry+1, max_retry=max_retry)
            print(";;;" + f"{response.status_code}" + ";"+response.content.decode())
        if(len(response.content)==0 or response.content is None): return None
        return response.json()
    
    def send_post_request(self, url, body):
        response = requests.post(url, headers=self.headers, json=body)
        return response.json()
    
    def create_table(self):
        """Create table in BigQuery if it does not exist."""
        if not self.table_exists(self.table.__name__):
            print(f"Creating {self.name} table in BigQuery")
            self.table.metadata.create_all(self.engine)
            print(f"{self.name} table created in BigQuery")
        else:
            print(f"{self.name} table already exists in BigQuery")

    def table_exists(self, table_name):
        """Check if table exists in BigQuery."""
        inspector = inspect(self.engine)
        return table_name in inspector.get_table_names()
    
    def load_data_to_bigquery(self, data, extracted_at, passing_df = False, _retry=0, max_retry=3):
        """Load the data into BigQuery."""
        print(f"Loading {self.name} data to BigQuery")
        if not passing_df:
            data = pd.DataFrame(data)
        data["ee_extracted_at"] = extracted_at
        # table = self.client.get_table(self.table_id)
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            # schema=table.schema,
        )
        try:
            job = self.client.load_table_from_dataframe(data, self.table_id, job_config=job_config)
            job.result()
        except Exception as e:
            if _retry+1<max_retry: 
                mint = 60
                print('Error:', str(e))
                print(f"SLEEPING :: Error inserting to BQ retrying in {mint} min")
                time.sleep(60*mint) # 15 min
                return self.load_data_to_bigquery(data, extracted_at, passing_df=passing_df, _retry=_retry+1, max_retry=max_retry)
            raise e
        

    def update_data(self, data, merge_query):
        """Update the data in BigQuery."""
        print(f"Updating {self.name} data in BigQuery")
        df = pd.DataFrame(data)
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
        job = self.client.load_table_from_dataframe(df, self.temp_table_id, job_config=job_config)
        job.result()
        print('temp_table is created')
        query_job = self.client.query(merge_query)
        query_job.result()

        self.client.delete_table(self.temp_table_id)

    def truncate_table(self, table_name = None):
        """Truncate the BigQuery table by deleting all rows."""
        print(f"Truncating {self.name} table for Easy eCom")
        table_name = table_name or self.table.__tablename__
        if not self.table_exists(table_name):
            print(f"{self.name} table does not exist in BigQuery")
            return
        table_ref = self.client.dataset(self.dataset_id).table(table_name)
        truncate_query = f"DELETE FROM `{table_ref}` WHERE true"
        self.client.query(truncate_query).result()  # Executes the DELETE query

    def _download_csv(self, url, report_type):
        """Download the CSV data. check if the url is returning a csv file or a zip folder ?"""
        response = requests.get(url, stream=True)
        content_type = response.headers.get('Content-Type')

        date = datetime.now().strftime("%Y-%m-%d")

        file_name = f"{report_type}_{date}.csv"
        file_path = f"files/{file_name}"

        # Ensure the directory exists
        os.makedirs('files', exist_ok=True)
        
        if "zip" in content_type:
            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                z.extractall('files')
                extracted_file_name = z.namelist()[0]
                z.extract(extracted_file_name, 'files')
                file_path = f"files/{extracted_file_name}"
        else:
            with open(file_path, "wb") as file:
                file.write(response.content)
                for chunk in response.iter_content(chunk_size=8192):  # Adjust chunk_size if necessary
                    if chunk:
                        file.write(chunk)
        return file_path
    
    def download_csv(self, url, report_type):
        file_path = self._download_csv(url, report_type)
        data_frames = pd.read_csv(file_path, chunksize=10000, on_bad_lines='skip')

        # os.remove(file_path)
        return data_frames
    
    # Function to convert column names to BigQuery-compatible names
    def clean_column_name(self, name):
        # Remove invalid characters and replace spaces with underscores

        name = re.sub(r'\W+', '_', name)
        if not name[0].isalpha() and name[0] != '_':
            name = '_' + name
        return name
    
    def convert(self, val, _type, **kwargs):
        if val is None: return val
        try:
            if _type == datetime:
                return datetime.strptime(val, kwargs["strptime"])
            return _type(val)
        except:
            return None
        
    def get_google_credentials_info(self):
        return Variable.get("GOOGLE_BIGQUERY_CREDENTIALS")
    
    def get_table_columns(self): 
        table = self.client.get_table(self.table_id) 
        #print(list(field.name for field in table.schema))
        return [field.name for field in table.schema]

    def convert_if_found(self, df:pd.DataFrame, keys:List[str], _type):
        for key in keys:
            if key in df.columns:
                df[key] = df[key].apply(lambda x: self.convert(x, _type))
        return df
    
    def apply_if_found(self, df:pd.DataFrame, keys:List[str], func):
        for key in keys:
            if key in df.columns:
                df[key] = df[key].apply(func)
        return df

    def upload_to_gcs(self, file_path, bucket_name=None, file_name=None, credentials_info=None)->str:
        if not bucket_name:
            bucket_name = self.data_download_bucket
        if not file_name:
            file_name = os.path.basename(file_path)
        if not credentials_info:
            credentials_info = Variable.get("GOOGLE_CLOUD_STORE_TOKEN")
        storage_client = get_gcs_client(credentials_info)
        
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        blob.upload_from_filename(file_path)
        return self.get_blob_uri(blob)

    def get_blob_uri(self, blob)->str:
        return 'gs://' + blob.id[:-(len(str(blob.generation)) + 1)]

    def download_from_gcs(self, gcs_uri, bucket_name=None, to_file_path=None, credentials_info=None):
        from utils.google_cloud import get_gcs_client
        if not bucket_name:
            bucket_name = self.data_download_bucket
        if not credentials_info:
            credentials_info = Variable.get("GOOGLE_CLOUD_STORE_TOKEN")
        if not to_file_path:
            to_file_path = f"./{os.path.basename(gcs_uri)}"
        storage_client = get_gcs_client(credentials_info)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(gcs_uri[6+len(bucket_name):])
        blob.download_to_filename(to_file_path)
        return to_file_path
