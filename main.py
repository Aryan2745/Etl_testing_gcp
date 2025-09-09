import pandas as pd 
import json
import base64
from google.oauth2 import service_account
from google.cloud import bigquery
from sqlalchemy import create_engine 
import logging
import requests 
from datetime import datetime
from google.cloud import secretmanager

def access_secret(secret_name, project_id):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(name=name)
    secret = response.payload.data.decode("UTF-8")
    return secret

project_id = "testing-dep-471617"
easy_ecom_email = access_secret("EASYCOM_EMAIL", project_id)
easy_ecom_password = access_secret("EASYCOM_PASSWORD", project_id)
easy_ecom_location_key = access_secret("EASYCOM_LOCATION_KEY", project_id)
logging.basicConfig(filename='easyecom_log.txt', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

def get_order_details_by_ref_code(ref_code, token):
    url = f"https://api.easyecom.io/orders/V2/getOrderDetails?reference_code={ref_code}"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        try:
            data = response.json()['data'][0]
            ref_code = data.get('reference_code', 'NA')
            order_status = data.get('order_status', 'NA')
            #ee_invoice = data.get('documents', {}).get('easyecom_invoice', 'NA')
            invoice_date = data.get('invoice_date','NA') 
            invoice_number = data.get('invoice_number','NA') 
            awb_number = data.get('awb_number', 'NA') 
            courier = data.get('courier', 'NA')
            courier_aggregator_name = data.get('courier_aggregator_name', 'NA')
            results = [] 
            for item in data.get('order_items', []):
                suborder_num = item.get('suborder_num', 'NA')
                results.append({
                    "reference_code": ref_code,
                    "suborder_num": suborder_num,
                    "invoice_date": invoice_date,
                    "order_status": order_status,
                    "invoice_number": invoice_number, 
                    "awb_number": awb_number,
                    "courier": courier,
                    "courier_aggregator_name": courier_aggregator_name
                })
            return results
        except Exception as e:
            logging.error(f"Parsing error for ref_code {ref_code}: {e}")
            return [] 
    elif response.status_code == 401:
        logging.info("JWT token expired. Refreshing token.")
        token = retrieve_jwt_token()
        return get_order_details_by_ref_code(ref_code,token)
    else:
        logging.error(f"Failed request for ref_code {ref_code}: {response.status_code} - {response.text}")
        return []
    
def fetch_all_order_details(df, token):
    all_details = []

    for ref_code in df['Reference_Code'].dropna().unique():
        logging.info(f"Fetching details for reference_code: {ref_code}")
        order_details = get_order_details_by_ref_code(ref_code, token)
        all_details.extend(order_details)  

    return pd.DataFrame(all_details)

def get_jwt_token():
    try:
        with open("jwt_token.txt", "r") as file:
            token = file.read().strip()
        if not token:
            logging.error("JWT token file is empty. Retrieving new token.")
            return retrieve_jwt_token()
        return token
    except FileNotFoundError:
        logging.error("JWT token file not found. Retrieving new token.")
        return retrieve_jwt_token() 
    
def retrieve_jwt_token():
    print("Creating a new token..")
    url = "https://api.easyecom.io/access/token"
    headers = {"Content-Type": "application/json"}
    data = {
        "email": easy_ecom_email,
        "password": easy_ecom_password,
        "location_key": easy_ecom_location_key
    }
    response = requests.post(url, headers=headers, json=data)
    if response.status_code == 200:
        token = response.json()['data']['token']['jwt_token']
        with open("jwt_token.txt", "w") as file:
            file.write(token)
        logging.info("New JWT token retrieved and stored.")
        return token 
    else:
        logging.error(f"Failed to retrieve JWT token: {response.text}")
        return None

def get_bq_client(credentials_info:str, project_id:str="shopify-pubsub-project")->bigquery.Client:
    credentials_info = base64.b64decode(credentials_info).decode("utf-8")
    credentials_info = json.loads(credentials_info)
    
    SCOPES = ['https://www.googleapis.com/auth/bigquery',
          'https://www.googleapis.com/auth/drive.readonly']
    
    credentials = service_account.Credentials.from_service_account_info(credentials_info, scopes=SCOPES)
    client = bigquery.Client(credentials=credentials, project=project_id)
    return client 

def read_from_gbq(bq_client,p_id,t_id):
    df = bq_client.query("SELECT DISTINCT Reference_Code FROM `testing-dep-471617.Data_Warehouse_Easyecom_testing.updated_orders`").to_dataframe() 
    return df

def main():
    try:
        credentials_info = access_secret("GOOGLE_BIGQUERY_CREDENTIALS", project_id)
        bq_client  = get_bq_client(credentials_info) 

        project_id = "testing-dep-471617" 
        dataset_id = "Data_Warehouse_Easyecom_testing" 
        table_id = 'testing-dep-471617.Data_Warehouse_Easyecom_testing.updated_orders'

        df = read_from_gbq(bq_client, project_id, dataset_id)
        print(f"Fetched {df.shape[0]} records from BigQuery")
        token = get_jwt_token() 
        result_df = fetch_all_order_details(df, token)

        print(result_df.head())
        result_df["pg_extracted_at"] = datetime.now() 
        job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
        job = bq_client.load_table_from_dataframe(result_df, table_id, job_config=job_config)
        job.result()

        print('Data loaded into BigQuery successfully.')

    except Exception as e:
        logging.error(f"Error in main execution: {str(e)}", exc_info=True)
        raise  

if __name__ == "__main__":
    main()
