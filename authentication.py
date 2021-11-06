from googleapiclient import http
from google.cloud import storage
from google.cloud import bigquery
from google.oauth2 import service_account

#authenticate api access to cloud storage
storage_client = storage.Client.from_service_account_json(
        'service_acc_key.json')

##authenticate api access to bigquery
bq_credentials = service_account.Credentials.from_service_account_file(
    'service_acc_key.json',
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )

destination_bucket_name='amber_data' #GCS bucket

price_temp_file='temp/price_temp.csv'  #GCS temp file

price_data_folder='price_data/' #GCS daily usage data file

usage_temp_file='temp/usage_temp.csv'  #GCS temp file

usage_data_folder='usage_data/' #GCS daily usage data file


usage_bq_table='bigquery_table_for_usage_data'

price_bq_table='bigquery_table_for_price_data'