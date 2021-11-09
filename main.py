import amberelectric #the package was developed by madpilot https://github.com/madpilot/amberelectric.py

from amberelectric.api import amber_api
import datetime
from datetime import date,timedelta,datetime
import pandas as pd
import webapp2
import pytz


from google.cloud import bigquery
import time

import api_token
import bigquery_schema
import authentication as authen


def api_init():

    #read api_token
    API_TOKEN=api_token.token
    
    configuration = amberelectric.Configuration(
        access_token = API_TOKEN)
    
    # Create an API instance
    api = amber_api.AmberApi.create(configuration)
    
    #listing all sites
    try:
        sites = api.get_sites()
    except amberelectric.ApiException as e:
        print("Exception: %s\n" % e)
    
    return(api,sites)

def price_extract(api,sites,start_day_v, end_date_v): 
    print('Getting data for start_date=',start_day_v,' end_date=',end_date_v)
    #get price
    site_id = sites[0].id
    try:
        if start_day_v!=None and end_date_v!=None:
        # start_date = pd.to_datetime(start_day_str,format='%Y-%m-%d')
        # end_date = pd.to_datetime(end_date_str,format='%Y-%m-%d')
            price_30sec = api.get_prices(site_id, start_date=start_day_v, end_date=end_date_v)
        else:
            price_30sec = api.get_prices(site_id) #get data for the current day
    except amberelectric.ApiException as e:
        print("Exception: %s\n" % e)

    #convert data to pd df
    temp=[]
    for data in price_30sec: #convert to list of dict
        temp.append(data.to_dict())
    
    hist_price=pd.DataFrame(temp)
    
    return(hist_price)

def manual_price_data_extract():
    date_range=pd.date_range(start='2020/09/01',end='2021/11/01',freq='MS') #generate a list of month starting date
    from pandas.tseries.offsets import MonthEnd
    
    data=pd.DataFrame()
    api,sites=api_init()
    
    for date_i in date_range:
        temp=price_extract(api,sites,date_i, date_i + MonthEnd(1))
        data=pd.concat([temp,data])
    data=data[['duration', 'spot_per_kwh', 'per_kwh', 'date', 'nem_time', 'start_time',
       'end_time', 'renewables', 'channel_type', 'spike_status', 'type','range', 'estimate']]
    data.to_csv('data.csv',index=False, encoding='utf-8')

def usage_extract(api,sites,start_day_v, end_date_v): 
    print('Getting data for start_date=',start_day_v,' end_date=',end_date_v)
    #get price
    site_id = sites[0].id
    try:
        if start_day_v!=None and end_date_v!=None:
        # start_date = pd.to_datetime(start_day_str,format='%Y-%m-%d')
        # end_date = pd.to_datetime(end_date_str,format='%Y-%m-%d')
            usage_30sec = api.get_usage(site_id, start_date=start_day_v, end_date=end_date_v)
        else:
            usage_30sec = api.get_usage(site_id) #get data for the current day
    except amberelectric.ApiException as e:
        print("Exception: %s\n" % e)

    #convert data to pd df
    temp=[]
    for data in usage_30sec: #convert to list of dict
        temp.append(data.to_dict())
    
    hist_price=pd.DataFrame(temp)
    
    return(hist_price)

def manual_usage_data_extract():
    date_range=pd.date_range(start='2020/09/01',end='2021/11/01',freq='MS') #generate a list of month starting date
    from pandas.tseries.offsets import MonthEnd
    
    data=pd.DataFrame()
    api,sites=api_init()
    
    for date_i in date_range:
        temp=usage_extract(api,sites,date_i, date_i + MonthEnd(1))
        data=pd.concat([temp,data])
    
    data=data[['duration', 'spot_per_kwh', 'per_kwh', 'date', 'nem_time', 'start_time',
       'end_time', 'renewables', 'channel_type', 'spike_status', 'type',
       'channelIdentifier', 'kwh', 'quality', 'cost']]
    
    data.to_csv('usage_data.csv',index=False, encoding='utf-8')


def upload_to_cloud_storage(storage_client,source_df,destination_blob_name,destination_bucket_name):
    print('Initiating uploading to Google Cloud Storage.')

    destination_bucket = storage_client.get_bucket(destination_bucket_name)
    blob = destination_bucket.blob(destination_blob_name)
    
    print('Destination bucket: ',destination_bucket)
    print('Blob name: ',blob)

    blob.upload_from_string(source_df.to_csv(encoding='utf-8',index=False),'text/csv')

    print('Dataframe has been uploaded to {}.'.format(destination_bucket_name + "/"+destination_blob_name))    

def upload_to_bigquery(schema,dataset_id,table_id,source_file_uri):
    print('Initiating uploading to BigQuery table.')
    bq_client = bigquery.Client(
    credentials=authen.bq_credentials,
    project=authen.bq_credentials.project_id,)

    dataset_id = dataset_id
    dataset_ref = bq_client.dataset(dataset_id)
    job_config = bigquery.LoadJobConfig()

    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND #WRITE_TRUNCATE = OVERWRITE THE EXISTING TABLE, TO APPEND USE "WRITE_APPEND"
    job_config.schema = schema
    job_config.skip_leading_rows = 1 #obmit headers, will rely on the schema for headers and data types
    # The source format defaults to CSV, so the line below is optional.
    job_config.source_format = bigquery.SourceFormat.CSV
    uri =source_file_uri

    load_job = bq_client.load_table_from_uri(
        uri, dataset_ref.table(table_id),job_config=job_config
    )  # API request
    print("Starting BigQuery job {}".format(load_job.job_id))

    count=0
    load_job.result()  # Waits for table load to complete.
    while load_job.result()==None and count<=45:
        time.sleep(2)
        count=count+1


    print("Job finished loading file {} from GCS to BigQuery.".format(source_file_uri))

    destination_table = bq_client.get_table(dataset_ref.table(table_id))
    print("Total rows are {}.".format(destination_table.num_rows))
    
def temp_data_store(pd_df,bucket,file):
    
    print('Upload the total file to the temp folder.')

    upload_to_cloud_storage(authen.storage_client,
                            pd_df,
                            file,
                            bucket)
    
def del_row_from_bq(table_name,start_date): #start_date is datetime type
    count=0
    print('Removing old data from bigquery table.')
    bq_Client = bigquery.Client(
                            credentials=authen.bq_credentials,
                            project=authen.bq_credentials.project_id,)
    query_statement="DELETE " + "`" + table_name + "`" + """ WHERE DATE(nem_time,"Australia/Brisbane")>=""" + "'" + start_date.strftime('%Y-%m-%d') + "'"
        
    print('Running query:')
    print(query_statement)
    query_job=bq_Client.query(query_statement)
    query_job.result()
    while query_job.result()==None and count<=30:
        time.sleep(2)
        count=count+1
    print ('Remove overlaped data from BigQuery has been completed')
    
def price_data_init(api,sites,current_day):
    #get price data for the current day and the previous day
    today_price_data=price_extract(api,sites,current_day+timedelta(days=-1), current_day)
    
    today_price_data=today_price_data[['duration', 'spot_per_kwh', 'per_kwh', 'date', 'nem_time', 'start_time',
       'end_time', 'renewables', 'channel_type', 'spike_status', 'type','range', 'estimate']] #to prevent potential changes in the schema in the future
    
    if len(today_price_data)>0:
        #push data to a temp folder on GCS
        temp_data_store(today_price_data,authen.destination_bucket_name,authen.price_data_folder+authen.price_temp_file)
        
        #store daily price data files on GCS
        for d in today_price_data['date'].unique():
            file_name=authen.price_data_folder+"price_" + d.strftime("%Y_%m_%d")+".csv"
            upload_to_cloud_storage(authen.storage_client,
                            today_price_data,
                            file_name,
                            authen.destination_bucket_name)
        
        
        date_to_del=today_price_data['nem_time'].min().date()
        del_row_from_bq(authen.price_bq_table,date_to_del)
        
        file_uri="gs://" + authen.destination_bucket_name +"/"+ authen.price_data_folder+authen.price_temp_file
        upload_to_bigquery(bigquery_schema.price_schema, 'data', 'price', file_uri)
    else:
        print("Data was not succesfully retrieved from Amber API")
        
def usage_data_init(api,sites,current_day):
    #get usage data for the current day and the two previous day
    today_usage_data=usage_extract(api,sites,current_day+timedelta(days=-2), current_day)
    
    today_usage_data=today_usage_data[['duration', 'spot_per_kwh', 'per_kwh', 'date', 'nem_time', 'start_time',
       'end_time', 'renewables', 'channel_type', 'spike_status', 'type',
       'channelIdentifier', 'kwh', 'quality', 'cost']] #to prevent potential changes in the schema in the future
    
    if len(today_usage_data)>0:
        #push data to a temp folder on GCS
        temp_data_store(today_usage_data,authen.destination_bucket_name,authen.usage_data_folder+authen.usage_temp_file)
        
        #store daily price data files on GCS
        for d in today_usage_data['date'].unique():
            file_name=authen.usage_data_folder+"usage_" + d.strftime("%Y_%m_%d")+".csv"
            upload_to_cloud_storage(authen.storage_client,
                            today_usage_data,
                            file_name,
                            authen.destination_bucket_name)
        
        
        date_to_del=today_usage_data['nem_time'].min().date()
        del_row_from_bq(authen.usage_bq_table,date_to_del)
        
        file_uri="gs://" + authen.destination_bucket_name +"/"+ authen.usage_data_folder+authen.usage_temp_file
        upload_to_bigquery(bigquery_schema.usage_schema, 'data', 'usage', file_uri)
    else:
        print("Data was not succesfully retrieved from Amber API")

#%%

def main():
    api,sites=api_init()
        
    to_day=datetime.today()
    t_mel=to_day.astimezone(pytz.timezone("Australia/Melbourne")).date()
 
    price_data_init(api,sites,t_mel)
    usage_data_init(api, sites, t_mel)
    


#########################################################################################

class MainPage(webapp2.RequestHandler):
    def get(self):
        #check if the request comes from a cron job, and only allow the cron job to trigger the pipeline
        if self.request.headers.get('X-AppEngine-Cron') is None:
            self.error(403)
            print('Not from a cron job')
        else:
            print('Innitiate Amber data extract.')
            main()
            print('Amber data extract has been finished with no error!')

app = webapp2.WSGIApplication([('your_app_url', MainPage)], debug=False) 



















































