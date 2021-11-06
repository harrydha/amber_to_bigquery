from google.cloud import bigquery

price_schema=[
            bigquery.SchemaField("duration","FLOAT","REQUIRED"),
            bigquery.SchemaField("spot_per_kwh","FLOAT","REQUIRED"),
            bigquery.SchemaField("per_kwh","FLOAT","REQUIRED"),
            bigquery.SchemaField("date","DATE","REQUIRED"),
            bigquery.SchemaField("nem_time","TIMESTAMP","REQUIRED"),
            bigquery.SchemaField("start_time","TIMESTAMP","REQUIRED"),
            bigquery.SchemaField("end_time","TIMESTAMP","REQUIRED"),
            bigquery.SchemaField("renewables","FLOAT","REQUIRED"),
            bigquery.SchemaField("channel_type","STRING","REQUIRED"),
            bigquery.SchemaField("spike_status","STRING","REQUIRED"),
            bigquery.SchemaField("type","STRING","REQUIRED"),
            bigquery.SchemaField("range","STRING","NULLABLE"),
            bigquery.SchemaField("estimate","BOOLEAN","NULLABLE")  
            ]

usage_schema=[
            bigquery.SchemaField("duration","FLOAT","REQUIRED"),
            bigquery.SchemaField("spot_per_kwh","FLOAT","REQUIRED"),
            bigquery.SchemaField("per_kwh","FLOAT","REQUIRED"),
            bigquery.SchemaField("date","DATE","REQUIRED"),
            bigquery.SchemaField("nem_time","TIMESTAMP","REQUIRED"),
            bigquery.SchemaField("start_time","TIMESTAMP","REQUIRED"),
            bigquery.SchemaField("end_time","TIMESTAMP","REQUIRED"),
            bigquery.SchemaField("renewables","FLOAT","REQUIRED"),
            bigquery.SchemaField("channel_type","STRING","REQUIRED"),
            bigquery.SchemaField("spike_status","STRING","REQUIRED"),
            bigquery.SchemaField("type","STRING","REQUIRED"),
            bigquery.SchemaField("channelIdentifier","STRING","REQUIRED"),
            bigquery.SchemaField("kwh","FLOAT","REQUIRED"),
            bigquery.SchemaField("quality","STRING","REQUIRED"),
            bigquery.SchemaField("cost","FLOAT","REQUIRED")
            ]

