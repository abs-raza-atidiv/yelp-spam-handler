import pandas as pd 
from google.cloud import bigquery
# import pandas_gbq as pgbq
# import pytz
# import datetime

# gcp_project = 'productivity-377410'
# bq_dataset = 'stagging_dataset'

gcp_project = 'atidiv-yelp'
bq_dataset = 'yelp_qa_tool'


client = bigquery.Client(project=gcp_project)
# dataset_ref = client.dataset(bq_dataset)

def gcp2df(sql):
    query = client.query(sql)
    results = query.result()
    return results.to_dataframe()

def df2gcp(dataframe, table_name, mode='append'):
    # table_name = 'clickup_task'
    table_id = gcp_project+'.'+bq_dataset+'.'+table_name

    # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table to create.
    # table_id = "your-project.your_dataset.your_table_name"

    # records = [
        # {
        #     "id": "The Meaning of Life",
            # "release_date": pytz.timezone("Europe/Paris")
            # .localize(datetime.datetime(1983, 5, 9, 13, 0, 0))
            # .astimezone(pytz.utc),
            # Assume UTC timezone when a datetime object contains no timezone.
            # "dvd_release": datetime.datetime(2002, 1, 22, 7, 0, 0),
    #         "name": ""
    #     }
      
    # ]

    upload_mode = 'WRITE_TRUNCATE' if mode == 'replace' else None

    job_config = bigquery.LoadJobConfig(
        # Specify a (partial) schema. All columns are always written to the
        # table. The schema is used to assist in data type definitions.
        # schema=[
        #     # Specify the type of columns whose type cannot be auto-detected. For
        #     # example the "title" column uses pandas dtype "object", so its
        #     # data type is ambiguous.
        #     bigquery.SchemaField("title", bigquery.enums.SqlTypeNames.STRING),
        #     # Indexes are written if included in the schema by name.
        #     bigquery.SchemaField("wikidata_id", bigquery.enums.SqlTypeNames.STRING),
        # ],
        # Optionally, set the write disposition. BigQuery appends loaded rows
        # to an existing table by default, but with WRITE_TRUNCATE write
        # disposition it replaces the table with the loaded data.
        write_disposition=upload_mode,

        # ''' CHANGE TO APPEND ONLY '''

    )
    job_config.schema_update_options = [
        bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
    ]
    
    job = client.load_table_from_dataframe(
        dataframe, table_id, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )

    '''
    for col in df1.columns:
        
        df1[col] = df1[col].replace('HNB-38', 'HNB38')


for elm in lst:
        if elm > -1:
            elm

    '''