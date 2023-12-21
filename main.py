# =================================================================================================
# Importing packages
# ==============================================================================================
from datetime import datetime, timedelta
import os
from io import BytesIO, StringIO
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.contrib.operators.gcs_delete_operator import GoogleCloudStorageDeleteOperator
from google.cloud import storage
from airflow.operators.empty import EmptyOperator
import requests
import csv
import pandas as pd
from mapping import company_name, segment_name
from config import API_CONFIG, QUERY_PARAMS
import configparser
import logging


# =================================================================================================
# Client for interacting with the Google Cloud Storage API
# =================================================================================================

storage_client = storage.Client()

# =================================================================================================
# Defining arguments for dag 
# =================================================================================================
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}
# =================================================================================================


# =================================================================================================
# Reading variables.properties and performing declaration of the variables
# =================================================================================================
env="DEV"

config = configparser.ConfigParser()
sample_config = """
[DEV]
Staging_Dataset = bkt_matomo_staging
Final_Dataset = bkt_matomo_curated
Transformation_Dataset = bkt_matomo_transformed
Project_Name = bkt-nonprod-dev-dwh-svc-00
Source_Landing_Bucket = bkt-matomo-staging
Destination_Landing_Bucket = bkt-matomo-destination
"""
# config.read("/home/airflow/gcs/data/variables.properties")
config.read_string(sample_config)

Source_Landing_Bucket = config[env]['Source_Landing_Bucket']
Destination_Landing_Bucket = config[env]['Destination_Landing_Bucket']
Project_Name = config[env]['Project_Name']
Staging_Dataset = config[env]['Staging_Dataset']
Final_Dataset = config[env]['Final_Dataset']
Transformation_Dataset = config[env]['Transformation_Dataset']
# =================================================================================================


# =====================================================================================================
# Calling matomo rest api and loading all the files to bakkt gcs bucket
# =========================================================================================================
def call_api_to_gcs(**kwargs):
    client = storage.Client()
    bucket_name = Source_Landing_Bucket
    bucket = client.bucket(bucket_name)

    base_url = API_CONFIG['base_url']
    endpoint = API_CONFIG['endpoint']

    # filename = (datetime.now() - timedelta(days=30)).strftime("%B_%Y")
    filename = "January_2023"

    for params in QUERY_PARAMS:
        print(params)

        SITEID = params['idSite']
        if SITEID in company_name:
            Id = company_name[SITEID]
        else:
            print(f"No matching Site ID found for {SITEID}")
            continue

        seg = ""
        SEGMENT= params['segment']
        if SEGMENT in segment_name:
            seg = segment_name[SEGMENT]
        else:
            print(f"No matching Segment ID found for {SEGMENT}")
            continue

        url = f"{base_url}/{endpoint}"
        response = requests.get(url, params=params)

        data = response.text.strip().splitlines()

        blob_name = f"{Id}_{seg}_{filename}.csv"
        blob = bucket.blob(blob_name)

        with open(blob_name, "w", newline="") as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_reader = csv.reader(data)
            for row in csv_reader:
                csv_writer.writerow(row)

        blob.upload_from_filename(blob_name)

        print(f"Data saved to {blob_name} in GCS bucket {bucket_name}")


# =================================================================================================
# Defining all the tasks we want to include in the dag 
# =================================================================================================

with DAG(
    'bkt_matomo_pipeline', 
    default_args=default_args, 
    schedule_interval='@monthly',
    catchup=False,
    params={
        "Project_Name": Project_Name,
        "Staging_Dataset" : Staging_Dataset,
        "Final_Dataset" : Final_Dataset,
        "Transformation_Dataset" : Transformation_Dataset,
    }
) as dag:

    # Start dummy task 
    start = EmptyOperator(
        task_id='Start_DAG'
    )


    # End dummy task
    end = EmptyOperator(
        task_id='End_DAG'
    )

    # Task to fetch data from matomo's rest api and load data into GCS bucket.
    api_to_gcs_operator = PythonOperator(
    task_id='call_api_to_gcs',
    python_callable=call_api_to_gcs,
    provide_context=True,
    dag=dag
    )

    #Fetching the required data from staging files to BQ transformation dataset.
    filtering_data = BigQueryInsertJobOperator(
    task_id="filtering_data_from_raw_files",
    configuration={
        "query": {
            "query": "{% include 'sql/filtering_data.sql' %}",
            "useLegacySql": False,
        }
    }
    )

    # Task for exporting GCS files to BQ Staging Dataset
    with TaskGroup("export_gcs_to_bq", tooltip="Export Matomo data from GCS to BQ") as export_gcs_to_bq:
        # filename = (datetime.now() - timedelta(days=30)).strftime("%B_%Y")
        filename = "January_2023"
        for params in QUERY_PARAMS:
            client_name = company_name[params['idSite']] 
            seg_name = segment_name[params['segment']]
            file_path = client_name + "_" + seg_name + "_" + filename + ".csv"
            table_name = client_name + "_" + seg_name

            task_id = 'gcs_to_bq_{}'.format(table_name)
            gcs_to_bq = GCSToBigQueryOperator(
                task_id=task_id,
                bucket=Source_Landing_Bucket,
                source_objects=file_path,
                destination_project_dataset_table=Project_Name + "." + Staging_Dataset + "." + table_name,
                source_format='CSV',
                create_disposition='CREATE_IF_NEEDED',
                write_disposition='WRITE_TRUNCATE',
                skip_leading_rows=1,
                field_delimiter=',',
                autodetect=True,
            )
    start >> api_to_gcs_operator >> export_gcs_to_bq >> filtering_data
        

    #Updating and doing some transformations from BQ staging dataset to BQ transformation dataset.
    update_tables = BigQueryInsertJobOperator(
    task_id="updating_data_from_staging_dataset",
    configuration={
        "query": {
            "query": "{% include 'sql/update_data.sql' %}",
            "useLegacySql": False,
        }
    }
    )

    #Transferring data to final dataset's tables
    data_curation = BigQueryInsertJobOperator(
    task_id="data_curation",
    configuration={
        "query": {
            "query": "{% include 'sql/data_curation.sql' %}",
            "useLegacySql": False,
        }
    }
    )

    #Truncating all the staging files
    Truncate_tables = BigQueryInsertJobOperator(
    task_id="Truncate_tables",
    configuration={
        "query": {
            "query": "{% include 'sql/truncate_tables.sql' %}",
            "useLegacySql": False,
        }
    }
    )

    #dump task
    dummy_task = EmptyOperator(
    task_id='Dummy'
    )

    # Define the BigQuery dataset and GCS bucket information
    bq_dataset_id = Final_Dataset
    gcs_bucket = Source_Landing_Bucket
    csv_prefix = 'csv'
    xlsx_prefix = 'excel'
    xlsx_bucket = Destination_Landing_Bucket
    bq_hook = BigQueryHook()
    # Loop through all tables in the BigQuery dataset and copy them to GCS
    with TaskGroup("csv_conversion", tooltip="CSV conversion") as csv_conversion:
        for table in bq_hook.get_dataset_tables(dataset_id=bq_dataset_id):
            try:
                # Define the BigQuery table information
                bq_table_id = table['tableId']
                gcs_csv_object = f'{csv_prefix}/{bq_table_id}.csv'
                gcs_xlsx_object = f'{xlsx_prefix}/{bq_table_id}.xlsx'
                xlsx_filename = str(bq_table_id) + '.xlsx'
                # Define the BigQueryToGCSOperator to copy the table to GCS
                export_task_id = f'export_{bq_table_id}_to_gcs'
                export_task = BigQueryToGCSOperator(
                    task_id=export_task_id,
                    source_project_dataset_table=f'{bq_dataset_id}.{bq_table_id}',
                    destination_cloud_storage_uris=[f'gs://{gcs_bucket}/{gcs_csv_object}'],
                    export_format='CSV',
                    field_delimiter=','
                )
            except Exception as e:
                # Log the exception and continue with the next table
                logging.exception(f"An error occurred while executing the export task for {bq_table_id}: {e}")
    data_curation >> csv_conversion >> dummy_task

    
    # Function to convert CSV to Excel
    def convert_csv_to_excel(blob_name):

        # Read the CSV file into a pandas DataFrame
        df = pd.read_csv(f"gs://{gcs_bucket}/{blob_name}")

        # Construct the Excel file name by replacing .csv with .xlsx
        excel_file_name = blob_name.replace("csv/","").replace('.csv', '.xlsx')

        # Convert CSV to Excel
        excel_writer = pd.ExcelWriter(excel_file_name, engine='xlsxwriter')
        df.to_excel(excel_writer, index=False)
        excel_writer.save()
        excel_writer.close()

        # Upload Excel file to destination GCS bucket
        destination_bucket = storage_client.bucket(xlsx_bucket)
        excel_blob = destination_bucket.blob("excel/" + excel_file_name)
        excel_blob.upload_from_filename(excel_file_name)

        # Print a message to indicate success
        print(f"Converted {blob_name} to {excel_file_name} and uploaded to {xlsx_bucket}")

        # Remove temporary Excel file
        os.remove(excel_file_name)

    # Iterate over all CSV files in the source bucket
    blobs = storage_client.get_bucket(gcs_bucket).list_blobs(prefix="csv/")
    with TaskGroup("excel_conversion", tooltip="Excel conversion") as excel_conversion:
        for blob in blobs:
            try:
                if blob.name.endswith(".csv"):
                    # Create a PythonOperator for each CSV file
                    convert_task = PythonOperator(
                        task_id=f"convert_{blob.name}_to_excel".replace("/","_"),
                        python_callable=convert_csv_to_excel,
                        op_args=[blob.name],
                        dag=dag
                    )
            except Exception as e:
                # Log the exception and continue with the next table
                logging.exception(f"An error occurred while executing the export task for {bq_table_id}: {e}")
            
    dummy_task >> excel_conversion >> end


# =================================================================================================
# Defining the execution order of tasks we created
# =================================================================================================
    # Set dependencies
    filtering_data >> Truncate_tables >> update_tables >> data_curation
    
     
# ================================================================================================