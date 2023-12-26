from google.cloud import bigquery
from google.cloud import storage
from google.auth import compute_engine
import os
import pandas as pd
from datetime import datetime, timedelta
import pyodbc
import sqlalchemy
import urllib

# from google import auth
# auth.authenticate_user()

project_id = "bkt-prod-dwh-svc-00"
dataset_id = "bq_dataset_jira_statuses"
table_id = "bq_jira_statuses_tbl"
print('user name:  ', os.getlogin())

bq_client = bigquery.Client(project = project_id)
credentials = compute_engine.Credentials()

print('credentials:   ', credentials)

# job_config = bigquery.QueryJobConfig()
#query = "SELECT * FROM `{}.{}.{}`".format(project_id,dataset_id, table_id)
query = "SELECT * FROM `bkt-prod-dwh-svc-00.bq_dataset_jira_statuses.bq_jira_statuses_tbl`"
print(query)


df = pd.read_gbq(query,
                project_id=project_id,
		credentials = credentials,
                # private_key='path_to/privatekey.json', -- credentials for the service account
                dialect="standard")

print(df.head())

#df = df.head()

df['created'] = df['created'].astype(str)
df['key'] = df['key'].astype(str)
df['toString'] = df['toString'].astype(str)
df['fromString'] = df['fromString'].astype(str)

print(df.dtypes)


engine = sqlalchemy.create_engine("mssql+pyodbc://PythonBQSQL:gFtymb~u8Xj2[;Q*eTc}J6@mktdwprdsql:1433/DBA?driver=ODBC+Driver+17+for+SQL+Server")
#sql_server = sqlalchemy.create_engine("mssql+pyodbc://PythonBQSQL:gFtymb~u8Xj2[;Q*eTc}J6@mktdwprdsql:1433/PMO_Reports?driver=SQL+Server")
# sql_query = "select * from DepartmentTest"
# df = pd.read_sql_query(sql_query, engine)
# print(df.head())

df.to_sql(name = 'bq_jira_statuses_tbl',  con = engine, index=False, if_exists="replace")

