from google.cloud import bigquery
from google.cloud import storage
import os
import pandas as pd
from datetime import datetime, timedelta
import pyodbc
import sqlalchemy
import urllib

# from google import auth
# auth.authenticate_user()

project_id = "bkt-nonprod-dev-dwh-svc-00"
dataset_id = "bq_dataset_jira_statuses"
table_id = "bq_jira_statuses_tbl"

bq_client = bigquery.Client(project = project_id)

# job_config = bigquery.QueryJobConfig()
query = "SELECT * FROM `{}.{}.{}`".format(project_id,dataset_id, table_id)
print(query)


df = pd.read_gbq(query,
                project_id=project_id,
                # private_key='path_to/privatekey.json', -- credentials for the service account
                dialect="standard")

print(df.head())

df = df.head()

df['created'] = df['created'].astype(str)
df['key'] = df['key'].astype(str)
df['toString'] = df['toString'].astype(str)
df['fromString'] = df['fromString'].astype(str)

print(df.dtypes)


engine = sqlalchemy.create_engine("mssql+pyodbc://data_analytics_ro:Welcome!@PH2SHRPRDBID01:1433/powerbi_poc?driver=ODBC+Driver+17+for+SQL+Server")
# sql_query = "select * from DepartmentTest"
# df = pd.read_sql_query(sql_query, engine)
# print(df.head())

df.to_sql(name = 'DepartmentTest_dest_1',  con = engine, index=False, if_exists="replace", )









"""

params =  urllib.parse.quote_plus(r'DRIVER={SQL Server Native Client 11.0};SERVER=PH2SHRPRDBID01;DATABASE=powerbi_poc; Trusted_Connection=no')
 


#standard connection string
conn_str = 'mssql+pyodbc:///?odbc_connect={}'.format(params)
print(conn_str)

#create an engine to bulk insert the data
engine = create_engine(conn_str,fast_executemany=True)

#the command to send your dataframe to SQL Server. 
df.to_sql(name='bq_jira_statuses_tbl',schema= 'dbo', con=engine, if_exists='replace',index=False, chunksize = 1000)



conn = pyodbc.connect('Driver={SQL Server};'
                        'Server=PH2SHRPRDBID01;'
                        'Database=powerbi_poc;'
                        'UID=data_analytics_ro;'
                        'PWD=Welcome!;'
                        'Trusted_Connection=no;')


print('writing into sql started', datetime.now())
# df.to_sql(name'bq_jira_statuses_tbl', conn, if_exists = 'replace')
print(conn)
print('writing into sql finished', datetime.now())
"""

