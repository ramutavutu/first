import pandas as pd
import requests
import io
import csv
from datetime import datetime
import os



def get_devicetype_data(start_date, var, var_name):
    
    url = f"https://bridge2solutions.matomo.cloud/index.php?module=API&format=CSV&idSite=" + str(var) + "&period=day&date=" + str(start_date) + "&method=DevicesDetection.getType&expanded=1&translateColumnNames=1&language=en&token_auth=c5414fd9e0063326ceefd7f832f93848&filter_limit=-1&translateColumnNames=1"
    print(url)
    
    # getting response from the url
    response = requests.get(url)

    # response.headers
    # contentType = response.headers['content-type']
    # contentType
    # print(response.encoding)
    
    
    # writing the csv file to local 
    open('response.csv', 'w', newline='\n', encoding='utf-8').write(response.text)
    if response.status_code == 200:
        csv_data = response.text
        # print(csv_data)
        # df = pd.read_csv(pd.compat.StringIO(csv_data))
        # use_cols = ['Label','Unique visitors','Visits']
        df = pd.read_csv('response.csv', skiprows=0, delimiter=",")
    else:
        print('failed to fetch data from the API')

    # fetching required columns and adding the date we querying as a column
    df = df[['Label','Unique visitors', 'Visits','Actions','Users']]
    df['var_name'] = var_name
    df['Web_Activity_Date'] = start_date
    df = df.fillna(0)
    
    # data types handling
    df['Unique visitors'] = df['Unique visitors'].astype(int)
    df['Actions'] = df['Actions'].astype(int)
    df['Users'] = df['Users'].astype(int)
    
    return df




start_date = '2023-12-08'
end_date = '2023-12-09'
gcs_bucket_name = 'bkt-external-files-staging'
df_final = pd.DataFrame() 

#Mapping of client's SITE ID to client name
company_name = {
    1:"b2s",
    2:"Delta",
    3:"WF",
    4:"rbc",
    5:"fsv",
    6:"psg",
    7:"FDR",
    8:"FDR_PSCU",
    9:"pnc",
    14:"BSWIFT",
    15:"Chase",
    16:"CITIFINTECH",
    18:"GRASSROOTSUK",
    20:"scotia",
    21:"ua",
    22:"VITALITYCA",
    24:"VITALITYUS",
    25:"CARD",
    26:"TAZ",
    27:"PAC",
    28:"CHIP",
    29:"PGA",
    34:"VIRGINUA",
    35:"bac"
}

daterange = pd.date_range(start = start_date, end=end_date)
for var in company_name:
    print(var , "--->", company_name[var]) 
    try:
        for i in daterange:
            print(i)
            df = get_devicetype_data(i.date(), var, company_name[var])
            df_final = df_final.append(df, ignore_index= True )
        # df_final = pd.concat(df_final,df)
        # print(df_final)
    except:
        pass
    
print(df_final)
print(df_final.query("var_name == 'Chase'"))
df_final.query("var_name == 'Chase'").to_csv('Device_Type_chase.csv', index=False)
# export_file = "gs://" + gcs_bucket_name + "/Matomo_Device_Type_" + datetime.now().strftime("%Y-%m-%d") + ".csv"
# df_final.to_csv(export_file, index=False)
df_final.to_csv('Device_Type.csv', index=False)



