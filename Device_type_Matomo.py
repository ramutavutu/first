import pandas as pd
import requests
import io
import csv
from datetime import datetime, date, timedelta
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import formatdate
from email import encoders
from google.cloud import storage
from google.cloud import secretmanager




def get_devicetype_data(start_date, var, var_name):
    
    url = f"https://bridge2solutions.matomo.cloud/index.php?module=API&format=CSV&idSite=" + str(var) + "&period=day&date=" + str(start_date) + "&method=DevicesDetection.getType&expanded=1&translateColumnNames=1&language=en&token_auth=c5414fd9e0063326ceefd7f832f93848&filter_limit=-1&translateColumnNames=1"
    print(url)    
    # getting response from the url
    response = requests.get(url)

    # response.headers
    # contentType = response.headers['content-type']
    # contentType
    # print(response.encoding)
    
    response = requests.get(url)  
    if response.status_code == 200:
        df = pd.read_csv(io.StringIO(response.text))
        # print(df.head())
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

def send_email( project_id, gcs_bucket_name, export_file, filename_chase, sendfrom, send_to, send_cc):
    bucket_name = gcs_bucket_name #"bkt-external-files-staging"
    blob_name = filename_chase # Name of the file to be attached
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)   

    username = sendfrom   # "no-reply-dba-alerts@bakkt.com"
    client = secretmanager.SecretManagerServiceClient()
    password = client.access_secret_version(request={"name": "projects/"+ project_id +"/secrets/no-reply-dba-alerts/versions/latest"}).payload.data.decode("UTF-8")
    print(password)
    # password = "VNB4jr$Il1=g"
    fileName = filename_chase #Name of the file to show as attachment name
    # sendfrom = "no-reply-dba-alerts@bakkt.com"
    # send_to = "ramu.tavutu@bakkt.com"
    server = "smtp.office365.com"
    port = 587
    subject = "Matomo Device Type Report for Chase"
    text = """ 
    Hello All ,
                
        Please find the Report attached. 
                
    Best Regards,
    Data Analytics
            """ 
                
    msg = MIMEMultipart()
    msg['From'] = sendfrom
    # msg['To'] =  send_to
    msg['To'] = '; '.join(send_to)
    msg['Cc'] = '; '.join(send_cc)
    msg['Date'] = formatdate(localtime = True)
    msg['Subject'] = subject
    msg.attach(MIMEText(text))
    part = MIMEBase('application', "octet-stream")
    part.set_payload(blob.open("r").read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', 'attachment; filename='+ fileName)
    msg.attach(part)
    smtp = smtplib.SMTP(server, port)

    smtp.ehlo()
    smtp.starttls()
    smtp.login(username,password)
    smtp.sendmail(msg['From'], [msg['To']], msg.as_string())
    # smtp.sendmail(msg['From'], [msg['To']], msg)
    smtp.quit()
    return 'Success'

# -----------------------------------------------Main Program start--------------------------------------------------------
# Please specify the below start_date and end_date
time_range = {
    "start_date": date.today() - timedelta(days=2), #'2023-12-08',
    "end_date" : date.today() - timedelta(days=1) #'2023-12-09'
}

project_id = "bkt-nonprod-dev-dwh-svc-00"
gcs_bucket_name = 'bkt-external-files-staging'

filename_chase = "Device_Type_chase" + "_" + datetime.now().strftime("%Y-%m-%d") + ".csv"
filename = "Device_Type"

sendfrom = "no-reply-dba-alerts@bakkt.com"
send_to = ["ramu.tavutu@bakkt.com", "no-reply-dba-alerts@bakkt.com"]
send_cc = ["ramu.tavutu@bakkt.com", "no-reply-dba-alerts@bakkt.com"]

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

# loop through the given dates and list of Vars
daterange = pd.date_range(start = time_range['start_date'], end = time_range['end_date'])
for var in company_name:
    print(var , "--->", company_name[var]) 
    var_name = company_name[var]
    try:
        for i in daterange:
            print(i.date())
            df = get_devicetype_data(i.date(), var, var_name) #Function call to fetch data from API
            # df_final = df_final.append(df, ignore_index= True )
            df_final = pd.concat([df_final,df])
       
    except:
        pass
    
print(df_final)
# below two lines can be commented
df_final.query("var_name == 'Chase'").to_csv('Device_Type_chase.csv', index=False)
df_final.to_csv('Device_Type.csv', index=False)

# writting to GCS bucket
export_file = "gs://" + gcs_bucket_name + "/" + filename_chase 
print(f"export file name {export_file}")
df_final.query("var_name == 'Chase'").to_csv(export_file, index=False) #exporting only Chase Device types
print('Output file written to GCS')



email_send_status = send_email(project_id, gcs_bucket_name, export_file, filename_chase, sendfrom, send_to, send_cc ) #Function call to send email
print(f"\n Email send status:  {email_send_status}")

# -----------------------------------------------Main Program End--------------------------------------------------------






