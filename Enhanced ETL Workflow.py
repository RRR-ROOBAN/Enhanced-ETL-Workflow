import pandas as pd 
import glob
from datetime import datetime
import xml.etree.ElementTree as ET
import os
import requests
import zipfile
import boto3
import credentials
import sqlalchemy

# ***SECTION 1: UnZip & Extract Data***

project_folder = os.path.dirname(os.path.abspath(__file__)) 


# Define output paths relative to the project folder
log_file =os.path.join(project_folder, "Etl_log.txt")
Extract_to =os.path.join(project_folder, "Extracted_data") # Folder where files will be extracted
Output_file = os.path.join(project_folder, "Transformed_data.csv") #Transformed Csv File Path

url="https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/source.zip"

# Get the absolute path of the directory where the current script is located
zip_path =project_folder+ r"\Source.zip" 

response=requests.get(url)


# In log adding date for better understaing
with open (log_file,"a") as dl:
    # Add a blank line
    dl.write("\n")
    # Add a simple header for separation
    dl.write("===== Logs Starts for 2024-11-29 =====\n")
    # Add a blank line
    dl.write("\n")

def log_progress (message):
    with open (log_file,"a") as l:
        l.write(f"{datetime.now()}:{message}\n")


# Download the file
if response.status_code==200:
    with open (zip_path,"wb") as f:
        f.write(response.content)
else:
    log_progress("Failed to download the file.")
    
# Extract the ZIP file
with zipfile.ZipFile(zip_path,"r") as zipref:
    zipref.extractall(Extract_to)   
    log_progress(f" Files Extracted to {Extract_to}")
    
log_progress("Download and extraction completed.")
print(Extract_to)

# Example paths for different file types...it will get list of desired files name
csv_file = glob.glob(os.path.join(Extract_to, "*.csv"))
json_file = glob.glob(os.path.join(Extract_to, "*.json"))
xml_file = glob.glob(os.path.join(Extract_to, "*.xml"))

# Function to extract data from CSV files
def extract_csv(file_path):
    return pd.read_csv(file_path)

# Function to extract data from json files
def extract_json(file_path):
    return pd.read_json(file_path,lines=True)

# Function to extract data from XML files

data=[]

def extract_xml(file_path):
    tree=ET.parse(file_path)
    root=tree.getroot()
    for i in root.findall("person"):
        Name=i.find("name").text
        Height=i.find("height").text
        Weigth=i.find("weight").text
        data.append({"name":Name,"height":Height,"weight": Weigth})
    return pd.DataFrame(data)


def extract_data():
    combined_data=pd.DataFrame()
    combined_data.rename(columns={"Weigth": "weight"}, inplace=True)
    
    for file in csv_file:
        csv_data=extract_csv(file)
        combined_data=pd.concat([combined_data,csv_data],ignore_index=True)
        log_progress(f"Extracted data from CSV: {file}")
    
    for file in json_file:
        json_data=extract_json(file)
        combined_data=pd.concat([combined_data,json_data],ignore_index=True)  # note paniko
        log_progress(f"Extracted data from json: {file}")
        
    for file in xml_file:
        xml_data=extract_xml(file)
        combined_data=pd.concat([combined_data,xml_data],ignore_index=True)
        log_progress(f"Extracted data from xml: {file}")


    log_progress("Data extraction completed.")
    return combined_data

extracted_data = extract_data()  # Call to extract data  

extracted_data.to_csv(Output_file,index=False)
log_progress("Transform Data completed.")
        
print(extracted_data)


#***SECTION 2: S3 Extraction***
file_to_download = "Transformed_data.csv"

# Initialize S3 client
S3_Credential=credentials.S3_CREDENTIAL
key_id=S3_Credential["Aws_access_key_id"]
access_key=S3_Credential["Aws_secret_access_key"]


s3_client = boto3.client(
        's3',
        aws_access_key_id=key_id,
        aws_secret_access_key=access_key,
        region_name='ap-south-1'  # Optional, e.g., 'us-east-1'
    )

bucket_name = "my-etl-project-bucket1"
s3_object_name = Output_file.split("\\")[-1]  # Extract file name from the path

#upload Transfered csv file to s3 bucket
# List objects in the bucket
response = s3_client.list_objects_v2(Bucket=bucket_name)

if any(obj['Key'] == s3_object_name for obj in response.get('Contents', [])):
    log_progress(f"The file {s3_object_name} already exists in the S3 bucket. Skipping upload.")
    print(f"The file {s3_object_name} already exists in the S3 bucket. Skipping upload.")
else:
    try:
        
        print(Output_file)
        s3_client.upload_file(Output_file, bucket_name, s3_object_name)
        print(f"File {Output_file} uploaded to {bucket_name}/{s3_object_name} successfully.")
        log_progress(f"File {Output_file} uploaded to {bucket_name}/{s3_object_name} successfully.")

    except Exception as e:
        print(f"Error uploading file: {e}")
        
    
    
    
#Downloading S3 Object
# Define the project folder

Obj_Extracted_to = os.path.join(project_folder, "S3_Extracted")

# Check if the folder exists, if not, create it
if not os.path.exists(Obj_Extracted_to):
    os.makedirs(Obj_Extracted_to)
    print(f"Created folder: {Obj_Extracted_to}")

# Directly download the specific file
S3_Extraceted_file_path = os.path.join(Obj_Extracted_to, file_to_download)

# Ensure subdirectories in S3 key are handled properly
os.makedirs(os.path.dirname(S3_Extraceted_file_path), exist_ok=True)

# Download the file
s3_client.download_file(bucket_name, file_to_download, S3_Extraceted_file_path)
print(f"Downloaded {file_to_download} to {S3_Extraceted_file_path}")

log_progress(f" Object downloaded from S3 bucket {s3_object_name}")

# ***SECTION 3: DMS***

df=pd.read_csv(S3_Extraceted_file_path)

MySql_Credential=credentials.MYSQL_CREDENTIALS

MySql_User=MySql_Credential['user']
MySql_Password=MySql_Credential['password']
MySql_DataBase=MySql_Credential['database']
MySql_Host=MySql_Credential['host']
MySql_Port=MySql_Credential['port']

# creating Engine 
engine=sqlalchemy.create_engine(f"mysql+mysqlconnector://{MySql_User}:{MySql_Password}@{MySql_Host}:{MySql_Port}/{MySql_DataBase}")

#
df.to_sql('Enhanced_ETL', con=engine, if_exists='replace')

log_progress(f" Transformed csv file uploaded to DMS {s3_object_name}")


# In log adding date for better understaing
with open (log_file,"a") as dl:
     # Add a blank line
    dl.write("\n")
    # Add a simple header for separation
    dl.write("***** Logs Ends for 2024-11-29 ***** \n")
    
