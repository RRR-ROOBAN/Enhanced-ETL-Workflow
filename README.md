# Enhanced-ETL-Workflow


ETL Project: Data Extraction, Transformation, and Loading
This project demonstrates a complete ETL (Extract, Transform, Load) process using Python. The main steps include downloading a ZIP file from an S3 bucket, extracting data, transforming it, and then loading the transformed data into both an S3 bucket and a MySQL database.

Project Workflow:
1. Unzip & Extract Data
The script downloads a ZIP file from an S3 URL, extracts its contents (which include CSV, JSON, and XML files), and logs the extraction progress.

2. Extract Data from Multiple File Formats
CSV: Extracts data using pandas.read_csv().
JSON: Extracts data using pandas.read_json().
XML: Extracts data using xml.etree.ElementTree.
3. Data Transformation
Combines all extracted data into a single DataFrame.
Transforms the data (e.g., renaming columns, type conversions).
4. Upload to S3
Uploads the transformed CSV file to an S3 bucket using boto3 and logs the progress.
5. Download from S3
Downloads the transformed CSV file from the S3 bucket to a local folder.
6. Data Migration to MySQL
Uploads the transformed CSV data to a MySQL database using SQLAlchemy.
7. Log Progress
Logs the progress of the ETL process to a file.
Requirements:
Python 3.6+
Pandas
Boto3
SQLAlchemy
Requests
MySQL Connector
To install the required libraries, use the following command:

bash
Copy code
pip install pandas boto3 sqlalchemy requests mysql-connector
Setup:
Credentials:

Add your AWS S3 credentials and MySQL database credentials in the credentials.py file.
Modify Project Paths:

Ensure the paths in the script (e.g., Extracted_data, Transformed_data.csv) match your desired project directory structure.
Run the ETL Process:

Run the script to start the ETL process. The script will:
Download and extract the ZIP file.
Extract data from the files.
Transform the data.
Upload the transformed data to S3 and MySQL.
Log the entire process.
bash
Copy code
python etl_script.py
Log Details:
Logs are stored in Etl_log.txt and include timestamps for tracking the ETL process. The script logs each extraction and upload operation.
