import csv
import boto3
import json
import logging
import os
import sys
from yaml import safe_load
from datetime import datetime
from psycopg2 import connect, ProgrammingError, OperationalError

# Code for importing custom modules
file_dir = os.path.dirname(__file__)
sys.path.append(file_dir)
import connections

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Load database configuration
config = connections.load_config(os.getenv("SPRING_PROFILES_ACTIVE"))
db_config = config["database"]
db_port = db_config["DB_PORT"]
db_user = db_config["DB_USER"]
db_host = db_config["DB_HOST"]

today_date = datetime.now().strftime('%Y-%m-%d')
query1 = f"""
    COPY (
        SELECT o.mntn_txn_rcrd_id, s.src_of_fund_acct_nb, s.src_of_fund_chck_nb,
               s.src_of_fund_dep_chck_am, d.acct_nb, d.dep_chck_am, MIN(o.cre_ts)
        FROM transactions o
        JOIN sources s ON o.src_id = s.id
        JOIN destinations d ON o.dest_id = d.id
        GROUP BY o.mntn_txn_rcrd_id, s.src_of_fund_acct_nb, s.src_of_fund_chck_nb,
                 s.src_of_fund_dep_chck_am, d.acct_nb, d.dep_chck_am
    ) TO STDOUT WITH CSV HEADER
"""

query2 = "COPY (SELECT * FROM public.data) TO STDOUT WITH CSV HEADER"

def get_connection():
    try:
        return connect(
            dbname=db_config["DB_NAME"],
            user=db_user,
            password=db_config["DB_PASSWORD"],
            host=db_host,
            port=db_port
        )
    except (ProgrammingError, OperationalError) as e:
        logger.error(f"Database connection error: {e}")
        raise

def execute(event, context):
    s3_bucket = os.getenv("BUCKET_NAME")
    today_folder = datetime.now().strftime('%Y-%m-%d')
    s3_key1 = f"{today_folder}/timeout_records_{today_folder}.csv"
    s3_key2 = f"{today_folder}/simple_timeout_records_{today_folder}.csv"
    csv_file_path1 = f"/tmp/timeout_records_{today_folder}.csv"
    csv_file_path2 = f"/tmp/simple_timeout_records_{today_folder}.csv"
    
    print(f"Query1: {query1}")
    print(f"Query2: {query2}")
    
    connection = get_connection()
    
    try:
        cursor = connection.cursor()
        print("Connected to DB.")

        # Write results of first query to a CSV file
        print("Executing first copy command...")
        with open(csv_file_path1, 'w') as f:
            cursor.copy_expert(query1, f)
        print(f"File successfully created at {csv_file_path1}")
        
        # Write results of second query to a CSV file
        print("Executing second copy command...")
        with open(csv_file_path2, 'w') as f:
            cursor.copy_expert(query2, f)
        print(f"File successfully created at {csv_file_path2}")

        if not s3_bucket:
            raise ValueError("s3_bucket environment variable is missing!")
        
        s3 = boto3.client('s3')
        try:
            s3.upload_file(csv_file_path1, s3_bucket, s3_key1)
            print(f"Upload Success: {s3_key1} in bucket {s3_bucket}")
            s3.upload_file(csv_file_path2, s3_bucket, s3_key2)
            print(f"Upload Success: {s3_key2} in bucket {s3_bucket}")
        except Exception as s3_error:
            print(f"Error uploading into S3: {s3_error}")

        # Log success and return response
        print(f"Files successfully uploaded to bucket {s3_bucket}.")
        return {
            "statusCode": 200,
            "body": f"Files successfully uploaded to bucket {s3_bucket}."
        }
    
    except Exception as e:
        logger.error(e.args)
    
    finally:
        connection.close()
