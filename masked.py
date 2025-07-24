import os
import logging
import boto3
from psycopg2 import connect
from datetime import datetime

# Get base directory
file_dir = os.path.dirname(__file__)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Load configuration (from env + resource YAML)
import connections
config = connections.load_config(os.getenv('SPRING_PROFILES_ACTIVE'))
db_config = config["database"]
DB_PORT = db_config["DB_PORT"]
DB_USER = db_config["DB_USER"]
DB_HOST = db_config["DB_HOST"]
DB_NAME = db_config["DB_NAME"]

def get_token():
    try:
        client = boto3.client('rds')
        return client.generate_db_auth_token(
            DBHostname=DB_HOST,
            Port=DB_PORT,
            DBUsername=DB_USER
        )
    except Exception as exc:
        logger.error("Creating token failed")
        logger.error(exc)
        raise

def get_connection():
    try:
        connection = connect(
            host=DB_HOST,
            user=DB_USER,
            password=get_token(),
            port=DB_PORT,
            dbname=DB_NAME
        )
        logger.info("Connected to database")
        connection.autocommit = True
        return connection
    except Exception as err:
        logger.error(f"Connection error: {err}")
        return None

def ensure_masked_schema(connection):
    try:
        cursor = connection.cursor()
        cursor.execute("CREATE SCHEMA IF NOT EXISTS masked;")
        logger.info("Ensured 'masked' schema exists.")
    except Exception as e:
        logger.error(f"Failed to ensure 'masked' schema: {e}")
        raise

def load_sql_template(schema_name: str, table_name: str) -> str:
    path = os.path.join(file_dir, 'resources', schema_name, f"{table_name}_mask.sql")
    if not os.path.exists(path):
        raise FileNotFoundError(f"SQL template not found: {path}")
    with open(path, 'r') as f:
        return f.read()

def create_masked_table(schema_name: str, partition: str, table_name: str, connection):
    try:
        sql_template = load_sql_template(schema_name, table_name)
        sql_filled = sql_template \
            .replace("$(schema_name_from_json_input)", schema_name) \
            .replace("$(partition_name_from_json_input)", partition)

        cursor = connection.cursor()
        cursor.execute(sql_filled)
        logger.info(f"Created masked table: masked.{table_name}_{partition}_masked")
    except Exception as e:
        logger.error(f"Error processing table {table_name}: {e}")
        raise

def execute(event, context=None):
    schema_name = event.get("schema_name")
    partition = event.get("partition_name")

    if not schema_name or not partition:
        logger.error("Missing schema_name or partition_name in input")
        return {"statusCode": 400, "body": "Missing schema_name or partition_name"}

    connection = get_connection()
    if not connection:
        return {"statusCode": 500, "body": "DB connection failed"}

    try:
        ensure_masked_schema(connection)

        schema_dir = os.path.join(file_dir, "resources", schema_name)
        if not os.path.isdir(schema_dir):
            return {"statusCode": 404, "body": f"Schema folder not found: {schema_name}"}

        table_files = [f for f in os.listdir(schema_dir) if f.endswith('_mask.sql')]
        table_names = [f.replace('_mask.sql', '') for f in table_files]

        for table in table_names:
            create_masked_table(schema_name, partition, table, connection)

        return {
            "statusCode": 200,
            "body": f"Masked tables created in schema '{schema_name}' for partition '{partition}'"
        }

    except Exception as err:
        logger.error(f"Unexpected error: {err}")
        return {"statusCode": 500, "body": "Internal error during masking process"}

    finally:
        connection.close()
