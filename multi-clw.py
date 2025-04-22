import os
import psycopg2
import boto3

def lambda_handler(event, context):
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(
            host=os.environ['DB_HOST'],
            port=os.environ['DB_PORT'],
            user=os.environ['DB_USER'],
            password=os.environ['DB_PASS'],
            dbname=os.environ['DB_NAME']
        )
        cursor = conn.cursor()

        # Define queries with expected column names
        queries = {
            'Query1': {
                'sql': "SELECT COUNT(*) AS counts, SUM(amount) AS amount FROM users;",
                'columns': ['counts', 'amount']
            },
            'Query2': {
                'sql': "SELECT AVG(user_count) AS avg_user, AVG(amount) AS avg_amount FROM daily_stats;",
                'columns': ['avg_user', 'avg_amount']
            }
        }

        metric_data = []
        for query_name, query_info in queries.items():
            cursor.execute(query_info['sql'])
            row = cursor.fetchone()
            for col_name, value in zip(query_info['columns'], row):
                metric_data.append({
                    'MetricName': col_name,
                    'Dimensions': [{'Name': 'Query', 'Value': query_name}],
                    'Value': value,
                    'Unit': 'None'  # Use 'Count' or 'Bytes' if specific
                })

        cloudwatch = boto3.client('cloudwatch')
        cloudwatch.put_metric_data(
            Namespace='MyApp/DBMetrics',
            MetricData=metric_data
        )

    except Exception as e:
        print(f"Error: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
