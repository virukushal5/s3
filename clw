import os
import psycopg2
import boto3

def lambda_handler(event, context):
    conn = psycopg2.connect(
        host=os.environ['DB_HOST'],
        port=os.environ['DB_PORT'],
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASS'],
        dbname=os.environ['DB_NAME']
    )
    cursor = conn.cursor()

    queries = {
        'TotalUsers': "SELECT count(*) FROM users;",
        'PendingOrders': "SELECT count(*) FROM orders WHERE status = 'pending';",
        'RecentPayments': "SELECT count(*) FROM payments WHERE created_at >= now() - interval '1 day';"
    }

    metric_data = []
    for name, query in queries.items():
        cursor.execute(query)
        result = cursor.fetchone()[0]
        metric_data.append({
            'MetricName': name,
            'Dimensions': [{'Name': 'App', 'Value': 'MyService'}],
            'Value': result,
            'Unit': 'Count'
        })

    cloudwatch = boto3.client('cloudwatch')
    cloudwatch.put_metric_data(
        Namespace='MyApp/Metrics',
        MetricData=metric_data
    )

    cursor.close()
    conn.close()

