from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import io

#sample code for data transformation

YOUR_DAG_NAME = 'simple_example_dag'

def print_hello():
    print("Hello from a Python function!")

def print_goodbye():
    print("Goodbye from a Python function!")


def get_data_from_s3(bucket_name, key, aws_conn_id='aws_default'):
    """Retrieves the content of an S3 object."""
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    try:
        # Get the object content as a string
        object_content = s3_hook.read_key(key=key, bucket_name=bucket_name)
        print(f"Content of s3://{bucket_name}/{key}:\n{object_content}")
        return object_content
    except Exception as e:
        print(f"Error retrieving S3 object: {e}")
        raise

def validate_for_silver() :
    raw_data = get_data_from_s3('0926-airflow-workshop', 'data/raw/sample_data.csv')
    df = pd.read_csv(io.StringIO(raw_data), sep='\t')
    print("DataFrame is:" + str(df.head()))
    df['inspection_date'] = pd.to_datetime(df['inspection_date'])
    #df['grade_date'] = pd.to_datetime(df['grade_date'])
    
    df['inspected'] = df['inspection_date'].apply(lambda x: False if x == pd.to_datetime('1900-01-01') else True)

    upload_to_s3_callable(df, 'data/silver/validated_data.csv', '0926-airflow-workshop', 'aws_default')



    #expected_columns = {'camis', 'dba', 'boro', 'building', 'street', 'zipcode', 'phone', 'cuisine_description', 'inspection_date', 'action', 'score', 'grade', 'grade_date', 'record_date', 'inspection_type', 'latitude

def transform_data():
    """
    Transforms the input DataFrame by filtering rows where 'inspected' is True
    and creating a new DataFrame with only those rows.

    Args:
        df (pd.DataFrame): Input DataFrame with an 'inspected' column.

    Returns:
        pd.DataFrame: Transformed DataFrame containing only inspected rows.
    """
    
    valid_data = get_data_from_s3('0926-airflow-workshop', 'data/silver/validated_data.csv')
    df = pd.read_csv(io.StringIO(valid_data), sep='\t')
    print("DataFrame is:" + str(df.head()))

    # Filter rows where 'inspected' is False
    #gold_df = df[df['inspected'] == False].copy()
    
    # print(f"Restaurants with critical violations: {gold_df['has_critical_violations'].sum()}")
    # gold_df = gold_df.groupby('boro').agg({
    #     'dba': 'count',  # Total restaurants (using any non-null column for count)
    #     'has_critical_violations': ['sum', 'mean'],  # Count and percentage of critical violations
    #     'score': 'mean'  # Average score
    # }).round(3)
    # gold_df.columns = ['total_restaurants', 'critical_violations', 'pct_critical_violations', 'avg_score']
    # gold_df = gold_df.sort_values('pct_critical_violations', ascending=False)
    # upload_to_s3_callable(gold_df, 'data/gold/gold_data.csv', '0926-airflow-workshop', 'aws_default')

def upload_to_s3_callable(df, key, bucket_name, aws_conn_id):
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    
    # Convert DataFrame to CSV in-memory
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_content = csv_buffer.getvalue()

    s3_hook.load_string(
        string_data=csv_content,
        key=key,
        bucket_name=bucket_name,
        replace=True # Set to True to overwrite if file exists
    )
    print(f"Data frame uploaded to s3://{bucket_name}/{key}")


with DAG(
    dag_id=YOUR_DAG_NAME,
    start_date=datetime(2025, 9, 1),
    schedule_interval=None,
    catchup=False,
    tags=['example'],
) as dag:

    # Task 1: Validations for silver data
    silver_data_task = PythonOperator(
        task_id='validate-silver-data',
        python_callable=validate_for_silver,        
    )

    # Task 2: Transform data for gold
    gold_data_task = PythonOperator(
        task_id='transform-data-for-gold',
        python_callable=transform_data,
    )

    # Define task dependencies
    silver_data_task >> gold_data_task
