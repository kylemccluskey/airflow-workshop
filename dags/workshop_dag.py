from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

#sample code for data transformation

YOUR_DAG_NAME = 'simple_example_dag'

def print_hello():
    print("Hello from a Python function!")

def print_goodbye():
    print("Goodbye from a Python function!")


def get_s3_object_content(bucket_name, key, aws_conn_id='aws_default'):
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

def transform_data(df: pd.DataFrame, outlier_threshold: float) -> pd.DataFrame:
    """
    Transforms the input DataFrame by filtering rows where 'inspected' is True
    and creating a new DataFrame with only those rows.

    Args:
        df (pd.DataFrame): Input DataFrame with an 'inspected' column.

    Returns:
        pd.DataFrame: Transformed DataFrame containing only inspected rows.
    """
    # Filter rows where 'inspected' is True
    gold_df = df[df['inspected'] == True].copy()
    gold_df['has_critical_violations'] = gold_df['score'] > outlier_threshold
    print(f"Restaurants with critical violations: {gold_df['has_critical_violations'].sum()}")
    gold_df = gold_df.groupby('boro').agg({
        'dba': 'count',  # Total restaurants (using any non-null column for count)
        'has_critical_violations': ['sum', 'mean'],  # Count and percentage of critical violations
        'score': 'mean'  # Average score
    }).round(3)
    gold_df.columns = ['total_restaurants', 'critical_violations', 'pct_critical_violations', 'avg_score']
    gold_df = gold_df.sort_values('pct_critical_violations', ascending=False)
    return gold_df



with DAG(
    dag_id=YOUR_DAG_NAME,
    start_date=datetime(2025, 9, 1),
    schedule_interval=None,
    catchup=False,
    tags=['example'],
) as dag:

    # Task 2: Call a Python function
    raw_data_task = PythonOperator(
        task_id='get-raw-data-from-s3',
        python_callable=get_s3_object_content,
        op_kwargs={
            'bucket_name': '0926-airflow-workshop',
            'key': 'data/raw/sample_data.csv',
        },
    )

    # Task 4: Call another Python function
    another_python_task = PythonOperator(
        task_id='execute_data_transformation',
        python_callable=transform_data,
    )


    # Define task dependencies
    raw_data_task >> another_python_task
