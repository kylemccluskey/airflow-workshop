from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import io

#sample code for data transformation

#Put a unique dag name here, e.g. 'my_example_dag'
#YOUR_DAG_NAME = 'empty_dag'

# Function to retrieve a dataframe from S3
def get_data_from_s3(bucket_name, key, aws_conn_id='aws_default') -> pd.DataFrame:
    """Retrieves the content of an S3 object."""
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    try:
        # Get the object content as a string
        object_content = s3_hook.read_key(key=key, bucket_name=bucket_name)
        print(f"Content of s3://{bucket_name}/{key}:\n{object_content}")
        df = pd.read_csv(io.StringIO(object_content), sep='\t')
        return df
    except Exception as e:
        print(f"Error retrieving S3 object: {e}")
        raise

# Function to upload DataFrame to S3
def put_data_in_s3(df, key, bucket_name, aws_conn_id):
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    
    # Convert DataFrame to CSV in-memory
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, sep='\t', index=False)
    csv_content = csv_buffer.getvalue()

    s3_hook.load_string(
        string_data=csv_content,
        key=key,
        bucket_name=bucket_name,
        replace=True # Set to True to overwrite if file exists
    )
    print(f"Data frame uploaded to s3://{bucket_name}/{key}")

# Function to validate the DataFrame
def validate(df: pd.DataFrame) -> pd.DataFrame:
    
    print("DataFrame is:" + str(df.head()))

    df['inspection_date'] = pd.to_datetime(df['inspection_date'])    
    df['inspected'] = df['inspection_date'].apply(lambda x: False if x == pd.to_datetime('1900-01-01') else True)

    return df
    
# function to transform the DataFrame
def transform(df: pd.DataFrame) -> pd.DataFrame:
    
    print("DataFrame is:" + str(df.head()))

    # Filter rows where 'inspected' is False
    gold_df = df[df['inspected'] == False].copy()
    
    gold_df = gold_df.groupby('boro').agg({
        'dba': 'count',  # Total restaurants (using any non-null column for count)
        
    }).round(3)
    gold_df.columns = ['total_restaurants']
    gold_df = gold_df.sort_values('total_restaurants', ascending=False)
    return gold_df


# create a function here that does three things
# 1. gets the rawsample_data.csv data from the raw folder in s3
# 2. validates the data
# 3. puts the validated data back to s3 into the silver folder

# def function_for_silver_task():
#    function body here 

# create a second function here that does three things
# 1. gets the validated data from the silver folder in s3
# 2. transforms the data
# 3. puts the transformed data back to s3 into the gold folder

# def function_for_gold_task():
#    function body here


with DAG(
    dag_id='', #YOUR_DAG_NAME
    start_date=datetime(2025, 9, 1),
    schedule_interval=None,
    catchup=False,
    tags=['airflow-workshop'], #add any tags here you want associated with you DAG in the Airflow UI
) as dag:

    # Task 1: Validations for silver data
    # create a task here that uses the PythonOperator to call the function you created for silver data
    # name the task whatever you want, e.g. 'validate-silver-data'
    # give the task a unique task_id, e.g. 'validate-silver-data'
    # call the function you created for silver data in the python_callable parameter

    # silver_data_task = PythonOperator(
    #     task_id='',
    #     python_callable=,        
    # )

    # Task 2: Transform data for gold
    # create a task here that uses the PythonOperator to call the function you created for gold data
    # name the task whatever you want, e.g. 'transform-gold-data'
    # give the task a unique task_id, e.g. 'transform-gold-data'
    # call the function you created for gold data in the python_callable parameter
    # gold_data_task = PythonOperator(
    #     task_id='',
    #     python_callable=,
    # )

    # Define task dependencies
    # 
    # Chain tasks together to set the order of execution for the DAG
    # using the bitshift operator (>>). For example:
    # e.g.
    # silver_data_task >> gold_data_task

    # Once you have created the tasks and set the dependencies, your DAG should be complete.
    # Upload the file to the dags folder in S3 in our AWS environment and test it out!
