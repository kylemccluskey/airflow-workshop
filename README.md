This repo contains the sample DAG for our Airflow workshop.

In this work we'll use Amazon S3 as a data layer and write a DAG that: 
   1. pulls raw data from S3
   2. validates the raw data
   3. writes validated data to a different S3 location
   4. retrieves validated data from s3
   5. transforms the validated data
   6. writes transformed data back to S3
