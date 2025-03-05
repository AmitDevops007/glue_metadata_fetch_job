import sys
import boto3
import pandas as pd
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType

# Initialize Spark Session and Glue Context
spark = SparkSession.builder.appName("GlueMetadataFetch").getOrCreate()
glueContext = GlueContext(spark.sparkContext)
client = boto3.client('glue')

# Define schema for job details
job_schema = StructType([
    StructField("Name", StringType(), True),
    StructField("Role", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("MaxCapacity", DoubleType(), True),
    StructField("CreatedOn", TimestampType(), True),
    StructField("LastModifiedOn", TimestampType(), True),
    StructField("WorkerType", StringType(), True),
    StructField("NumberOfWorkers", IntegerType(), True)
])

# Fetch Glue Job Details
def fetch_jobs():
    jobs = []
    response = client.get_jobs()
    jobs.extend(response['Jobs'])
    while 'NextToken' in response:
        response = client.get_jobs(NextToken=response['NextToken'])
        jobs.extend(response['Jobs'])
    return jobs

# Fetch Glue Job Run Details
def fetch_job_runs(job_name):
    runs = []
    response = client.get_job_runs(JobName=job_name)
    runs.extend(response['JobRuns'])
    while 'NextToken' in response:
        response = client.get_job_runs(JobName=job_name, NextToken=response['NextToken'])
        runs.extend(response['JobRuns'])
    return runs

# Fetch Job Details
print("Fetching Job Details...🔥")
job_list = fetch_jobs()
if job_list:
    job_df = pd.json_normalize(job_list)
    if not job_df.empty:
        for col in job_df.columns:
            if job_df[col].dtype == 'object':
                job_df[col] = job_df[col].astype(str)
        job_spark_df = spark.createDataFrame(job_df)
        job_dynamic_df = DynamicFrame.fromDF(job_spark_df, glueContext, "job_details_df")
        glueContext.write_dynamic_frame.from_options(
            frame=job_dynamic_df,
            connection_type="s3",
            connection_options={"path": "s3://glue-job-metadata-file/job_details/"},
            format="json"
        )
        print("✅ Job Details Successfully Written to S3")

# Fetch Job Run Details
print("Fetching Job Run Details...🔥")
for job in job_list:
    job_name = job['Name']
    job_runs = fetch_job_runs(job_name)
    if job_runs:
        run_df = pd.json_normalize(job_runs)
        if not run_df.empty:
            for col in run_df.columns:
                if run_df[col].dtype == 'object':
                    run_df[col] = run_df[col].astype(str)
            run_spark_df = spark.createDataFrame(run_df)
            run_dynamic_df = DynamicFrame.fromDF(run_spark_df, glueContext, "job_run_details_df")
            glueContext.write_dynamic_frame.from_options(
                frame=run_dynamic_df,
                connection_type="s3",
                connection_options={"path": f"s3://glue-job-metadata-file/job_run_details/{job_name}/"},
                format="json"
            )
            print(f"✅ Job Run Details for {job_name} Successfully Written to S3")

print("✅ Data Extraction Completed")
