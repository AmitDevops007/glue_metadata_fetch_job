import sys
import boto3
import pandas as pd
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from awsglue.dynamicframe import DynamicFrame

# Initialize Spark Session and Glue Context
spark = SparkSession.builder.appName("GlueMetadataFetch").getOrCreate()
glueContext = GlueContext(spark.sparkContext)
client = boto3.client('glue')

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
        job_spark_df = spark.createDataFrame(job_df)
        job_dynamic_df = DynamicFrame.fromDF(job_spark_df, glueContext, "job_details_df")
        glueContext.write_dynamic_frame.from_options(
            frame=job_dynamic_df,
            connection_type="s3",
            connection_options={"path": "s3://glue-job-metadata-file/job_details/"},
            format="json"
        )
        print("✅ Job Details Successfully Written to S3")

# Setup Crawler
crawler_client = boto3.client('glue')
try:
    crawler_client.create_crawler(
        Name="glue_metadata_crawler",
        Role="glue-metadata-fetch-role",
        DatabaseName="glue_metadata_db",
        Targets={"S3Targets": [{"Path": "s3://glue-job-metadata-file/job_details/"}]}
    )
    print("🚀 Crawler Successfully Created")
except Exception as e:
    print(f"Crawler Already Exists: {e}")

crawler_client.start_crawler(Name="glue_metadata_crawler")
print("🚀 Crawler Successfully Started")

# Fetch Job Run Details
print("Fetching Job Run Details...🔥")
for job in job_list:
    job_name = job['Name']
    job_runs = fetch_job_runs(job_name)
    if job_runs:
        run_df = pd.json_normalize(job_runs)
        if not run_df.empty:
            run_spark_df = spark.createDataFrame(run_df)
            run_dynamic_df = DynamicFrame.fromDF(run_spark_df, glueContext, "job_run_details_df")
            glueContext.write_dynamic_frame.from_options(
                frame=run_dynamic_df,
                connection_type="s3",
                connection_options={"path": f"s3://glue-job-metadata-file/job_run_details/{job_name}/"},
                format="json"
            )
            print(f"✅ Job Run Details for {job_name} Successfully Written to S3")
            try:
                crawler_client.create_crawler(
                    Name=f"glue_metadata_crawler_{job_name}",
                    Role="glue-metadata-fetch-role",
                    DatabaseName="glue_metadata_db",
                    Targets={"S3Targets": [{"Path": f"s3://glue-job-metadata-file/job_run_details/{job_name}/"}]}
                )
                print(f"🚀 Crawler Successfully Created for {job_name}")
            except Exception as e:
                print(f"Crawler Already Exists for {job_name}: {e}")
            crawler_client.start_crawler(Name=f"glue_metadata_crawler_{job_name}")
            print(f"🚀 Crawler Successfully Started for {job_name}")

print("✅ Data Extraction and Table Creation Completed")
