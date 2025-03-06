# Glue Metadata Fetch Job

## Project Overview
This AWS Glue job fetches metadata details of all Glue jobs and their corresponding job runs from your AWS account. 
The data is then stored in an S3 bucket in Parquet format for further analysis. The project leverages AWS Glue Crawlers and Athena to enable seamless querying and data exploration.

## Features
- Fetch Glue Job metadata.
- Fetch Glue Job Run details.
- Store data in S3 in Parquet format.
- Automatic schema validation and union of dataframes.
- Handles large paginated responses.
- AWS Glue Crawler for automatic table creation.
- Query data using AWS Athena.

## Prerequisites
- AWS Account with Glue, S3, and Athena access.
- IAM Role 

## Folder Structure
.
├── glue_metadata_fetch_job.py     # Glue Job Script
├── README.md                     # Project Documentation
└── requirements.txt              # Dependencies

## Configuration
1. Update the S3 bucket path in the script.
```python
s3://glue-job-metadata-file/job_details/
s3://glue-job-metadata-file/job_run_details/
```

2. Create a Glue Crawler named `Metadata_job_runs`.
   - Set the data source to the S3 bucket path.
   - Select the database `glue_metadata_db`.

3. Ensure your Glue IAM Role has the necessary permissions.

## How to Run the Job
1. Upload the script to an S3 bucket.
2. Create a new Glue job with the script path.
3. Set the IAM role in Glue job settings.
4. Run the job from the AWS Glue console.
5. Start the Glue Crawler `Metadata_job_runs` to update the tables in the `glue_metadata_db` database.

## Output
- Job Details Table: `glue_metadata_db.job_details`
- Job Run Details Table: `glue_metadata_db.job_run_details`
- S3 Bucket Paths:
  - Job Details: `s3://glue-job-metadata-file/job_details/`
  - Job Run Details: `s3://glue-job-metadata-file/job_run_details/`

## Querying Data with Athena
You can use AWS Athena to query the tables created by the Glue Crawler.
Example Query:
```sql
SELECT * FROM glue_metadata_db.job_details;
SELECT * FROM glue_metadata_db.job_run_details WHERE JobRunState = 'FAILED';
```

## Error Handling
- The script will log any schema mismatches during union operations.
- Invalid data types will be cast to strings.
- Timestamp fields will be correctly parsed.
- Glue Crawler will automatically update tables in case of schema changes.

## Author
Amit

