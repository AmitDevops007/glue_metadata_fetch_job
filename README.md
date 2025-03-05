# Glue Metadata Fetch Job

## Project Overview
This AWS Glue job fetches metadata details of all Glue jobs and their corresponding job runs from your AWS account. The data is then stored in an S3 bucket in Parquet format for further analysis.

## Features
- Fetch Glue Job metadata.
- Fetch Glue Job Run details.
- Store data in S3 in Parquet format.
- Automatic schema validation and union of dataframes.
- Handles large paginated responses.

## Prerequisites
- AWS Account with Glue and S3 access.
- IAM Role with the following permissions:
  - glue:GetJobs
  - glue:GetJobRuns
  - s3:PutObject
  - s3:GetObject
  - s3:ListBucket

## Folder Structure
```
.
├── glue_metadata_fetch_job.py     # Glue Job Script
├── README.md                     # Project Documentation
└── requirements.txt              # Dependencies
```

## Configuration
1. Update the S3 bucket path in the script.
```python
s3://glue-job-metadata-file/job_details/
s3://glue-job-metadata-file/job_run_details/
```

2. Ensure your Glue IAM Role has the necessary permissions.

## How to Run the Job
1. Upload the script to an S3 bucket.
2. Create a new Glue job with the script path.
3. Set the IAM role in Glue job settings.
4. Run the job from the AWS Glue console.

## Output
- Job Details: `s3://glue-job-metadata-file/job_details/`
- Job Run Details: `s3://glue-job-metadata-file/job_run_details/`

## Error Handling
- The script will log any schema mismatches during union operations.
- Invalid data types will be cast to strings.
- Timestamp fields will be correctly parsed.



## Author
Amit


