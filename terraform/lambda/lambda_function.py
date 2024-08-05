import json
import boto3
import os

def lambda_handler(event, context):
    glue = boto3.client('glue')
    job_name = os.environ['GLUE_JOB_NAME']
    
    try:
        response = glue.start_job_run(JobName=job_name)
        return {
            'statusCode': 200,
            'body': json.dumps('Glue job started successfully!')
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error starting Glue job: {str(e)}')
        }
