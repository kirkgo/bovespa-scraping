output "bucket_name" {
  value = aws_s3_bucket.bovespa_bucket.bucket
}

output "lambda_function_name" {
  value = aws_lambda_function.trigger_glue_job.function_name
}

output "glue_job_name" {
  value = aws_glue_job.bovespa_glue_job.name
}
