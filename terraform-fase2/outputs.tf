output "bucket_name" {
  value = aws_s3_bucket.bovespa_bucket.bucket
}

output "lambda_function_name" {
  value = aws_lambda_function.trigger_glue_job.function_name
}

output "glue_job_name_input" {
  value = aws_glue_job.bovespa_glue_job_input.name
}

output "glue_job_name_output" {
  value = aws_glue_job.bovespa_glue_job_output.name
}
