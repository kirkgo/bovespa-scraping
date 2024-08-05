# Output para o nome do bucket S3
output "bucket_name" {
  value = aws_s3_bucket.bovespa_bucket.bucket
}

# Output para o nome do Glue job
output "glue_job_name" {
  value = aws_glue_job.bovespa_job.name
}

# Output para o ARN da função Lambda
output "lambda_function_arn" {
  value = aws_lambda_function.trigger_glue_job.arn
}

# Output para o nome da função Lambda
output "lambda_function_name" {
  value = aws_lambda_function.trigger_glue_job.function_name
}

# Output para a role da Lambda
output "lambda_execution_role_arn" {
  value = aws_iam_role.lambda_execution_role.arn
}

# Output para a role do Glue
output "glue_role_arn" {
  value = aws_iam_role.glue_role.arn
}
