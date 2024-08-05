variable "glue_job_name" {
  description = "The name of the Glue Job to trigger"
  type        = string
}

variable "bucket_name" {
  description = "The name of the S3 bucket"
  type        = string
  default     = "bovespa-bucket"
}

variable "lambda_function_zip_path" {
  description = "Path to the lambda function zip file"
  type        = string
  default     = "lambda/lambda_function.zip"
}

variable "glue_script_path" {
  description = "Path to the Glue script"
  type        = string
  default     = "scripts/bovespa_aggregation.py"
}

variable "glue_script_s3_key" {
  description = "S3 key for the Glue script"
  type        = string
  default     = "scripts/bovespa_aggregation.py"
}

variable "region" {
  description = "AWS Region"
  type        = string
  default     = "us-east-1"
}
