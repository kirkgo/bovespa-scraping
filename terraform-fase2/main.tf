# Configurar o provedor AWS
provider "aws" {
  region = var.region
}

# Obter informações sobre a conta AWS atual
data "aws_caller_identity" "current" {}

# Configurar o bucket S3
resource "aws_s3_bucket" "bovespa_bucket" {
  bucket = var.bucket_name

  tags = {
    Name        = var.bucket_name
    Environment = "Dev"
  }
}

# Habilitar versionamento no bucket
resource "aws_s3_bucket_versioning" "bovespa_bucket_versioning" {
  bucket = aws_s3_bucket.bovespa_bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Configurar a propriedade do objeto (Object Ownership)
resource "aws_s3_bucket_ownership_controls" "bovespa_bucket_ownership" {
  bucket = aws_s3_bucket.bovespa_bucket.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

# Configurar a política de bucket S3
resource "aws_s3_bucket_policy" "bovespa_bucket_policy" {
  bucket = aws_s3_bucket.bovespa_bucket.id

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Sid       = "AllowCurrentAccountAccess",
        Effect    = "Allow",
        Principal = {
          AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"
        },
        Action = [
          "s3:GetObject",
          "s3:PutObject"
        ],
        Resource = "${aws_s3_bucket.bovespa_bucket.arn}/*"
      }
    ]
  })
}

# Carregar o script para o S3
resource "aws_s3_object" "glue_script" {
  bucket = aws_s3_bucket.bovespa_bucket.bucket
  key    = var.glue_script_s3_key
  source = var.glue_script_path
}

# Definir a notificação do S3 para invocar a Lambda
resource "aws_s3_bucket_notification" "bovespa_bucket_notification" {
  bucket = aws_s3_bucket.bovespa_bucket.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.trigger_glue_job.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "raw/"
  }

  depends_on = [aws_lambda_permission.allow_s3_invoke]
}

# Permissão para S3 invocar a Lambda
resource "aws_lambda_permission" "allow_s3_invoke" {
  statement_id  = "AllowS3Invoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.trigger_glue_job.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.bovespa_bucket.arn
}

# Definição da função Lambda
resource "aws_lambda_function" "trigger_glue_job" {
  filename         = var.lambda_function_zip_path
  function_name    = "trigger_glue_job"
  role             = aws_iam_role.lambda_execution_role.arn
  handler          = "lambda_function.lambda_handler"
  source_code_hash = filebase64sha256(var.lambda_function_zip_path)
  runtime          = "python3.8"
  environment {
    variables = {
      GLUE_JOB_NAME = var.glue_job_name
    }
  }
}

# Role IAM para a Lambda
resource "aws_iam_role" "lambda_execution_role" {
  name = "lambda_execution_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action    = "sts:AssumeRole",
        Effect    = "Allow",
        Principal = {
          Service = "lambda.amazonaws.com",
        },
      },
    ],
  })

  inline_policy {
    name   = "lambda_glue_policy"
    policy = jsonencode({
      Version = "2012-10-17",
      Statement = [
        {
          Action = [
            "logs:CreateLogGroup",
            "logs:CreateLogStream",
            "logs:PutLogEvents",
          ],
          Effect   = "Allow",
          Resource = "arn:aws:logs:*:*:*",
        },
        {
          Action = "glue:StartJobRun",
          Effect = "Allow",
          Resource = "*",
        },
        {
          Action = [
            "s3:GetObject",
            "s3:ListBucket"
          ],
          Effect   = "Allow",
          Resource = [
            "arn:aws:s3:::${aws_s3_bucket.bovespa_bucket.bucket}",
            "arn:aws:s3:::${aws_s3_bucket.bovespa_bucket.bucket}/*"
          ]
        },
        {
          Action = [
            "glue:GetTable",
            "glue:CreateTable",
            "glue:UpdateTable",
            "glue:GetDatabase",
            "glue:CreateDatabase",
            "glue:UpdateDatabase"
          ],
          Effect   = "Allow",
          Resource = "*"
        }
      ],
    })
  }
}

# Role IAM para o Glue
resource "aws_iam_role" "glue_role" {
  name = "glue_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect    = "Allow",
        Principal = {
          Service = "glue.amazonaws.com"
        },
        Action    = "sts:AssumeRole"
      }
    ]
  })

  inline_policy {
    name   = "glue_policy"
    policy = jsonencode({
      Version = "2012-10-17",
      Statement = [
        {
          Effect   = "Allow",
          Action   = [
            "s3:ListBucket",
            "s3:PutObject",
            "s3:GetObject",
            "s3:DeleteObject"
          ],
          Resource = [
            "arn:aws:s3:::${aws_s3_bucket.bovespa_bucket.bucket}",
            "arn:aws:s3:::${aws_s3_bucket.bovespa_bucket.bucket}/*"
          ]
        },
        {
          Effect   = "Allow",
          Action   = [
            "logs:CreateLogGroup",
            "logs:CreateLogStream",
            "logs:PutLogEvents"
          ],
          Resource = "arn:aws:logs:*:*:*"
        },
        {
          Effect   = "Allow",
          Action   = [
            "glue:GetTable",
            "glue:CreateTable",
            "glue:UpdateTable",
            "glue:GetDatabase",
            "glue:CreateDatabase",
            "glue:UpdateDatabase"
          ],
          Resource = "*"
        }
      ]
    })
  }
}

# Glue job
resource "aws_glue_job" "bovespa_job" {
  name     = var.glue_job_name
  role_arn = aws_iam_role.glue_role.arn
  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.bovespa_bucket.bucket}/${var.glue_script_s3_key}"
    python_version  = "3"
  }
  default_arguments = {
    "--job-language" = "python"
    "--TempDir"      = "s3://${aws_s3_bucket.bovespa_bucket.bucket}/temp/"
    "--additional-python-modules" = "pandas,pyarrow"
  }
  max_capacity = 10
  glue_version = "4.0"
}
