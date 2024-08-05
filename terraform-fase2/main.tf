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

# Upload do script de agregação para o bucket S3
resource "aws_s3_object" "bovespa_aggregation_script" {
  bucket = aws_s3_bucket.bovespa_bucket.bucket
  key    = var.glue_script_s3_key
  source = var.glue_script_path
}

# Notificação do bucket S3 para Lambda
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
            "s3:GetObject"
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


# provider "aws" {
#   region = "us-east-1"
# }

# # Criar o bucket S3
# resource "aws_s3_bucket" "bovespa_bucket" {
#   bucket = "my-bovespa-bucket"
#   acl    = "private"

#   versioning {
#     enabled = true
#   }
# }

# # Configuração de versionamento do S3
# resource "aws_s3_bucket_versioning" "bovespa_bucket_versioning" {
#   bucket = aws_s3_bucket.bovespa_bucket.id

#   versioning_configuration {
#     status = "Enabled"
#   }
# }

# # Carregar o arquivo glue_script.py para o S3
# resource "aws_s3_object" "glue_script" {
#   bucket = aws_s3_bucket.bovespa_bucket.bucket
#   key    = "scripts/glue_script.py"
#   source = "${path.module}/scripts/glue_script.py"
#   acl    = "private"
# }

# # Banco de dados Glue
# resource "aws_glue_catalog_database" "bovespa_db" {
#   name = "bovespa_db"
# }

# # Tabela Glue para dados de entrada
# resource "aws_glue_catalog_table" "bovespa_table" {
#   database_name = aws_glue_catalog_database.bovespa_db.name
#   name          = "bovespa_input_table"
#   table_type    = "EXTERNAL_TABLE"

#   storage_descriptor {
#     location      = "s3://${aws_s3_bucket.bovespa_bucket.bucket}/raw/"
#     input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
#     output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
#     ser_de_info {
#       serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
#     }

#     columns {
#       name = "Código"
#       type = "string"
#     }
#     columns {
#       name = "Ação"
#       type = "string"
#     }
#     columns {
#       name = "Tipo"
#       type = "string"
#     }
#     columns {
#       name = "Qtde_Teorica"
#       type = "double"
#     }
#     columns {
#       name = "Part_Perc"
#       type = "double"
#     }
#   }
# }

# # Tabela Glue para dados de saída
# resource "aws_glue_catalog_table" "bovespa_output_table" {
#   database_name = aws_glue_catalog_database.bovespa_db.name
#   name          = "bovespa_output_table"
#   table_type    = "EXTERNAL_TABLE"

#   storage_descriptor {
#     location      = "s3://${aws_s3_bucket.bovespa_bucket.bucket}/refined/"
#     input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
#     output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
#     ser_de_info {
#       serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
#     }

#     columns {
#       name = "CodigoRenomeado"
#       type = "string"
#     }
#     columns {
#       name = "AcaoRenomeada"
#       type = "string"
#     }
#     columns {
#       name = "Qtde_Teorica_Total"
#       type = "double"
#     }
#     columns {
#       name = "Part_Perc_Total"
#       type = "double"
#     }
#     columns {
#       name = "date_partition"
#       type = "string"
#     }
#     columns {
#       name = "symbol"
#       type = "string"
#     }
#   }

#   partition_keys {
#     name = "date_partition"
#     type = "string"
#   }
#   partition_keys {
#     name = "symbol"
#     type = "string"
#   }
# }

# # Função Lambda
# resource "aws_lambda_function" "trigger_glue_job" {
#   filename         = "${path.module}/lambda/glue_trigger.zip"
#   function_name    = "trigger_glue_job"
#   role             = aws_iam_role.lambda_exec_role.arn
#   handler          = "glue_trigger.lambda_handler"
#   runtime          = "python3.9"
#   source_code_hash = filebase64sha256("${path.module}/lambda/glue_trigger.zip")
#   environment {
#     variables = {
#       GLUE_JOB_NAME = aws_glue_job.bovespa_glue_job.name
#     }
#   }
# }

# # Permissões para a Lambda acionar o Glue Job
# resource "aws_iam_role_policy" "lambda_policy" {
#   role = aws_iam_role.lambda_exec_role.id
#   policy = jsonencode({
#     Version = "2012-10-17",
#     Statement = [
#       {
#         Effect = "Allow",
#         Action = [
#           "glue:StartJobRun",
#           "s3:GetObject",
#           "s3:ListBucket"
#         ],
#         Resource = "*"
#       }
#     ]
#   })
# }

# # Definir a role para a Lambda
# resource "aws_iam_role" "lambda_exec_role" {
#   name = "lambda_exec_role"
#   assume_role_policy = jsonencode({
#     Version = "2012-10-17",
#     Statement = [
#       {
#         Action = "sts:AssumeRole",
#         Effect = "Allow",
#         Sid    = "",
#         Principal = {
#           Service = "lambda.amazonaws.com"
#         }
#       }
#     ]
#   })
# }

# # Anexar política de execução para a role da Lambda
# resource "aws_iam_role_policy_attachment" "lambda_exec_attach" {
#   role       = aws_iam_role.lambda_exec_role.name
#   policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
# }

# # Configurar o evento do bucket S3 para acionar a Lambda
# resource "aws_s3_bucket_notification" "bovespa_bucket_notification" {
#   bucket = aws_s3_bucket.bovespa_bucket.id

#   lambda_function {
#     lambda_function_arn = aws_lambda_function.trigger_glue_job.arn
#     events              = ["s3:ObjectCreated:*"]
#   }
# }

# # Permissão para que o bucket S3 acione a Lambda
# resource "aws_lambda_permission" "allow_s3_to_call_lambda" {
#   statement_id  = "AllowS3Invoke"
#   action        = "lambda:InvokeFunction"
#   function_name = aws_lambda_function.trigger_glue_job.function_name
#   principal     = "s3.amazonaws.com"
#   source_arn    = "arn:aws:s3:::${aws_s3_bucket.bovespa_bucket.bucket}"
# }

# # Job Glue
# resource "aws_glue_job" "bovespa_glue_job" {
#   name     = "bovespa-glue-job"
#   role_arn = aws_iam_role.glue_exec_role.arn

#   command {
#     script_location = "s3://${aws_s3_object.glue_script.bucket}/${aws_s3_object.glue_script.key}"
#     python_version  = "3"
#   }

#   default_arguments = {
#     "--job-language" = "python"
#     "--TempDir"      = "s3://${aws_s3_bucket.bovespa_bucket.bucket}/temp/"
#     "--additional-python-modules" = "pandas,pyarrow"
#   }

#   glue_version = "4.0"
# }

# # Permissões para o Glue Job
# resource "aws_iam_role" "glue_exec_role" {
#   name = "glue_exec_role"
#   assume_role_policy = jsonencode({
#     Version = "2012-10-17",
#     Statement = [
#       {
#         Action = "sts:AssumeRole",
#         Effect = "Allow",
#         Sid    = "",
#         Principal = {
#           Service = "glue.amazonaws.com"
#         }
#       }
#     ]
#   })
# }

# resource "aws_iam_role_policy_attachment" "glue_exec_attach" {
#   role       = aws_iam_role.glue_exec_role.name
#   policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
# }

# resource "aws_iam_role_policy" "glue_s3_access" {
#   name = "glue_s3_access_policy"
#   role = aws_iam_role.glue_exec_role.id

#   policy = jsonencode({
#     Version = "2012-10-17",
#     Statement = [
#       {
#         Effect = "Allow",
#         Action = [
#           "s3:GetObject",
#           "s3:PutObject",
#           "s3:DeleteObject",
#           "s3:ListBucket"
#         ],
#         Resource = [
#           "arn:aws:s3:::${aws_s3_bucket.bovespa_bucket.bucket}",
#           "arn:aws:s3:::${aws_s3_bucket.bovespa_bucket.bucket}/*"
#         ]
#       },
#       {
#         Effect = "Allow",
#         Action = [
#           "glue:GetTable",
#           "glue:UpdateTable",
#           "glue:CreateTable",
#           "glue:DeleteTable",
#           "glue:GetPartitions",
#           "glue:GetDatabase",
#           "glue:CreateDatabase",
#           "glue:DeleteDatabase"
#         ],
#         Resource = "*"
#       }
#     ]
#   })
# }

# # Adicionar CloudWatch Logs para monitoramento
# resource "aws_cloudwatch_log_group" "glue_log_group" {
#   name              = "/aws-glue/jobs/${aws_glue_job.bovespa_glue_job.name}"
#   retention_in_days = 14
# }

# resource "aws_iam_role_policy_attachment" "glue_cloudwatch_logs" {
#   policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceNotebookRole"
#   role       = aws_iam_role.glue_exec_role.name
# }
