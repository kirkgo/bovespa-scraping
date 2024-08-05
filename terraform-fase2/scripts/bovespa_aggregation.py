import sys
from datetime import datetime
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
import pandas as pd
import pyarrow.parquet as pq

# Obter argumentos do job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Inicialização do contexto do Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Inicializar cliente S3
s3 = boto3.client('s3')

# Nome do bucket e prefixo dos arquivos
bucket_name = 'bovespa-bucket'
prefix = 'raw/'

# Listar objetos no bucket
response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

# Obter a lista de arquivos
files = response.get('Contents', [])

# Extrair as datas dos arquivos
dates = []
for file in files:
    if 'bovespa.parquet' in file['Key']:
        date_str = file['Key'].split('/')[1]  # Assumindo que a estrutura é raw/YYYY-MM-DD/bovespa.parquet
        try:
            date = datetime.strptime(date_str, '%Y-%m-%d')
            dates.append(date)
        except ValueError:
            continue

# Obter a data mais recente
if dates:
    data_date = max(dates).strftime('%Y-%m-%d')
else:
    raise ValueError('Nenhum arquivo válido encontrado no bucket S3.')

# Definição do caminho S3 para o arquivo parquet
input_path = f"s3://{bucket_name}/raw/{data_date}/bovespa.parquet"
output_path = f"s3://{bucket_name}/processed/aggregated_bovespa.parquet"

# Leitura do arquivo parquet
df = spark.read.parquet(input_path)

# Realização da agregação (soma e sumarização)
aggregated_df = df.groupBy("Código").agg(
    {"Qtde_Teorica": "sum", "Part_Perc": "sum"}
)

# Renomear colunas para refletir a agregação
aggregated_df = aggregated_df.withColumnRenamed("sum(Qtde_Teorica)", "Total_Qtde_Teorica") \
                             .withColumnRenamed("sum(Part_Perc)", "Total_Part_Perc")

# Gravação do resultado em um novo arquivo parquet
aggregated_df.write.mode('overwrite').parquet(output_path)

# Finalização do job
job.commit()
