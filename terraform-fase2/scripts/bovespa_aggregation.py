import sys
from datetime import datetime
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

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
output_path = f"s3://{bucket_name}/refined/aggregated_bovespa.parquet"

# Leitura do arquivo parquet
df = spark.read.parquet(input_path)

# Renomear colunas existentes
df = df.withColumnRenamed("Código", "Codigo") \
       .withColumnRenamed("Ação", "Acao")

# Realização da agregação (soma e sumarização)
aggregated_df = df.groupBy("Codigo").agg(
    {"Qtde_Teorica": "sum", "Part_Perc": "sum"}
)

# Renomear colunas de agregação para refletir a agregação
aggregated_df = aggregated_df.withColumnRenamed("sum(Qtde_Teorica)", "Total_Qtde_Teorica") \
                             .withColumnRenamed("sum(Part_Perc)", "Total_Part_Perc")

# Adicionar colunas de data para cálculo de diferença de datas
# Exemplo: Adicionando duas colunas de data fictícias para cálculo
df = df.withColumn("Data_Inicial", F.lit("2024-01-01")) \
       .withColumn("Data_Final", F.lit(data_date))

# Convertendo colunas de data para tipo Date
df = df.withColumn("Data_Inicial", F.to_date(df["Data_Inicial"], "yyyy-MM-dd")) \
       .withColumn("Data_Final", F.to_date(df["Data_Final"], "yyyy-MM-dd"))

# Calcular a diferença entre as datas
df = df.withColumn("Diferenca_Dias", F.datediff(df["Data_Final"], df["Data_Inicial"]))

# Gravação do resultado em um novo arquivo parquet
aggregated_df.write.mode('overwrite').parquet(output_path)

# Finalização do job
job.commit()
