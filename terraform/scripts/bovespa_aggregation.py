import sys
from datetime import datetime
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
import pyspark.sql.functions as F
from awsglue.dynamicframe import DynamicFrame
from botocore.exceptions import ClientError

# Obter argumentos do job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Inicialização do contexto do Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Inicializar cliente S3 e Glue
s3 = boto3.client('s3')
glue_client = boto3.client('glue')

# Nome do bucket e prefixo dos arquivos
bucket_name = 'bovespa-bucket'
prefix = 'raw/'

# Nome do banco de dados e tabela no Glue
database_name = 'default'
table_name = 'bovespa_aggregated'

# Verificar se o banco de dados existe, senão, criar
try:
    glue_client.get_database(Name=database_name)
except ClientError as e:
    if e.response['Error']['Code'] == 'EntityNotFoundException':
        glue_client.create_database(
            DatabaseInput={
                'Name': database_name,
                'Description': 'Database for Bovespa aggregated data'
            }
        )
    else:
        raise e

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
output_path = f"s3://{bucket_name}/refined/"

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

# Gravação do resultado em um novo arquivo parquet particionado por data e ação
aggregated_df = aggregated_df.withColumn("data", F.lit(data_date))

# Limpar o caminho de destino antes de escrever novos dados
try:
    refined_files = s3.list_objects_v2(Bucket=bucket_name, Prefix='refined/')
    if 'Contents' in refined_files:
        for obj in refined_files['Contents']:
            s3.delete_object(Bucket=bucket_name, Key=obj['Key'])
except Exception as e:
    raise ValueError(f"Erro ao limpar o caminho de destino: {e}")

# Escrever os dados particionados no S3
aggregated_df.write.mode('overwrite').partitionBy("data", "Codigo").parquet(output_path)

# Converte o DataFrame do Spark para DynamicFrame do Glue
dynamic_frame = DynamicFrame.fromDF(aggregated_df, glueContext, "dynamic_frame")

# Criação da tabela no Glue Data Catalog
sink = glueContext.getSink(
    connection_type="s3",
    path=output_path,
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE"
)
sink.setCatalogInfo(
    catalogDatabase=database_name,
    catalogTableName=table_name
)
sink.setFormat("glueparquet")
sink.writeFrame(dynamic_frame)

# Finalização do job
job.commit()
