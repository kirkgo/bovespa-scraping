import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, datediff, current_date, date_format, sum as _sum
import logging

# Configuração do log
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Inicialização do Job Glue
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

try:
    # Carregar dados de uma tabela Glue
    logger.info("Carregando dados da tabela Glue 'bovespa_input_table'")
    datasource = glueContext.create_dynamic_frame.from_catalog(
        database="bovespa_db", 
        table_name="bovespa_input_table", 
        transformation_ctx="datasource"
    )

    # Conversão para DataFrame do Spark
    df = datasource.toDF()

    # Verificação inicial do esquema
    logger.info(f"Esquema inicial dos dados:\n{df.printSchema()}")

    # Renomear colunas
    logger.info("Renomeando colunas")
    df = df.withColumnRenamed("Código", "CodigoRenomeado") \
           .withColumnRenamed("Ação", "AcaoRenomeada")

    # Agrupamento e sumarização dos dados
    logger.info("Agrupando e sumarizando os dados")
    df_grouped = df.groupBy("CodigoRenomeado").agg(
        _sum("Qtde_Teorica").alias("Qtde_Teorica_Total"),
        _sum("Part_Perc").alias("Part_Perc_Total")
    )

    # Adicionar colunas de partição e cálculo de data
    logger.info("Adicionando colunas de partição e cálculo de data")
    df_grouped = df_grouped.withColumn("date_partition", date_format(current_date(), "yyyy-MM-dd"))
    df_grouped = df_grouped.withColumn("symbol", col("CodigoRenomeado"))

    # Verificação final do esquema
    logger.info(f"Esquema final dos dados:\n{df_grouped.printSchema()}")

    # Conversão de volta para DynamicFrame
    dynamic_frame = DynamicFrame.fromDF(df_grouped, glueContext, "dynamic_frame")

    # Gravar de volta para S3 no formato Parquet, particionado por data e nome da ação
    logger.info("Gravando os dados no S3 em formato Parquet")
    sink = glueContext.getSink(
        connection_type="s3", 
        path="s3://my-bovespa-bucket/refined/", 
        transformation_ctx="sink"
    )
    sink.setFormat("parquet")
    sink.setCatalogInfo(
        catalogDatabase="bovespa_db", 
        catalogTableName="bovespa_output_table"
    )
    sink.setFormatOptions(
        format_options={"partitionKeys": ["date_partition", "symbol"]}
    )
    sink.writeFrame(dynamic_frame)

    job.commit()
    logger.info("Job finalizado com sucesso")

except Exception as e:
    logger.error(f"Erro durante o processamento do job Glue: {e}")
    job.commit()
