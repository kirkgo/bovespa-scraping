import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, datediff, current_date, date_format
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
    logger.info("Carregando dados da tabela Glue 'bovespa_table'")
    datasource = glueContext.create_dynamic_frame.from_catalog(
        database="bovespa_db", 
        table_name="bovespa_table", 
        transformation_ctx="datasource"
    )

    # Renomear colunas com nomes da tabela
    logger.info("Aplicando mapeamento nas colunas")
    applymapping = ApplyMapping.apply(
        frame=datasource, 
        mappings=[
            ("Código", "string", "Codigo", "string"), 
            ("Ação", "string", "Acao", "string"), 
            ("Tipo", "string", "Tipo", "string"),
            ("Qtde_Teorica", "string", "Qtde_Teorica", "string"),  # Atualizado
            ("Part_Perc", "string", "Part_Pct", "string")  # Atualizado
        ], 
        transformation_ctx="applymapping"
    )

    # Conversão para DataFrame do Spark
    df = applymapping.toDF()

    # Agrupamento e sumarização dos dados
    logger.info("Agrupando e sumarizando os dados")
    df_grouped = df.groupBy("Codigo").agg({'Qtde_Teorica': 'sum'})

    # Adicionar colunas de partição
    logger.info("Adicionando colunas de partição")
    df_grouped = df_grouped.withColumn("date_partition", date_format(current_date(), "yyyy-MM-dd"))
    df_grouped = df_grouped.withColumn("symbol", col("Codigo"))

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
