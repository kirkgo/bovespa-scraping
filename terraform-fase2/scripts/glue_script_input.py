import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
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
            ("Qtde_Teorica", "double", "Qtde_Teorica", "double"),  # Atualizado para double
            ("Part_Perc", "double", "Part_Perc", "double")  # Atualizado para double
        ], 
        transformation_ctx="applymapping"
    )

    # Conversão para DataFrame do Spark
    df = applymapping.toDF()

    # Gravar de volta para S3 no formato Parquet, particionado por data
    logger.info("Gravando os dados no S3 em formato Parquet")
    dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")
    sink = glueContext.getSink(
        connection_type="s3", 
        path="s3://my-bovespa-bucket/raw/", 
        transformation_ctx="sink"
    )
    sink.setFormat("parquet")
    sink.setCatalogInfo(
        catalogDatabase="bovespa_db", 
        catalogTableName="bovespa_input_table"
    )
    sink.setFormatOptions(
        format_options={"partitionKeys": ["Codigo"]}
    )
    sink.writeFrame(dynamic_frame)

    job.commit()
    logger.info("Job finalizado com sucesso")

except Exception as e:
    logger.error(f"Erro durante o processamento do job Glue: {e}")
    job.commit()
