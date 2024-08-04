import json
import boto3
import os
import logging

# Configuração do log
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def lambda_handler(event, context):
    try:
        logger.info(f"Evento recebido do S3: {json.dumps(event)}")
        
        # Verificar se a variável de ambiente GLUE_JOB_NAME está definida
        job_name = os.environ.get('GLUE_JOB_NAME')
        if not job_name:
            raise ValueError("A variável de ambiente 'GLUE_JOB_NAME' não está definida.")

        # Inicializar cliente do Glue
        glue = boto3.client('glue')

        # Iniciar o job Glue
        logger.info(f"Iniciando o job Glue: {job_name}")
        response = glue.start_job_run(JobName=job_name)
        job_run_id = response['JobRunId']
        
        logger.info(f"Job Glue iniciado com sucesso: {job_run_id}")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Glue job started successfully',
                'JobRunId': job_run_id
            })
        }
    
    except Exception as e:
        logger.error(f"Erro ao iniciar o job Glue: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Erro ao iniciar o job Glue',
                'error': str(e)
            })
        }
