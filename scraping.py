import sys
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import boto3
from pyarrow import parquet as pq
import pyarrow as pa
import io
import logging
import os
import numpy as np

# Configuração do log
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    # Configurar o driver do Selenium
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    driver = webdriver.Chrome(options=options)

    try:
        # Acessar a página e realizar o scraping
        url = "https://sistemaswebb3-listados.b3.com.br/indexPage/day/IBOV?language=pt-br"
        driver.get(url)

        # Aguardar até que a tabela esteja presente na página
        wait = WebDriverWait(driver, 20)
        table = wait.until(EC.presence_of_element_located((By.XPATH, '//table')))

        # Encontrar todas as linhas da tabela
        rows = table.find_elements(By.XPATH, './/tr')

        data = []
        for row in rows:
            cols = row.find_elements(By.XPATH, './/td')
            if len(cols) > 0:
                data.append([col.text for col in cols])

        # Verificar o conteúdo de `data`
        if len(data) == 0 or len(data[0]) == 0:
            raise ValueError("Nenhum dado foi encontrado na tabela.")
        else:
            logger.info("Dados coletados com sucesso")

        # Adicionar manualmente os nomes das colunas
        column_names = ["Código", "Ação", "Tipo", "Qtde. Teórica", "Part. (%)"]
        df = pd.DataFrame(data, columns=column_names)

        # Verificar as primeiras linhas do DataFrame
        logger.info(f"Primeiras linhas do DataFrame:\n{df.head()}")

        # Remover linhas indesejadas
        df = df[~df['Código'].str.contains('Quantidade Teórica Total|Redutor', na=False)]

        # Função para limpar e converter para float
        def clean_and_convert(x):
            if pd.isna(x) or x == '':
                return np.nan
            return float(x.replace('.', '').replace(',', '.'))

        # Aplicar a função de limpeza e conversão
        df['Qtde. Teórica'] = df['Qtde. Teórica'].apply(clean_and_convert)
        df['Part. (%)'] = df['Part. (%)'].apply(clean_and_convert)

        # Salvar o DataFrame no formato parquet
        table = pa.Table.from_pandas(df)
        parquet_buffer = io.BytesIO()
        pq.write_table(table, parquet_buffer)
        parquet_buffer.seek(0)

        # Verificar o conteúdo do arquivo Parquet
        parquet_buffer.seek(0)  # Reposicionar o ponteiro para o início do buffer
        table_read = pq.read_table(parquet_buffer)
        df_read = table_read.to_pandas()
        logger.info(f"Conteúdo do arquivo Parquet:\n{df_read}")

        # Nome do arquivo parquet com partição diária
        current_date = pd.to_datetime('now').strftime('%Y-%m-%d')
        parquet_file_name = f'raw/{current_date}/bovespa.parquet'

        # Enviar para o S3
        s3 = boto3.client('s3')
        bucket_name = os.getenv('S3_BUCKET_NAME', 'my-bovespa-bucket')  # Use variável de ambiente para o bucket
        s3.upload_fileobj(parquet_buffer, bucket_name, parquet_file_name)

        logger.info(f"Arquivo {parquet_file_name} enviado para o S3")

    except Exception as e:
        logger.error(f"Erro durante o scraping ou processamento dos dados: {e}", exc_info=True)
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
