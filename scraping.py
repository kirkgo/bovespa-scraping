import os
import logging
import boto3
import io
import numpy as np
import pandas as pd
import pyarrow as pa
from pyarrow import parquet as pq
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Configuração do log
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def clean_and_convert(x):
    try:
        return float(x.replace('.', '').replace(',', '.'))
    except (ValueError, AttributeError):
        return np.nan

def main():
    # Configurar o driver do Selenium
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    driver = webdriver.Chrome(options=options)

    try:
        url = "https://sistemaswebb3-listados.b3.com.br/indexPage/day/IBOV?language=pt-br"
        driver.get(url)

        wait = WebDriverWait(driver, 20)
        table = wait.until(EC.presence_of_element_located((By.XPATH, '//table')))

        rows = table.find_elements(By.XPATH, './/tr')

        data = []
        for row in rows:
            cols = row.find_elements(By.XPATH, './/td')
            if cols:
                data.append([col.text for col in cols])

        if not data or not data[0]:
            raise ValueError("Nenhum dado foi encontrado na tabela.")
        else:
            logger.info("Dados coletados com sucesso")

        column_names = ["Código", "Ação", "Tipo", "Qtde. Teórica", "Part. (%)"]
        df = pd.DataFrame(data, columns=column_names)
        logger.info(f"Primeiras linhas do DataFrame:\n{df.head()}")

        df = df[~df['Código'].str.contains('Quantidade Teórica Total|Redutor', na=False)]

        df['Qtde. Teórica'] = df['Qtde. Teórica'].apply(clean_and_convert)
        df['Part. (%)'] = df['Part. (%)'].apply(clean_and_convert)

        if df['Qtde. Teórica'].isnull().any() or df['Part. (%)'].isnull().any():
            raise ValueError("Valores inválidos encontrados após a conversão para float.")

        logger.info(f"Schema do DataFrame:\n{df.dtypes}")
        logger.info(f"Valores nulos no DataFrame:\n{df.isnull().sum()}")

        df.rename(columns={"Qtde. Teórica": "Qtde_Teorica", "Part. (%)": "Part_Perc"}, inplace=True)
        df.reset_index(drop=True, inplace=True)

        table = pa.Table.from_pandas(df)
        parquet_buffer = io.BytesIO()
        pq.write_table(table, parquet_buffer)
        parquet_buffer.seek(0)

        table_read = pq.read_table(parquet_buffer)
        df_read = table_read.to_pandas()
        logger.info(f"Conteúdo do arquivo Parquet:\n{df_read}")

        current_date = pd.to_datetime('now').strftime('%Y-%m-%d')
        directory_name = f'raw/{current_date}'
        parquet_file_name = f'{directory_name}/bovespa.parquet'

        if not os.path.exists(directory_name):
            os.makedirs(directory_name)

        pq.write_table(table, parquet_file_name)
        logger.info(f"Arquivo Parquet salvo como {parquet_file_name}")

        s3 = boto3.client('s3')
        bucket_name = os.getenv('S3_BUCKET_NAME', 'bovespa-bucket')
        s3.upload_file(parquet_file_name, bucket_name, parquet_file_name)

        logger.info(f"Arquivo {parquet_file_name} enviado para o S3")

    except Exception as e:
        logger.error(f"Erro durante o scraping ou processamento dos dados: {e}", exc_info=True)
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
