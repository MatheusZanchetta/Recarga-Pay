import logging
import requests
import json
import boto3
import os
from datetime import datetime, timedelta
from airflow.models import Variable

def cdi_ingest_job(**context):
    # Pega a data do execution_date do Airflow (sempre D-1)
    execution_date = context["ds"]  # formato YYYY-MM-DD
    date_obj = datetime.strptime(execution_date, "%Y-%m-%d")
    date_str_bucket = (date_obj - timedelta(days=1)).strftime("%Y-%m-%d")  # <- apenas data
    query_date = (date_obj - timedelta(days=1)).strftime("%d/%m/%Y")

    url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.12/dados?formato=json&dataInicial={query_date}&dataFinal={query_date}"
    logging.info(f"Buscando dados na URL: {url}")

    response = requests.get(url)
    if response.status_code != 200:
        logging.warning(f"Não foi possível acessar a API. Status code: {response.status_code}")
        return

    data = response.json()
    if not data:
        logging.warning(f"Nenhum dado encontrado para a data {query_date}.")
        return

    logging.info(f"Dado encontrado: {data}")

    # Configura S3
    s3_bucket  = "mzan-raw-rp-case"
    s3_prefix = f"cdi/data={date_str_bucket}/data.json"  # <- corrigido

    s3 = boto3.client("s3")
    json_data = json.dumps(data)

    s3.put_object(
        Bucket=s3_bucket,
        Key=s3_prefix,
        Body=json_data
    )

    logging.info(f"Arquivo salvo no S3: s3://{s3_bucket}/{s3_prefix}")

if __name__ == "__main__":
    data_execucao = os.getenv('EXECUTION_DATE') or datetime.now().strftime('%Y-%m-%d')
    cdi_ingest_job(data_execucao)