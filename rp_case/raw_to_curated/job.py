import logging
from datetime import datetime, timedelta
from pyspark.sql.functions import col, lit, to_date
from delta.tables import DeltaTable
from rp_case.utils.spark_session import create_spark_session

# Configurar logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def raw_to_curated_job(execution_date=None):
    spark = create_spark_session()
    logger.info("SparkSession iniciada.")

    # Ajuste do parâmetro de data (sempre D-1)
    if isinstance(execution_date, str):
        execution_date_obj = datetime.strptime(execution_date, "%Y-%m-%d")
    else:
        execution_date_obj = execution_date

    # Datas ajustadas
    prev_date_obj = execution_date_obj - timedelta(days=1)
    execution_date_str = prev_date_obj.strftime("%Y-%m-%d")
    date_str_bucket = prev_date_obj.strftime("%Y-%m-%d")

    # Buckets raiz
    raw_bucket_tx = "s3a://mzan-raw-rp-case/"
    raw_bucket_cdi = f"s3a://mzan-raw-rp-case/cdi/data={date_str_bucket}/data.json"
    curated_bucket_tx = "s3a://mzan-curated-rp-case/transactions_delta/"
    curated_bucket_cdi = "s3a://mzan-curated-rp-case/cdi_delta/"

    # ---------------------------------------------------------
    # 1. Processar TRANSACTIONS (Parquet → Delta, particionado)
    # ---------------------------------------------------------
    logger.info(f"Lendo transações de {raw_bucket_tx} para {execution_date_str}")
    df_raw_tx = spark.read.parquet(raw_bucket_tx)

    df_filtered_tx = (
        df_raw_tx.filter(to_date(col("event_time")) == lit(execution_date_str))
                 .withColumn("date", to_date(col("event_time")))
    )

    if not df_filtered_tx.isEmpty():
        df_filtered_tx.show()

        # Verifica se a tabela Delta já existe
        if DeltaTable.isDeltaTable(spark, curated_bucket_tx):
            delta_tx = DeltaTable.forPath(spark, curated_bucket_tx)
            (
                delta_tx.alias("t")
                .merge(
                    df_filtered_tx.alias("s"),
                    "t.account_id = s.account_id AND t.event_time = s.event_time"
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
            logger.info("Transações Delta atualizadas (merge).")
        else:
            (
                df_filtered_tx.write.format("delta")
                .mode("overwrite")
                .partitionBy("date")
                .save(curated_bucket_tx)
            )
            logger.info("Transações Delta criadas.")
    else:
        logger.info("Nenhuma transação encontrada para a data.")

    # ---------------------------------------------------------
    # 2. Processar CDI (JSON → Delta, append único)
    # ---------------------------------------------------------
    logger.info(f"Lendo CDI JSON de {raw_bucket_cdi} para {execution_date_str}")
    try:
        df_raw_cdi = spark.read.json(raw_bucket_cdi)
    except Exception as e:
        logger.warning(f"Nenhum arquivo de CDI encontrado para {execution_date_str}. Detalhe: {e}")
        spark.stop()
        return

    df_cdi = (
        df_raw_cdi.withColumn("date", lit(execution_date_str))
                  .withColumn("valor", col("valor").cast("double"))
                  .select("date", "valor")
    )

    if not df_cdi.isEmpty():
        df_cdi.show()

        if DeltaTable.isDeltaTable(spark, curated_bucket_cdi):
            delta_cdi = DeltaTable.forPath(spark, curated_bucket_cdi)
            (
                delta_cdi.alias("t")
                .merge(
                    df_cdi.alias("s"),
                    "t.date = s.date"
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
            logger.info("CDI Delta atualizado (merge).")
        else:
            (
                df_cdi.write.format("delta")
                .mode("overwrite")
                .save(curated_bucket_cdi)
            )
            logger.info("CDI Delta criado.")
    else:
        logger.info("Nenhum CDI encontrado para a data.")

    spark.stop()
    logger.info("Job finalizado com sucesso.")

if __name__ == "__main__":
    raw_to_curated_job()
