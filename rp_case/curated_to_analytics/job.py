import logging
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from rp_case.utils.spark_session import create_spark_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def analytics_job(execution_date: str):
    spark = create_spark_session()
    logger.info("SparkSession iniciada (analytics).")

    # Ajusta datas
    proc_date = datetime.strptime(execution_date, "%Y-%m-%d").date()
    prev_date = proc_date - timedelta(days=1)
    proc_date_str = proc_date.strftime("%Y-%m-%d")
    prev_date_str = prev_date.strftime("%Y-%m-%d")

    # Caminhos raiz (sempre a raiz da tabela Delta, nunca partição)
    ANALYTICS_PATH = "s3a://mzan-analytics-rp-case/payouts_delta/"
    CURATED_TX_PATH = "s3a://mzan-curated-rp-case/transactions_delta/"
    CURATED_CDI_PATH = "s3a://mzan-curated-rp-case/cdi_delta/"

    # 1) Carregar CDI e pegar taxa do dia
    df_cdi_all = spark.read.format("delta").load(CURATED_CDI_PATH)
    df_cdi_all.createOrReplaceTempView("cdi_all")

    df_cdi_day = spark.sql(f"""
        SELECT valor as cdi_valor
        FROM cdi_all
        WHERE date = '{proc_date_str}'
    """)

    if df_cdi_day.rdd.isEmpty():
        logger.info(f"Sem CDI para {proc_date_str}. Encerrando sem erro.")
        spark.stop()
        return

    cdi_valor = df_cdi_day.collect()[0]["cdi_valor"] / 100.0
    spark.sql(f"CREATE OR REPLACE TEMP VIEW cdi_rate AS SELECT {cdi_valor} as rate")

    # 2) Transações curated (filtra pelo SQL, não pelo caminho)
    df_tx = spark.read.format("delta").load(CURATED_TX_PATH)
    df_tx.createOrReplaceTempView("tx")

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW tx_norm AS
        SELECT
            user_id,
            account_id,
            event_time,
            amount,
            TO_DATE(event_time) AS event_date
        FROM tx
    """)

    # 3) Movimentação do dia D
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW mov_dia AS
        SELECT
            user_id,
            account_id,
            COALESCE(SUM(amount),0) AS movimentacao_dia
        FROM tx_norm
        WHERE event_date = '{proc_date_str}'
        GROUP BY user_id, account_id
    """)

    # 4) Saldo acumulado até D-1 (caso primeira carga)
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW saldo_ate_ontem AS
        SELECT
            user_id,
            account_id,
            COALESCE(SUM(amount),0) AS saldo_ate_ontem_curated
        FROM tx_norm
        WHERE event_date <= '{prev_date_str}'
        GROUP BY user_id, account_id
    """)

    # 5) Saldo final do dia D-1 via analytics (se existir)
    analytics_exists = DeltaTable.isDeltaTable(spark, ANALYTICS_PATH)
    if analytics_exists:
        spark.read.format("delta").load(ANALYTICS_PATH).createOrReplaceTempView("analytics")
        spark.sql(f"""
            CREATE OR REPLACE TEMP VIEW prev_analytics AS
            SELECT
                user_id,
                account_id,
                saldo_final AS prev_saldo_final
            FROM analytics
            WHERE date = '{prev_date_str}'
        """)
    else:
        spark.sql("CREATE OR REPLACE TEMP VIEW prev_analytics AS SELECT NULL as user_id, NULL as account_id, NULL as prev_saldo_final LIMIT 0")

    # 6) Base de contas (mov hoje ou saldo até ontem)
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW accounts_today AS
        SELECT user_id, account_id FROM mov_dia
        UNION
        SELECT user_id, account_id FROM saldo_ate_ontem
    """)

    # 7) Montar resultado final
    df_result = spark.sql(f"""
        SELECT
            a.user_id,
            a.account_id,
            COALESCE(pa.prev_saldo_final, s.saldo_ate_ontem_curated, 0.0) AS saldo_a_mais_24_horas,
            COALESCE(m.movimentacao_dia, 0.0) AS movimentacao_dia,
            CASE WHEN COALESCE(pa.prev_saldo_final, s.saldo_ate_ontem_curated, 0.0) >= 100
                 THEN COALESCE(pa.prev_saldo_final, s.saldo_ate_ontem_curated, 0.0)
                 ELSE 0.0 END AS saldo_elegivel,
            CASE WHEN COALESCE(pa.prev_saldo_final, s.saldo_ate_ontem_curated, 0.0) >= 100
                 THEN ROUND(COALESCE(pa.prev_saldo_final, s.saldo_ate_ontem_curated, 0.0) * r.rate, 8)
                 ELSE 0.0 END AS juros_calculado,
            ROUND(
                COALESCE(pa.prev_saldo_final, s.saldo_ate_ontem_curated, 0.0)
                + COALESCE(m.movimentacao_dia,0.0)
                + CASE WHEN COALESCE(pa.prev_saldo_final, s.saldo_ate_ontem_curated, 0.0) >= 100
                       THEN COALESCE(pa.prev_saldo_final, s.saldo_ate_ontem_curated, 0.0) * r.rate
                       ELSE 0.0 END,
                8
            ) AS saldo_final,
            '{proc_date_str}' AS date
        FROM accounts_today a
        LEFT JOIN prev_analytics pa
          ON a.user_id = pa.user_id AND a.account_id = pa.account_id
        LEFT JOIN saldo_ate_ontem s
          ON a.user_id = s.user_id AND a.account_id = s.account_id
        LEFT JOIN mov_dia m
          ON a.user_id = m.user_id AND a.account_id = m.account_id
        CROSS JOIN cdi_rate r
    """)

    if df_result.rdd.isEmpty():
        logger.info(f"Nenhum dado para {proc_date_str}, nada a salvar.")
        spark.stop()
        return

    # 8) Escrever no Delta (merge idempotente)
    if analytics_exists:
        delta = DeltaTable.forPath(spark, ANALYTICS_PATH)
        (
            delta.alias("t")
            .merge(
                df_result.alias("s"),
                "t.user_id = s.user_id AND t.account_id = s.account_id AND t.date = s.date"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        logger.info(f"Analytics atualizado (merge) para {proc_date_str}.")
    else:
        (
            df_result.write.format("delta")
            .mode("overwrite")
            .partitionBy("date")
            .save(ANALYTICS_PATH)
        )
        logger.info(f"Analytics Delta criado em {ANALYTICS_PATH}.")
    df_result.show()
    spark.stop()
    logger.info(f"Job analytics finalizado com sucesso para {proc_date_str}.")
