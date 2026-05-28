# src/gold.py
# FinPay Lakehouse - Gold
#
# Objetivo:
#   Crear una tabla Gold única y enriquecida que será consumida por las
#   Materialized Views del modelo dimensional:
#     - fact_transactions
#     - dim_merchant
#     - dim_user
#     - dim_channel
#     - dim_date
#
# Criterio del proyecto:
#   El modelo dimensional se crea con CREATE OR REPLACE MATERIALIZED VIEW.
#   Por eso Gold actúa como fuente curada para esas vistas materializadas.

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="gold.transactions_enriched",
    comment=(
        "Gold enriched transaction table. Fuente curada para el modelo dimensional "
        "creado mediante Materialized Views. Integra transacciones, comercios, usuarios, "
        "canal, calendario y score de riesgo calculado."
    ),
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true",
    },
)
def transactions_enriched():
    transactions = (
        dp.read_stream("silver.transactions")
        .alias("t")
    )

    merchants = (
        dp.read("silver.merchants")
        .select(
            F.col("merchant_id").alias("m_merchant_id"),
            F.col("merchant_name"),
            F.col("category").alias("merchant_category"),
            F.col("country").alias("merchant_country"),
            F.col("affiliation_date"),
            F.col("status").alias("merchant_status"),
            F.col("risk_level"),
        )
        .alias("m")
    )

    users = (
        dp.read("silver.users")
        .select(
            F.col("user_id").alias("u_user_id"),
            F.col("full_name"),
            F.col("document_id"),
            F.col("email"),
            F.col("phone"),
            F.col("country").alias("user_country"),
            F.col("segment").alias("user_segment"),
            F.col("registration_date"),
        )
        .alias("u")
    )

    enriched = (
        transactions
        .join(
            merchants,
            F.col("t.merchant_id") == F.col("m.m_merchant_id"),
            "left",
        )
        .join(
            users,
            F.col("t.user_id") == F.col("u.u_user_id"),
            "left",
        )
    )

    # Score simple y explicable para el proyecto.
    # Se calcula por transacción y luego el modelo dimensional puede agregarlo.
    risk_score = (
        F.when(F.col("risk_level") == "alto", F.lit(60))
        .when(F.col("risk_level") == "medio", F.lit(35))
        .when(F.col("risk_level") == "bajo", F.lit(15))
        .otherwise(F.lit(25))
        + F.when(F.col("status") == "rechazado", F.lit(25)).otherwise(F.lit(0))
        + F.when(F.col("is_reverse") == F.lit(True), F.lit(15)).otherwise(F.lit(0))
    )

    return (
        enriched
        .withColumn("transaction_count", F.lit(1))
        .withColumn("approved_transaction_count", F.when(F.col("status") == "aprobado", F.lit(1)).otherwise(F.lit(0)))
        .withColumn("rejected_transaction_count", F.when(F.col("status") == "rechazado", F.lit(1)).otherwise(F.lit(0)))
        .withColumn("pending_transaction_count", F.when(F.col("status") == "pendiente", F.lit(1)).otherwise(F.lit(0)))
        .withColumn("reverse_transaction_count", F.when(F.col("is_reverse") == F.lit(True), F.lit(1)).otherwise(F.lit(0)))
        .withColumn("risk_score", F.least(risk_score, F.lit(100)))
        .withColumn("date_key", F.date_format(F.col("transaction_date"), "yyyyMMdd").cast("int"))
        .withColumn("year", F.year(F.col("transaction_date")))
        .withColumn("quarter", F.quarter(F.col("transaction_date")))
        .withColumn("month", F.month(F.col("transaction_date")))
        .withColumn("week_of_year", F.weekofyear(F.col("transaction_date")))
        .withColumn("day_of_month", F.dayofmonth(F.col("transaction_date")))
        .withColumn(
            "channel_id",
            F.when(F.col("channel") == "web", F.lit(1))
            .when(F.col("channel") == "app", F.lit(2))
            .when(F.col("channel") == "pos", F.lit(3))
            .otherwise(F.lit(0)),
        )
        .withColumn(
            "channel_name",
            F.when(F.col("channel") == "web", F.lit("Web"))
            .when(F.col("channel") == "app", F.lit("App"))
            .when(F.col("channel") == "pos", F.lit("POS"))
            .otherwise(F.lit("Unknown")),
        )
        .select(
            # Transaction grain
            F.col("transaction_id"),
            F.col("transaction_date"),
            F.col("date_key"),
            F.col("year"),
            F.col("quarter"),
            F.col("month"),
            F.col("week_of_year"),
            F.col("day_of_month"),

            # Degenerate / fact keys
            F.col("user_id"),
            F.col("merchant_id"),
            F.col("channel_id"),
            F.col("channel"),
            F.col("channel_name"),

            # Measures
            F.col("amount"),
            F.col("currency"),
            F.col("transaction_count"),
            F.col("approved_transaction_count"),
            F.col("rejected_transaction_count"),
            F.col("pending_transaction_count"),
            F.col("reverse_transaction_count"),
            F.col("risk_score"),

            # Transaction attributes
            F.col("transaction_type"),
            F.col("status"),
            F.col("reference_id"),
            F.col("is_reverse"),

            # Merchant attributes for dimensional MVs
            F.col("merchant_name"),
            F.col("merchant_category"),
            F.col("merchant_country"),
            F.col("affiliation_date"),
            F.col("merchant_status"),
            F.col("risk_level"),

            # User attributes for dimensional MVs
            F.col("full_name"),
            F.col("document_id"),
            F.col("email"),
            F.col("phone"),
            F.col("user_country"),
            F.col("user_segment"),
            F.col("registration_date"),

            # Audit
            F.col("_source_file"),
            F.col("_ingested_at"),
            F.col("_processed_at"),
            F.col("_load_date"),
        )
    )
