# src/silver.py
# FinPay Lakehouse - Silver con AUTO CDC SCD Type 1
#
# Silver lee directamente desde:
#   bronze.transactions_valid_changes
#   bronze.merchants_valid_changes
#   bronze.users_valid_changes
#
# Silver persiste:
#   silver.transactions
#   silver.merchants
#   silver.users
#   silver.quarantine

from pyspark import pipelines as dp
from pyspark.sql import functions as F


# ============================================================
# AUTO CDC SCD Type 1 hacia Silver current
# ============================================================

dp.create_streaming_table(
    name="silver.transactions",
    comment="Silver current transactions. Último estado por transaction_id usando AUTO CDC SCD Type 1.",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
    },
)

dp.create_auto_cdc_flow(
    name="transactions_scd1_flow",
    target="silver.transactions",
    source="bronze.transactions_valid_changes",
    keys=["transaction_id"],
    sequence_by=F.struct(F.col("_sequence_ts"), F.col("_source_file"), F.col("_record_hash")),
    ignore_null_updates=False,
    except_column_list=[
        "_cdc_operation",
        "_sequence_ts",
        "_record_hash",
        "is_valid",
        "quality_errors",
    ],
    stored_as_scd_type=1,
)


dp.create_streaming_table(
    name="silver.merchants",
    comment="Silver current merchants. Último estado por merchant_id usando AUTO CDC SCD Type 1.",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
    },
)

dp.create_auto_cdc_flow(
    name="merchants_scd1_flow",
    target="silver.merchants",
    source="bronze.merchants_valid_changes",
    keys=["merchant_id"],
    sequence_by=F.struct(F.col("_sequence_ts"), F.col("_source_file"), F.col("_record_hash")),
    ignore_null_updates=False,
    except_column_list=[
        "_cdc_operation",
        "_sequence_ts",
        "_record_hash",
        "is_valid",
        "quality_errors",
    ],
    stored_as_scd_type=1,
)


dp.create_streaming_table(
    name="silver.users",
    comment="Silver current users. Último estado por user_id usando AUTO CDC SCD Type 1. Contiene PII.",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
    },
)

dp.create_auto_cdc_flow(
    name="users_scd1_flow",
    target="silver.users",
    source="bronze.users_valid_changes",
    keys=["user_id"],
    sequence_by=F.struct(F.col("_sequence_ts"), F.col("_source_file"), F.col("_record_hash")),
    ignore_null_updates=False,
    except_column_list=[
        "_cdc_operation",
        "_sequence_ts",
        "_record_hash",
        "is_valid",
        "quality_errors",
    ],
    stored_as_scd_type=1,
)


# ============================================================
# Silver quarantine
# ============================================================

@dp.table(
    name="silver.quarantine",
    comment=(
        "Tabla de cuarentena con registros inválidos detectados en bronze.*_invalid_changes "
        "antes de AUTO CDC."
    ),
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
    },
)
def quarantine():
    tx_quarantine = (
        dp.read_stream("bronze.transactions_invalid_changes")
        .select(
            F.lit("transactions").alias("source_name"),
            F.lit("silver.transactions").alias("target_table"),
            F.array_join(F.col("quality_errors"), "; ").alias("rejection_reason"),
            F.col("quality_errors"),
            F.col("_processed_at").alias("processed_at"),
            F.col("_raw_record").alias("raw_record"),
            F.col("_source_file").alias("source_file"),
            F.col("_load_date").alias("load_date"),
        )
    )

    merchants_quarantine = (
        dp.read_stream("bronze.merchants_invalid_changes")
        .select(
            F.lit("merchants").alias("source_name"),
            F.lit("silver.merchants").alias("target_table"),
            F.array_join(F.col("quality_errors"), "; ").alias("rejection_reason"),
            F.col("quality_errors"),
            F.col("_processed_at").alias("processed_at"),
            F.col("_raw_record").alias("raw_record"),
            F.col("_source_file").alias("source_file"),
            F.col("_load_date").alias("load_date"),
        )
    )

    users_quarantine = (
        dp.read_stream("bronze.users_invalid_changes")
        .select(
            F.lit("users").alias("source_name"),
            F.lit("silver.users").alias("target_table"),
            F.array_join(F.col("quality_errors"), "; ").alias("rejection_reason"),
            F.col("quality_errors"),
            F.col("_processed_at").alias("processed_at"),
            F.col("_raw_record").alias("raw_record"),
            F.col("_source_file").alias("source_file"),
            F.col("_load_date").alias("load_date"),
        )
    )

    return tx_quarantine.unionByName(merchants_quarantine).unionByName(users_quarantine)
