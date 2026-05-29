# src/bronze.py
# FinPay Lakehouse - Bronze
#
# Persistimos en Bronze:
#   1. Raw tables desde Auto Loader
#   2. Changes tables CDC-like
#   3. Valid/invalid typed changes para alimentar Silver AUTO CDC y quarantine
#
# Silver ya NO usa temporary views como source de AUTO CDC.
# Silver lee directamente desde bronze.*_valid_changes.

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from utils import (
    VALID_CHANNELS,
    VALID_COUNTRIES,
    VALID_CURRENCIES,
    VALID_MERCHANT_CATEGORIES,
    VALID_MERCHANT_STATUS,
    VALID_RISK_LEVELS,
    VALID_TRANSACTION_STATUS,
    VALID_TRANSACTION_TYPES,
    VALID_USER_SEGMENTS,
    add_bronze_audit_columns,
    add_bronze_change_columns,
    add_processed_columns,
    clean_string,
    lower_clean,
    normalize_country_expr,
    normalize_email_expr,
    normalize_phone_expr,
    normalize_risk_level_expr,
    normalize_segment_expr,
    normalize_status_expr,
    parse_amount,
    parse_date_multi,
    read_autoloader_stream,
    with_quality_errors,
)


# ============================================================
# Bronze raw tables
# ============================================================

@dp.table(
    name="bronze.transactions",
    comment=(
        "Bronze raw transactions. Ingesta incremental con Auto Loader. "
        "Columnas de negocio en STRING y trazabilidad de archivo."
    ),
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true",
    },
)
def transactions():
    df = read_autoloader_stream(spark, "transactions")
    return add_bronze_audit_columns(df, "transactions")


@dp.table(
    name="bronze.merchants",
    comment=(
        "Bronze raw merchants. Ingesta incremental con Auto Loader. "
        "Columnas de negocio en STRING y trazabilidad de archivo."
    ),
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true",
    },
)
def merchants():
    df = read_autoloader_stream(spark, "merchants")
    return add_bronze_audit_columns(df, "merchants")


@dp.table(
    name="bronze.users",
    comment=(
        "Bronze raw users. Ingesta incremental de TXT pipe-delimited tratado como CSV. "
        "Columnas de negocio en STRING y trazabilidad de archivo."
    ),
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true",
    },
)
def users():
    df = read_autoloader_stream(spark, "users")
    return add_bronze_audit_columns(df, "users")


# ============================================================
# Bronze CDC-like changes tables
# ============================================================

@dp.table(
    name="bronze.transactions_changes",
    comment=(
        "Bronze CDC-like transactions changes. Cada registro raw se modela como UPSERT."
    ),
    table_properties={
        "quality": "bronze_changes",
        "delta.enableChangeDataFeed": "true",
    },
)
def transactions_changes():
    df = dp.read_stream("bronze.transactions")
    return add_bronze_change_columns(df, "transactions")


@dp.table(
    name="bronze.merchants_changes",
    comment=(
        "Bronze CDC-like merchants changes. Cada registro raw se modela como UPSERT."
    ),
    table_properties={
        "quality": "bronze_changes",
        "delta.enableChangeDataFeed": "true",
    },
)
def merchants_changes():
    df = dp.read_stream("bronze.merchants")
    return add_bronze_change_columns(df, "merchants")


@dp.table(
    name="bronze.users_changes",
    comment=(
        "Bronze CDC-like users changes. Cada registro raw se modela como UPSERT."
    ),
    table_properties={
        "quality": "bronze_changes",
        "delta.enableChangeDataFeed": "true",
    },
)
def users_changes():
    df = dp.read_stream("bronze.users")
    return add_bronze_change_columns(df, "users")


# ============================================================
# Helper functions for typed changes
# ============================================================

def build_transactions_typed_changes():
    bronze_changes = dp.read_stream("bronze.transactions_changes")

    cleaned_df = (
        bronze_changes
        .withColumn("transaction_id", clean_string("transaction_id"))
        .withColumn("user_id", clean_string("user_id"))
        .withColumn("merchant_id", clean_string("merchant_id"))
        .withColumn("channel", lower_clean("channel"))
        .withColumn("transaction_type", lower_clean("transaction_type"))
        .withColumn("amount", parse_amount("amount"))
        .withColumn("currency", F.upper(clean_string("currency")))
        .withColumn("transaction_date", parse_date_multi("transaction_date"))
        .withColumn("status", lower_clean("status"))
        .withColumn("reference_id", clean_string("reference_id"))
        .withColumn("is_reverse", F.col("transaction_type") == F.lit("reversa"))
    )

    quality_rules = {
        "transaction_id inválido o nulo": ~F.col("transaction_id").rlike(r"^TXN-\d{8}-\d{5}$") | F.col("transaction_id").isNull(),
        "user_id inválido o nulo": ~F.col("user_id").rlike(r"^USR-\d{6}$") | F.col("user_id").isNull(),
        "merchant_id inválido o nulo": ~F.col("merchant_id").rlike(r"^MCH-\d{5}$") | F.col("merchant_id").isNull(),
        "channel fuera de catálogo": ~F.col("channel").isin(*VALID_CHANNELS) | F.col("channel").isNull(),
        "transaction_type fuera de catálogo": ~F.col("transaction_type").isin(*VALID_TRANSACTION_TYPES) | F.col("transaction_type").isNull(),
        "amount inválido": F.col("amount").isNull() | (F.col("amount") <= 0),
        "currency fuera de catálogo": ~F.col("currency").isin(*VALID_CURRENCIES) | F.col("currency").isNull(),
        "transaction_date inválida": F.col("transaction_date").isNull(),
        "status fuera de catálogo": ~F.col("status").isin(*VALID_TRANSACTION_STATUS) | F.col("status").isNull(),
        "reference_id obligatorio para reversa": (F.col("transaction_type") == "reversa") & F.col("reference_id").isNull(),
        "reference_id debe ser nulo si no es reversa": (F.col("transaction_type") != "reversa") & F.col("reference_id").isNotNull(),
    }

    return (
        add_processed_columns(with_quality_errors(cleaned_df, quality_rules))
        .select(
            "transaction_id",
            "user_id",
            "merchant_id",
            "channel",
            "transaction_type",
            "amount",
            "currency",
            "transaction_date",
            "status",
            "reference_id",
            "is_reverse",
            "is_valid",
            "quality_errors",
            "_cdc_operation",
            "_sequence_ts",
            "_record_hash",
            "_raw_record",
            "_source_file",
            "_ingested_at",
            "_processed_at",
            "_load_date",
        )
    )


def build_merchants_typed_changes():
    bronze_changes = dp.read_stream("bronze.merchants_changes")

    cleaned_df = (
        bronze_changes
        .withColumn("merchant_id", clean_string("merchant_id"))
        .withColumn("merchant_name", clean_string("merchant_name"))
        .withColumn("category", lower_clean("category"))
        .withColumn("country", normalize_country_expr("country"))
        .withColumn("affiliation_date", parse_date_multi("affiliation_date"))
        .withColumn("status", normalize_status_expr("status", VALID_MERCHANT_STATUS))
        .withColumn("risk_level", normalize_risk_level_expr("risk_level"))
    )

    quality_rules = {
        "merchant_id inválido o nulo": ~F.col("merchant_id").rlike(r"^MCH-\d{5}$") | F.col("merchant_id").isNull(),
        "merchant_name vacío o nulo": F.col("merchant_name").isNull(),
        "category fuera de catálogo": ~F.col("category").isin(*VALID_MERCHANT_CATEGORIES) | F.col("category").isNull(),
        "affiliation_date inválida": F.col("affiliation_date").isNull(),
        "status fuera de catálogo": ~F.col("status").isin(*VALID_MERCHANT_STATUS) | F.col("status").isNull(),
        "risk_level fuera de catálogo": F.col("risk_level").isNotNull() & ~F.col("risk_level").isin(*VALID_RISK_LEVELS),
    }

    return (
        add_processed_columns(with_quality_errors(cleaned_df, quality_rules))
        .select(
            "merchant_id",
            "merchant_name",
            "category",
            "country",
            "affiliation_date",
            "status",
            "risk_level",
            "is_valid",
            "quality_errors",
            "_cdc_operation",
            "_sequence_ts",
            "_record_hash",
            "_raw_record",
            "_source_file",
            "_ingested_at",
            "_processed_at",
            "_load_date",
        )
    )


def build_users_typed_changes():
    bronze_changes = dp.read_stream("bronze.users_changes")

    cleaned_df = (
        bronze_changes
        .withColumn("user_id", clean_string("user_id"))
        .withColumn("full_name", clean_string("full_name"))
        .withColumn("document_id", clean_string("document_id"))
        .withColumn("email", normalize_email_expr("email"))
        .withColumn("phone", normalize_phone_expr("phone"))
        .withColumn("country", normalize_country_expr("country"))
        .withColumn("segment", normalize_segment_expr("segment"))
        .withColumn("registration_date", parse_date_multi("registration_date"))
    )

    # Reglas críticas para usuarios.
    # No bloqueamos país, segmento, email, teléfono ni documento para evitar
    # enviar registros recuperables a cuarentena. Esos campos se conservan en
    # Silver para análisis y pueden auditarse con reglas no bloqueantes.
    quality_rules = {
        "user_id nulo": F.col("user_id").isNull(),
        "full_name vacío o nulo": F.col("full_name").isNull(),
        "registration_date inválida": F.col("registration_date").isNull(),
    }

    return (
        add_processed_columns(with_quality_errors(cleaned_df, quality_rules))
        .select(
            "user_id",
            "full_name",
            "document_id",
            "email",
            "phone",
            "country",
            "segment",
            "registration_date",
            "is_valid",
            "quality_errors",
            "_cdc_operation",
            "_sequence_ts",
            "_record_hash",
            "_raw_record",
            "_source_file",
            "_ingested_at",
            "_processed_at",
            "_load_date",
        )
    )


# ============================================================
# Bronze valid changes used by Silver AUTO CDC
# ============================================================

@dp.table(
    name="bronze.transactions_valid_changes",
    comment="Bronze typed valid transaction changes used as AUTO CDC source for silver.transactions.",
    table_properties={
        "quality": "bronze_valid_changes",
        "delta.enableChangeDataFeed": "true",
    },
)
def transactions_valid_changes():
    return (
        build_transactions_typed_changes()
        .filter(F.col("is_valid"))
        .drop("_raw_record")
    )


@dp.table(
    name="bronze.merchants_valid_changes",
    comment="Bronze typed valid merchant changes used as AUTO CDC source for silver.merchants.",
    table_properties={
        "quality": "bronze_valid_changes",
        "delta.enableChangeDataFeed": "true",
    },
)
def merchants_valid_changes():
    return (
        build_merchants_typed_changes()
        .filter(F.col("is_valid"))
        .drop("_raw_record")
    )


@dp.table(
    name="bronze.users_valid_changes",
    comment="Bronze typed valid user changes used as AUTO CDC source for silver.users.",
    table_properties={
        "quality": "bronze_valid_changes",
        "delta.enableChangeDataFeed": "true",
    },
)
def users_valid_changes():
    return (
        build_users_typed_changes()
        .filter(F.col("is_valid"))
        .drop("_raw_record")
    )


# ============================================================
# Bronze invalid changes used by Silver quarantine
# ============================================================

@dp.table(
    name="bronze.transactions_invalid_changes",
    comment="Bronze typed invalid transaction changes used as source for silver.quarantine.",
    table_properties={
        "quality": "bronze_invalid_changes",
        "delta.enableChangeDataFeed": "true",
    },
)
def transactions_invalid_changes():
    return build_transactions_typed_changes().filter(~F.col("is_valid"))


@dp.table(
    name="bronze.merchants_invalid_changes",
    comment="Bronze typed invalid merchant changes used as source for silver.quarantine.",
    table_properties={
        "quality": "bronze_invalid_changes",
        "delta.enableChangeDataFeed": "true",
    },
)
def merchants_invalid_changes():
    return build_merchants_typed_changes().filter(~F.col("is_valid"))


@dp.table(
    name="bronze.users_invalid_changes",
    comment="Bronze typed invalid user changes used as source for silver.quarantine.",
    table_properties={
        "quality": "bronze_invalid_changes",
        "delta.enableChangeDataFeed": "true",
    },
)
def users_invalid_changes():
    return build_users_typed_changes().filter(~F.col("is_valid"))
