# src/utils.py
# FinPay Lakehouse - utilidades compartidas
#
# Arquitectura final:
#   Landing files
#      -> Bronze raw tables
#      -> Bronze CDC-like changes tables
#      -> Bronze typed valid/invalid changes
#      -> AUTO CDC SCD Type 1
#      -> Silver current tables
#
# Principios:
# - ingestion_archetypes.json controla rutas, formato, delimitador, schemaLocation y active.
# - Bronze raw y bronze changes mantienen columnas de negocio como STRING.
# - Bronze valid_changes contiene cambios ya tipados/validados para alimentar AUTO CDC.
# - NO usamos _change_type porque es columna reservada por Delta Change Data Feed.
# - Usamos _cdc_operation como operación CDC-like propia del proyecto.
# - Silver solo persiste current tables y quarantine.

from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, StringType


DEFAULT_CATALOG = "fintech_finpay_dev"
DEFAULT_DEFAULT_SCHEMA = "default"
DEFAULT_LANDING_VOLUME = "vol_landing"


# ============================================================
# Configuración metadata-driven
# ============================================================

def get_catalog(spark: SparkSession) -> str:
    return spark.conf.get("finpay.catalog", DEFAULT_CATALOG)


def get_landing_path(spark: SparkSession) -> str:
    catalog = get_catalog(spark)
    default_schema = spark.conf.get("finpay.default_schema", DEFAULT_DEFAULT_SCHEMA)
    landing_volume = spark.conf.get("finpay.landing_volume", DEFAULT_LANDING_VOLUME)
    return f"/Volumes/{catalog}/{default_schema}/{landing_volume}"


def get_archetypes_path(spark: SparkSession) -> str:
    default_path = f"{get_landing_path(spark)}/metadata/ingestion_archetypes.json"
    return spark.conf.get("finpay.ingestion_archetypes_path", default_path)


def load_ingestion_archetypes(spark: SparkSession) -> List[Dict]:
    """
    Devuelve configuración metadata-driven sin acciones Spark.

    En Lakeflow Declarative Pipelines no se deben usar acciones como collect()
    dentro de funciones decoradas. Por eso, el pipeline reconstruye la misma
    configuración del ingestion_archetypes.json usando variables del bundle.

    El JSON sigue siendo creado por 00_setup como contrato/evidencia metadata-driven.
    """
    landing_path = get_landing_path(spark)
    catalog = get_catalog(spark)

    return [
        {
            "source_name": "transactions",
            "source_path": f"{landing_path}/transactions/",
            "file_format": "csv",
            "delimiter": ",",
            "header": True,
            "multiline": False,
            "schema_location": f"{landing_path}/metadata/schema/transactions/",
            "checkpoint_path": f"{landing_path}/metadata/checkpoints/transactions/",
            "partition_by": "transaction_date",
            "target_table": f"{catalog}.bronze.transactions",
            "active": True,
        },
        {
            "source_name": "merchants",
            "source_path": f"{landing_path}/merchants/",
            "file_format": "json",
            "delimiter": None,
            "header": None,
            "multiline": True,
            "schema_location": f"{landing_path}/metadata/schema/merchants/",
            "checkpoint_path": f"{landing_path}/metadata/checkpoints/merchants/",
            "partition_by": "country",
            "target_table": f"{catalog}.bronze.merchants",
            "active": True,
        },
        {
            "source_name": "users",
            "source_path": f"{landing_path}/users/",
            "file_format": "csv",
            "delimiter": "|",
            "header": True,
            "multiline": False,
            "schema_location": f"{landing_path}/metadata/schema/users/",
            "checkpoint_path": f"{landing_path}/metadata/checkpoints/users/",
            "partition_by": "country",
            "target_table": f"{catalog}.bronze.users",
            "active": True,
        },
    ]


def get_source_config(spark: SparkSession, source_name: str) -> Dict:
    configs = load_ingestion_archetypes(spark)

    matches = [
        cfg for cfg in configs
        if cfg.get("source_name") == source_name and bool(cfg.get("active", False))
    ]

    if not matches:
        raise ValueError(
            f"No se encontró configuración activa para source_name='{source_name}' "
            f"en configuración metadata-driven del pipeline"
        )

    return matches[0]


# ============================================================
# Esquemas Bronze explícitos
# ============================================================

def bronze_business_columns(source_name: str) -> List[str]:
    columns_by_source = {
        "transactions": [
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
        ],
        "merchants": [
            "merchant_id",
            "merchant_name",
            "category",
            "country",
            "affiliation_date",
            "status",
            "risk_level",
        ],
        "users": [
            "user_id",
            "full_name",
            "document_id",
            "email",
            "phone",
            "country",
            "segment",
            "registration_date",
        ],
    }

    if source_name not in columns_by_source:
        raise ValueError(f"Fuente no soportada: {source_name}")

    return columns_by_source[source_name]


def bronze_schema(source_name: str) -> StructType:
    return StructType([
        StructField(column_name, StringType(), True)
        for column_name in bronze_business_columns(source_name)
    ])


def _bool_as_str(value: Optional[bool], default: str = "false") -> str:
    if value is None:
        return default
    return str(bool(value)).lower()


# ============================================================
# Lectura Bronze con Auto Loader
# ============================================================

def read_autoloader_stream(spark: SparkSession, source_name: str) -> DataFrame:
    cfg = get_source_config(spark, source_name)

    file_format = cfg.get("file_format")
    if source_name == "users" and file_format == "text":
        file_format = "csv"

    reader = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", file_format)
        .option("cloudFiles.schemaLocation", cfg["schema_location"])
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("rescuedDataColumn", "_rescued_data")
        .schema(bronze_schema(source_name))
    )

    if file_format == "csv":
        reader = (
            reader
            .option("header", _bool_as_str(cfg.get("header"), "true"))
            .option("delimiter", cfg.get("delimiter") or ",")
            .option("mode", "PERMISSIVE")
        )

    if file_format == "json":
        reader = (
            reader
            .option("multiLine", _bool_as_str(cfg.get("multiline"), "false"))
            .option("mode", "PERMISSIVE")
        )

    return reader.load(cfg["source_path"])


def add_bronze_audit_columns(df: DataFrame, source_name: str) -> DataFrame:
    business_cols = bronze_business_columns(source_name)

    for column_name in business_cols:
        if column_name not in df.columns:
            df = df.withColumn(column_name, F.lit(None).cast("string"))
        else:
            df = df.withColumn(column_name, F.col(column_name).cast("string"))

    if "_rescued_data" not in df.columns:
        df = df.withColumn("_rescued_data", F.lit(None).cast("string"))
    else:
        df = df.withColumn("_rescued_data", F.col("_rescued_data").cast("string"))

    raw_struct = F.struct(*[F.col(c).alias(c) for c in business_cols])

    return (
        df
        .withColumn("_raw_record", F.to_json(raw_struct))
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_load_date", F.current_date())
        .select(
            *business_cols,
            "_rescued_data",
            "_raw_record",
            "_source_file",
            "_ingested_at",
            "_load_date",
        )
    )


def add_bronze_change_columns(df: DataFrame, source_name: str) -> DataFrame:
    """
    Genera eventos CDC-like persistidos en Bronze.

    Como los archivos fuente no traen operation/is_deleted, todos los registros se modelan como UPSERT.
    """
    business_cols = bronze_business_columns(source_name)
    hash_struct = F.struct(*[F.col(c).cast("string").alias(c) for c in business_cols])

    return (
        df
        .withColumn("_cdc_operation", F.lit("UPSERT"))
        .withColumn("_sequence_ts", F.col("_ingested_at"))
        .withColumn("_record_hash", F.sha2(F.to_json(hash_struct), 256))
        .select(
            *business_cols,
            "_cdc_operation",
            "_sequence_ts",
            "_record_hash",
            "_rescued_data",
            "_raw_record",
            "_source_file",
            "_ingested_at",
            "_load_date",
        )
    )


# ============================================================
# Silver/Bronze valid changes: catálogos
# ============================================================

VALID_COUNTRIES = ["PE", "CO", "MX", "CL", "AR"]

VALID_CHANNELS = ["web", "app", "pos"]
VALID_TRANSACTION_TYPES = ["pago", "reversa", "retiro"]
VALID_TRANSACTION_STATUS = ["aprobado", "rechazado", "pendiente"]
VALID_CURRENCIES = ["PEN", "USD", "COP", "MXN", "CLP", "ARS"]

VALID_MERCHANT_CATEGORIES = [
    "retail",
    "restaurante",
    "farmacia",
    "supermercado",
    "tecnologia",
    "transporte",
    "educacion",
    "salud",
    "entretenimiento",
    "moda",
]
VALID_MERCHANT_STATUS = ["activo", "inactivo", "suspendido"]
VALID_RISK_LEVELS = ["bajo", "medio", "alto"]

VALID_USER_SEGMENTS = ["premium", "estandar", "nuevo"]


# ============================================================
# Normalización y casteo
# ============================================================

def clean_string(column_name: str):
    value = F.trim(F.col(column_name).cast("string"))

    return (
        F.when(value.isNull(), F.lit(None).cast("string"))
        .when(value == "", F.lit(None).cast("string"))
        .when(
            F.upper(value).isin(
                "N/A",
                "NA",
                "NULL",
                "NONE",
                "NODISPONIBLE",
                "NO DISPONIBLE",
                "NO_DISPONIBLE",
                "SIN_CORREO",
                "SIN CORREO",
                "SIN NOMBRE",
                "DESCONOCIDO",
            ),
            F.lit(None).cast("string"),
        )
        .otherwise(value)
    )


def lower_clean(column_name: str):
    return F.lower(clean_string(column_name))


def parse_date_multi(column_name: str):
    value = clean_string(column_name)

    return F.coalesce(
        F.to_date(value, "yyyy-MM-dd"),
        F.to_date(value, "dd/MM/yyyy"),
        F.to_date(value, "yyyy/MM/dd"),
        F.to_date(value, "dd-MM-yyyy"),
    )


def parse_amount(column_name: str):
    value = F.regexp_replace(clean_string(column_name), r"[^0-9,.\-]", "")

    normalized = (
        F.when(value.rlike(r"^\d{1,3}(,\d{3})+(\.\d+)?$"), F.regexp_replace(value, ",", ""))
        .when(value.rlike(r"^\d+,\d{1,2}$"), F.regexp_replace(value, ",", "."))
        .otherwise(value)
    )

    return normalized.cast("decimal(18,2)")


def normalize_country_expr(column_name: str):
    value = F.upper(F.trim(clean_string(column_name)))

    return (
        F.when(value.isNull(), F.lit(None).cast("string"))
        .when(value.isin("PE", "PER", "PERU"), F.lit("PE"))
        .when(value.isin("CO", "COL", "COLOMBIA"), F.lit("CO"))
        .when(value.isin("MX", "MEX", "MEXICO"), F.lit("MX"))
        .when(value.isin("CL", "CHI", "CHILE"), F.lit("CL"))
        .when(value.isin("AR", "ARG", "ARGENTINA"), F.lit("AR"))
        .otherwise(value)
    )


def normalize_status_expr(column_name: str, allowed_values: List[str]):
    value = lower_clean(column_name)

    value = (
        F.when(value == "active", F.lit("activo"))
        .when(value == "suspended", F.lit("suspendido"))
        .otherwise(value)
    )

    return F.when(value.isin(*allowed_values), value).otherwise(value)


def normalize_risk_level_expr(column_name: str):
    value = lower_clean(column_name)

    return (
        F.when(value.isin("sin clasificar", "n/a", "na"), F.lit(None).cast("string"))
        .when(value.isin(*VALID_RISK_LEVELS), value)
        .otherwise(value)
    )


def normalize_segment_expr(column_name: str):
    value = lower_clean(column_name)
    return F.when(value.isin(*VALID_USER_SEGMENTS), value).otherwise(value)


def normalize_email_expr(column_name: str):
    return F.lower(clean_string(column_name))


def normalize_phone_expr(column_name: str):
    value = clean_string(column_name)
    digits = F.regexp_replace(value, r"[^0-9]", "")

    return (
        F.when(value.isNull(), F.lit(None).cast("string"))
        .when(value.startswith("+"), F.concat(F.lit("+"), digits))
        .when(digits.startswith("51"), F.concat(F.lit("+"), digits))
        .when(F.length(digits) == 9, F.concat(F.lit("+51"), digits))
        .otherwise(digits)
    )


def with_quality_errors(df: DataFrame, rules: Dict[str, object]) -> DataFrame:
    """
    Agrega quality_errors e is_valid.
    """
    error_items = [
        F.when(condition, F.lit(message)).otherwise(F.lit(None).cast("string"))
        for message, condition in rules.items()
    ]

    raw_errors = F.array(*error_items)
    clean_errors = F.filter(raw_errors, lambda error: error.isNotNull())

    return (
        df
        .withColumn("quality_errors", clean_errors)
        .withColumn("is_valid", F.size(F.col("quality_errors")) == 0)
    )


def add_processed_columns(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("_processed_at", F.current_timestamp())
        .withColumn("_load_date", F.current_date())
    )
