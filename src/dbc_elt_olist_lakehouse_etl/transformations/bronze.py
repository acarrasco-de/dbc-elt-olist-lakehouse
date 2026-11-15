# Bronze layer — Olist Lakehouse.
#
# Patrón del Wizard Bank:
#   - pyspark.pipelines as dp + @dp.table
#   - Autoloader: todo STRING (sin inferColumnTypes), header=True
#   - Columnas de auditoría: _archivo (_metadata.file_path) e _ingestado_en
#   - appendOnly = true (Bronze es inmutable)
#   - Landing path viene de la configuración del pipeline (Settings → Configuration)
#   - schemaLocation en carpeta schemas/ del mismo bucket

from pyspark import pipelines as dp
from pyspark.sql import functions as F

LANDING = spark.conf.get(
    "landing",
    "s3://databricks-datawizard-aaron-ext/landing/olist",
)
SCHEMA_STORE = spark.conf.get(
    "schema_store",
    "s3://databricks-datawizard-aaron-ext/schemas",
)


def _leer_csv(tabla: str):
    """Autoloader: lee todos los CSVs nuevos de landing/<tabla>/ como STRING."""
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{SCHEMA_STORE}/{tabla}")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("header", "true")
        .option("rescuedDataColumn", "_rescued")
        .load(f"{LANDING}/{tabla}/")
        .withColumn("_archivo",      F.col("_metadata.file_path"))
        .withColumn("_ingestado_en", F.current_timestamp())
    )


# ---------------------------------------------------------------------------
# Tablas de hechos
# ---------------------------------------------------------------------------

@dp.table(
    name="orders_brz",
    comment="Pedidos Olist — Bronze, todo STRING, append-only",
    table_properties={"delta.appendOnly": "true"},
)
def orders_brz():
    return _leer_csv("orders")


@dp.table(
    name="order_items_brz",
    comment="Ítems de pedido — Bronze, todo STRING, append-only",
    table_properties={"delta.appendOnly": "true"},
)
def order_items_brz():
    return _leer_csv("order_items")


@dp.table(
    name="order_payments_brz",
    comment="Pagos de pedido — Bronze, todo STRING, append-only",
    table_properties={"delta.appendOnly": "true"},
)
def order_payments_brz():
    return _leer_csv("order_payments")


@dp.table(
    name="order_reviews_brz",
    comment="Reseñas de pedido — Bronze, todo STRING, append-only",
    table_properties={"delta.appendOnly": "true"},
)
def order_reviews_brz():
    return _leer_csv("order_reviews")


# ---------------------------------------------------------------------------
# Tablas maestras (candidatos SCD2 en Silver)
# ---------------------------------------------------------------------------

@dp.table(
    name="customers_brz",
    comment="Clientes — Bronze, todo STRING, append-only",
    table_properties={"delta.appendOnly": "true"},
)
def customers_brz():
    return _leer_csv("customers")


@dp.table(
    name="sellers_brz",
    comment="Vendedores — Bronze, todo STRING, append-only",
    table_properties={"delta.appendOnly": "true"},
)
def sellers_brz():
    return _leer_csv("sellers")


@dp.table(
    name="products_brz",
    comment="Productos — Bronze, todo STRING, append-only",
    table_properties={"delta.appendOnly": "true"},
)
def products_brz():
    return _leer_csv("products")
