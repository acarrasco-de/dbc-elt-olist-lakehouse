from pyspark import pipelines as dp
from pyspark.sql import functions as F

LANDING = spark.conf.get(
    "landing",
    "s3://databricks-datawizard-aaron-ext/landing/olist",
)


def _leer_csv(tabla: str):
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
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


@dp.table(
    name="geolocation_brz",
    comment="Geolocalización — Bronze, todo STRING, append-only",
    table_properties={"delta.appendOnly": "true"},
)
def geolocation_brz():
    return _leer_csv("geolocation")


@dp.table(
    name="product_category_brz",
    comment="Categorías de producto — Bronze, todo STRING, append-only",
    table_properties={"delta.appendOnly": "true"},
)
def product_category_brz():
    return _leer_csv("product_category")
