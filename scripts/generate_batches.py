"""
Generador de batches incrementales para el pipeline Olist Lakehouse.

Propósito: simular un sistema fuente que entrega datos en lotes sucesivos
e inyectar problemas intencionales para probar que Silver los maneja bien.

Problemas inyectados por diseño:
  - DUPLICADOS: mismo order_id dos veces → Silver debe deduplicar
  - SCD2 TRIGGER: mismo customer_id/seller_id con ciudad distinta → Silver debe
    cerrar la fila histórica y abrir una nueva
  - NULLS en columna esperada: order_status nulo → expectation debe descartar la fila

Genera 3 batches por cada tabla relevante y los sube a S3 en landing/.

Uso:
    pip install boto3 pandas
    python scripts/generate_batches.py

Requiere credenciales de AWS configuradas (aws configure o variables de entorno).
"""

import io
import random
import uuid
from datetime import datetime, timedelta

import boto3
import pandas as pd

BUCKET = "databricks-datawizard-aaron-ext"
LANDING = "landing/olist"

ORIGINAL_FILES = {
    "orders":       "olist_orders_dataset.csv",
    "customers":    "olist_customers_dataset.csv",
    "sellers":      "olist_sellers_dataset.csv",
    "order_items":  "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews":  "olist_order_reviews_dataset.csv",
    "products":     "olist_products_dataset.csv",
}

CITIES_BR = [
    ("sao paulo", "SP"), ("rio de janeiro", "RJ"), ("belo horizonte", "MG"),
    ("curitiba", "PR"), ("porto alegre", "RS"), ("salvador", "BA"),
    ("fortaleza", "CE"), ("recife", "PE"), ("manaus", "AM"), ("goiania", "GO"),
    ("campinas", "SP"), ("guarulhos", "SP"), ("maceio", "AL"), ("natal", "RN"),
]

STATUSES = ["created", "approved", "invoiced", "processing",
            "shipped", "delivered", "canceled", "unavailable"]


# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------

def _read(s3, table: str, filename: str) -> pd.DataFrame:
    key = f"{LANDING}/{table}/{filename}"
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    return pd.read_csv(io.BytesIO(obj["Body"].read()), dtype=str)


def _write(s3, df: pd.DataFrame, table: str, batch_name: str) -> None:
    buf = io.BytesIO()
    df.to_csv(buf, index=False)
    key = f"{LANDING}/{table}/{batch_name}.csv"
    s3.put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue())
    print(f"  -> s3://{BUCKET}/{key}  ({len(df)} filas)")


def _now_str(offset_days: int = 0) -> str:
    dt = datetime.utcnow() + timedelta(days=offset_days)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


# ---------------------------------------------------------------------------
# Generadores por tabla
# ---------------------------------------------------------------------------

def _gen_orders(orders: pd.DataFrame, existing_ids: list[str],
                batch_num: int) -> pd.DataFrame:
    """
    - 80 pedidos nuevos (order_status aleatorio, fechas recientes)
    - 10 duplicados exactos de filas ya existentes (prueba de dedup en Silver)
    - 10 pedidos con order_status nulo (deben ser descartados por expectation)
    """
    rows = []

    # 80 pedidos nuevos
    sample = orders.sample(n=min(80, len(orders)), replace=True)
    for _, row in sample.iterrows():
        r = row.to_dict()
        r["order_id"] = str(uuid.uuid4()).replace("-", "")
        r["customer_id"] = str(uuid.uuid4()).replace("-", "")
        r["order_status"] = random.choice(STATUSES)
        r["order_purchase_timestamp"] = _now_str(-random.randint(0, 7))
        rows.append(r)

    # 10 duplicados exactos de IDs existentes
    dup_ids = random.sample(existing_ids, min(10, len(existing_ids)))
    dup_rows = orders[orders["order_id"].isin(dup_ids)]
    rows.extend(dup_rows.to_dict("records"))

    # 10 pedidos con order_status nulo
    for _ in range(10):
        r = orders.sample(1).iloc[0].to_dict()
        r["order_id"] = str(uuid.uuid4()).replace("-", "")
        r["order_status"] = None          # expectation debe descartar esto
        rows.append(r)

    return pd.DataFrame(rows, columns=orders.columns)


def _gen_customers(customers: pd.DataFrame, batch_num: int) -> pd.DataFrame:
    """
    - 50 clientes nuevos
    - 20 clientes existentes con ciudad/estado distinto (SCD2 trigger)
    """
    rows = []

    # 50 nuevos
    for _ in range(50):
        city, state = random.choice(CITIES_BR)
        rows.append({
            "customer_id":           str(uuid.uuid4()).replace("-", ""),
            "customer_unique_id":    str(uuid.uuid4()).replace("-", ""),
            "customer_zip_code_prefix": str(random.randint(10000, 99999)),
            "customer_city":         city,
            "customer_state":        state,
        })

    # 20 existentes con dirección cambiada (SCD2 trigger)
    sample = customers.sample(n=min(20, len(customers)))
    for _, row in sample.iterrows():
        r = row.to_dict()
        new_city, new_state = random.choice(CITIES_BR)
        r["customer_city"] = new_city + f"_updated_b{batch_num}"
        r["customer_state"] = new_state
        r["customer_zip_code_prefix"] = str(random.randint(10000, 99999))
        rows.append(r)

    return pd.DataFrame(rows, columns=customers.columns)


def _gen_sellers(sellers: pd.DataFrame, batch_num: int) -> pd.DataFrame:
    """
    - 20 sellers nuevos
    - 10 sellers existentes con ciudad/estado distinto (SCD2 trigger)
    """
    rows = []

    # 20 nuevos
    for _ in range(20):
        city, state = random.choice(CITIES_BR)
        rows.append({
            "seller_id":              str(uuid.uuid4()).replace("-", ""),
            "seller_zip_code_prefix": str(random.randint(10000, 99999)),
            "seller_city":            city,
            "seller_state":           state,
        })

    # 10 existentes con dirección cambiada (SCD2 trigger)
    sample = sellers.sample(n=min(10, len(sellers)))
    for _, row in sample.iterrows():
        r = row.to_dict()
        new_city, new_state = random.choice(CITIES_BR)
        r["seller_city"] = new_city + f"_updated_b{batch_num}"
        r["seller_state"] = new_state
        rows.append(r)

    return pd.DataFrame(rows, columns=sellers.columns)


def _gen_order_items(order_items: pd.DataFrame,
                     new_order_ids: list[str]) -> pd.DataFrame:
    """Items para los nuevos pedidos del batch (1-3 items por pedido)."""
    rows = []
    products = order_items["product_id"].dropna().unique().tolist()
    sellers  = order_items["seller_id"].dropna().unique().tolist()

    for oid in new_order_ids:
        n_items = random.randint(1, 3)
        for seq in range(1, n_items + 1):
            price = round(random.uniform(10, 500), 2)
            rows.append({
                "order_id":          oid,
                "order_item_id":     str(seq),
                "product_id":        random.choice(products),
                "seller_id":         random.choice(sellers),
                "shipping_limit_date": _now_str(random.randint(3, 15)),
                "price":             str(price),
                "freight_value":     str(round(price * 0.12, 2)),
            })

    return pd.DataFrame(rows, columns=order_items.columns)


def _gen_order_payments(order_payments: pd.DataFrame,
                        new_order_ids: list[str]) -> pd.DataFrame:
    """Un pago por pedido nuevo."""
    types = ["credit_card", "boleto", "voucher", "debit_card"]
    rows = []
    for oid in new_order_ids:
        rows.append({
            "order_id":               oid,
            "payment_sequential":     "1",
            "payment_type":           random.choice(types),
            "payment_installments":   str(random.randint(1, 12)),
            "payment_value":          str(round(random.uniform(20, 800), 2)),
        })
    return pd.DataFrame(rows, columns=order_payments.columns)


def _gen_order_reviews(order_reviews: pd.DataFrame,
                       new_order_ids: list[str]) -> pd.DataFrame:
    """Reseña opcional (70% de probabilidad) para pedidos nuevos."""
    rows = []
    for oid in new_order_ids:
        if random.random() > 0.3:
            rows.append({
                "review_id":                   str(uuid.uuid4()).replace("-", ""),
                "order_id":                    oid,
                "review_score":                str(random.randint(1, 5)),
                "review_comment_title":        "",
                "review_comment_message":      "",
                "review_creation_date":        _now_str(-random.randint(0, 5)),
                "review_answer_timestamp":     _now_str(-random.randint(0, 2)),
            })
    return pd.DataFrame(rows, columns=order_reviews.columns)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    s3 = boto3.client("s3")

    print("Leyendo datasets originales desde S3...")
    orders   = _read(s3, "orders",   ORIGINAL_FILES["orders"])
    customers = _read(s3, "customers", ORIGINAL_FILES["customers"])
    sellers  = _read(s3, "sellers",  ORIGINAL_FILES["sellers"])
    items    = _read(s3, "order_items", ORIGINAL_FILES["order_items"])
    payments = _read(s3, "order_payments", ORIGINAL_FILES["order_payments"])
    reviews  = _read(s3, "order_reviews",  ORIGINAL_FILES["order_reviews"])

    existing_order_ids = orders["order_id"].dropna().tolist()

    for batch_num in range(1, 4):
        batch_name = f"batch_{batch_num:03d}"
        print(f"\n{'='*50}")
        print(f"Generando {batch_name}...")
        print('='*50)

        orders_batch = _gen_orders(orders, existing_order_ids, batch_num)
        # solo los IDs nuevos (no duplicados) para relacionar con items/pagos/reviews
        new_ids = [
            r["order_id"] for r in orders_batch.to_dict("records")
            if r["order_id"] not in existing_order_ids
        ]

        _write(s3, orders_batch,                        "orders",         batch_name)
        _write(s3, _gen_customers(customers, batch_num), "customers",      batch_name)
        _write(s3, _gen_sellers(sellers, batch_num),     "sellers",        batch_name)
        _write(s3, _gen_order_items(items, new_ids),     "order_items",    batch_name)
        _write(s3, _gen_order_payments(payments, new_ids), "order_payments", batch_name)
        _write(s3, _gen_order_reviews(reviews, new_ids), "order_reviews",  batch_name)

    print("\nListo. Ahora puedes correr el pipeline de Bronze en Databricks.")
    print("Verifica en Silver que:")
    print("  - Los duplicados de orders no aparecen dos veces")
    print("  - Los customers con ciudad _updated_bN tienen fila histórica cerrada")
    print("  - Los orders con order_status=NULL fueron descartados por la expectation")


if __name__ == "__main__":
    main()
