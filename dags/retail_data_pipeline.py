import json
from datetime import datetime

from airflow.decorators import dag, task , task_group
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.models.baseoperator import chain

from google.cloud import bigquery

from synthetic_sales_generator import (
    fetch_all_products,
    fetch_all_users,
    generate_sales_for_day,
)

GCS_BUCKET_VARIABLE_KEY = "RETAIL_GCS_BUCKET"
GCS_BASE_PATH = "retail"
GCP_CONN_ID = "google_cloud_default"
BQ_PROJECT = "project-4d474cb9-a1a5-457f-80b"
BQ_DATASET_BRONZE = "retail_bronze"


def _get_bucket_name() -> str:
    return Variable.get(GCS_BUCKET_VARIABLE_KEY)


def _upload_json_to_gcs(data, object_name: str, gcp_conn_id: str = GCP_CONN_ID) -> str:
    bucket_name = _get_bucket_name()
    hook = GCSHook(gcp_conn_id=gcp_conn_id)
    ndjson = "\n".join([json.dumps(row) for row in data])
    hook.upload(
        bucket_name=bucket_name,
        object_name=object_name,
        data=ndjson,
        mime_type="application/json",
    )
    return f"gs://{bucket_name}/{object_name}"


def _add_audit_columns(data, source: str):
    ingestion_ts = datetime.utcnow().isoformat()
    for row in data:
        row["ingestion_ts"] = ingestion_ts
        row["source"] = source
    return data



def _load_gcs_to_bigquery(
    gcs_uri: str,
    dataset: str,
    table: str,
    write_disposition: str = "WRITE_APPEND",
):
    hook = BigQueryHook(gcp_conn_id="google_cloud_default")
    client = hook.get_client()  # returns a google.cloud.bigquery.Client

    destination = f"{BQ_PROJECT}.{dataset}.{table}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=write_disposition,
        autodetect=True,
    )

    load_job = client.load_table_from_uri(
        gcs_uri,
        destination,
        job_config=job_config,
    )

    load_job.result()  # Wait for the job to complete

@dag(
    dag_id="retail_bronze_to_bigquery",
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 2 * * *",
    catchup=False,
    tags=["retail", "bronze", "bigquery"],
)
def retail_bronze_to_bigquery():

    @task
    def extract_products():
        context = get_current_context()
        ds = context["ds"]
        products = fetch_all_products()
        products = _add_audit_columns(products, source="DummyJSON")
        object_name = f"{GCS_BASE_PATH}/products/dt={ds}/products.json"
        gcs_uri = _upload_json_to_gcs(products, object_name)
        return gcs_uri  # FIX 1: Return only the URI, keep it simple

    @task
    def extract_users():
        context = get_current_context()
        ds = context["ds"]
        users = fetch_all_users()
        users = _add_audit_columns(users, source="DummyJSON")
        object_name = f"{GCS_BASE_PATH}/users/dt={ds}/users.json"
        gcs_uri = _upload_json_to_gcs(users, object_name)
        return gcs_uri  # FIX 1: Return only the URI

    @task(multiple_outputs=True) 
    def generate_orders_and_items(products_gcs_uri: str, users_gcs_uri: str):
        context = get_current_context()
        logical_date = context["logical_date"]

        gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
        bucket_name = _get_bucket_name()

        products_object = products_gcs_uri.replace(f"gs://{bucket_name}/", "")
        products_bytes = gcs_hook.download(bucket_name=bucket_name, object_name=products_object)
        products = [json.loads(line) for line in products_bytes.decode("utf-8").splitlines() if line]

        users_object = users_gcs_uri.replace(f"gs://{bucket_name}/", "")
        users_bytes = gcs_hook.download(bucket_name=bucket_name, object_name=users_object)
        users = [json.loads(line) for line in users_bytes.decode("utf-8").splitlines() if line]

        orders, order_items = generate_sales_for_day(
            target_date=logical_date,
            products=products,
            users=users,
        )

        orders = _add_audit_columns(orders, source="synthetic_sales_generator")
        order_items = _add_audit_columns(order_items, source="synthetic_sales_generator")

        orders_object = f"{GCS_BASE_PATH}/orders/dt={logical_date.date()}/orders.json"
        order_items_object = f"{GCS_BASE_PATH}/order_items/dt={logical_date.date()}/order_items.json"

        orders_gcs_uri = _upload_json_to_gcs(orders, orders_object)
        order_items_gcs_uri = _upload_json_to_gcs(order_items, order_items_object)

        return {
            "orders_gcs_uri": orders_gcs_uri,
            "order_items_gcs_uri": order_items_gcs_uri,
            "products_gcs_uri": products_gcs_uri,
            "users_gcs_uri": users_gcs_uri,
        }

    @task
    def load_users_to_bq(gcs_uri: str):
        _load_gcs_to_bigquery(gcs_uri=gcs_uri, dataset="retail_bronze", table="users", write_disposition="WRITE_TRUNCATE")

    @task
    def load_products_to_bq(gcs_uri: str):
        _load_gcs_to_bigquery(gcs_uri=gcs_uri, dataset="retail_bronze", table="products", write_disposition="WRITE_TRUNCATE")

    @task
    def load_orders_to_bq(gcs_uri: str):
        _load_gcs_to_bigquery(gcs_uri=gcs_uri, dataset="retail_bronze", table="orders", write_disposition="WRITE_APPEND")

    @task
    def load_order_items_to_bq(gcs_uri: str):
        _load_gcs_to_bigquery(gcs_uri=gcs_uri, dataset="retail_bronze", table="order_items", write_disposition="WRITE_APPEND")

    # -------------------------
    # Orchestration
    # -------------------------
    # 1. Extract raw data to GCS
    products_gcs_uri = extract_products()
    users_gcs_uri = extract_users()

    # 2. Load raw (bronze) entities to BigQuery
    load_users_to_bq(users_gcs_uri)
    load_products_to_bq(products_gcs_uri)

    # 3. Generate orders and order_items from products/users
    sales_output = generate_orders_and_items(products_gcs_uri, users_gcs_uri)

    # 4. Load generated fact data to BigQuery
    load_orders_to_bq(sales_output["orders_gcs_uri"])
    load_order_items_to_bq(sales_output["order_items_gcs_uri"])


retail_bronze_to_bigquery()