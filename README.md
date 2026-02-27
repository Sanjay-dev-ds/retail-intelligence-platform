### Retail Intelligence Platform

End‑to‑end retail analytics demo using **Airflow (Astronomer)**, **BigQuery**, **dbt (medallion architecture)**, and **Terraform**.

### High‑level architecture

- **Synthetic data generator** (`dags/synthetic_sales_generator.py`) produces products, users, orders, and order items.
- **Bronze DAG** (`dags/retail_data_pipeline.py`) lands JSON to GCS and loads **bronze** BigQuery tables.
- **dbt project** (`dags/dbt_retail`) builds **silver** (staging) and **gold** (dimensions, fact, aggregations) layers.
- **Cosmos dbt DAG** (`dags/retail_dbt_medallion.py`) runs the dbt project after the bronze load.

Image placeholders (replace with your own diagrams/screenshots):

- **Architecture diagram**: `[architecture-diagram](./docs/img/architecture.png)`
- **Airflow workflows**: `[airflow-workflows](./docs/img/airflow_workflows.png)`
- **BigQuery schema**: `[bigquery-schema](./docs/img/bigquery_schema.png)`
- **dbt lineage**: `[dbt-lineage](./docs/img/dbt_lineage.png)`

### Prerequisites

- Docker and **Astro CLI** installed.
- A GCP project and BigQuery enabled.
- Service account with BigQuery + GCS access (mounted in the container or available via ADC).

### Running locally with Astronomer

- **Start Airflow**

```bash
astro dev start
```

Open the UI at `http://localhost:8080`.

- **Stop Airflow**

```bash
astro dev stop
```

### Airflow configuration

- **Variable**  
  - `RETAIL_GCS_BUCKET` → your GCS bucket name.

- **Connection**  
  - `google_cloud_default` → points to your GCP project / credentials (used by both the bronze DAG and dbt via Cosmos).

### Terraform (GCS + IAM)

Terraform files live in `terraform/`. From the repo root:

```bash
cd terraform
terraform init
terraform apply
```

Use the outputs to:

- Configure the GCS bucket for `RETAIL_GCS_BUCKET`.
- Configure the service account used by Airflow for GCP access.

### dbt project (medallion)

- Project root: `dags/dbt_retail`
- Layers:
  - `bronze`: BigQuery sources (`retail_bronze.*`).
  - `silver`: staging models (`stg_*`).
  - `gold`: `dim_*`, `fct_sales`, `agg_*`.

You can run dbt manually from inside the scheduler/webserver container (optional):

```bash
astro dev bash
cd /usr/local/airflow/dags/dbt_retail
dbt run
dbt test
```
