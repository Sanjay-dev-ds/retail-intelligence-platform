### Retail Intelligence Platform – Airflow + GCS + Terraform

This project generates synthetic retail sales data and orchestrates it with Airflow (e.g. on Astronomer), writing the results to Google Cloud Storage (GCS). Terraform is used to provision the GCS bucket and a service account for Airflow.

### Data flow

- **Sources** (via `synthetic_sales_generator.py` using DummyJSON APIs):
  - **Products**
  - **Users**
  - **Orders**
  - **Order items**
- **Orchestration**:
  - Airflow DAG: `dags/retail_sales_to_gcs_dag.py`
  - Daily run (cron `0 2 * * *`) generates data for the Airflow logical date.
- **Targets (GCS)** – all written as JSON:
  - `gs://<bucket>/retail/products/products_{{ ds }}.json`
  - `gs://<bucket>/retail/users/users_{{ ds }}.json`
  - `gs://<bucket>/retail/orders/orders_{{ ds }}.json`
  - `gs://<bucket>/retail/order_items/order_items_{{ ds }}.json`

### Airflow / Astronomer setup

- **Dependencies**:
  - Install from `requirements.txt` in your Astronomer project (or Airflow image):
    - `requests`
    - `google-cloud-storage`
    - `apache-airflow-providers-google`
- **DAG**:
  - Copy `dags/retail_sales_to_gcs_dag.py` into your Astronomer project’s `dags` folder.
  - Ensure `synthetic_sales_generator.py` is importable from the same project root (as in this repo).
- **Configuration**:
  - Create an Airflow Variable named `RETAIL_GCS_BUCKET` with the name of the GCS bucket created by Terraform.
  - Configure your Airflow/Astronomer deployment to use the Terraform-created service account (see outputs below) as its workload identity / credentials, so `google-cloud-storage` can authenticate via Application Default Credentials.

### Terraform infrastructure

Terraform files live in the `terraform/` directory:

- `main.tf` – Google provider, GCS bucket, service account, and IAM binding.
- `variables.tf` – Input variables.
- `outputs.tf` – Bucket name and service account email.

**Key resources:**

- **GCS bucket**: `google_storage_bucket.retail_data`
  - Holds all retail JSON data.
  - Has a simple lifecycle rule to delete objects older than 30 days (adjust as needed).
- **Service account**: `google_service_account.airflow_gcs`
  - Granted `roles/storage.objectAdmin` on the bucket.
  - Use this service account for your Airflow/Astronomer deployment.

### How to apply Terraform

From the `terraform/` directory:

```bash
cd terraform

# Set your project/bucket details (example)
cat > terraform.tfvars <<EOF
project_id      = "your-gcp-project-id"
region          = "us-central1"
bucket_location = "US"
gcs_bucket_name = "your-retail-data-bucket"
EOF

terraform init
terraform plan
terraform apply
```

After `terraform apply`:

- Note the `gcs_bucket_name` output and set it as the Airflow Variable `RETAIL_GCS_BUCKET`.
- Note the `airflow_service_account_email` output and configure your Astronomer/Airflow deployment to use that identity (e.g. via Workload Identity / Kubernetes secret, depending on your environment).

### Running the pipeline

1. **Provision infra** with Terraform as above.
2. **Deploy code** to Astronomer / Airflow:
   - Include `synthetic_sales_generator.py`, `dags/retail_sales_to_gcs_dag.py`, and `requirements.txt` in your project.
3. **Configure Airflow**:
   - Set Variable `RETAIL_GCS_BUCKET` to the Terraform-created bucket name.
   - Ensure Airflow is running with the Terraform-created service account credentials.
4. **Enable the DAG** `retail_sales_to_gcs` in the Airflow UI.
5. On each run, Airflow will generate and push **products**, **users**, **orders**, and **order items** JSON files to the configured GCS bucket.

