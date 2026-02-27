output "gcs_bucket_name" {
  description = "Retail data GCS bucket name"
  value       = google_storage_bucket.retail_data.name
}
