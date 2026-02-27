variable "project_id" {
  description = "GCP project ID where resources will be created"
  type        = string
}

variable "region" {
  description = "Default GCP region"
  type        = string
  default     = "us-central1"
}

variable "bucket_location" {
  description = "Location/region for the GCS bucket"
  type        = string
  default     = "US"
}

variable "gcs_bucket_name" {
  description = "Name of the GCS bucket for retail data"
  type        = string
}

