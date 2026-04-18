variable "project" {
  description = "The Google Cloud Project ID where resources will be created."
  type        = string
  default     = "air-quality-project-491604" # Replace with your actual Project ID
}


variable "region" {
  description = "The GCP region for regional resources (like the GCS bucket)."
  type        = string
  default     = "us-central1"
}

variable "location" {
  description = "The multi-region or region location for BigQuery and GCS."
  type        = string
  default     = "US"
}

variable "bq_dataset_name" {
  description = "The name of the BigQuery dataset for your air quality data."
  type        = string
  default     = "air_quality_dataset"
}

variable "gcs_bucket_name" {
  description = "The base name for your GCS bucket. Terraform will append your Project ID for uniqueness."
  type        = string
  default     = "aq_data_lake"
}

variable "credentials" {
  description = "My Credentials"
  default     = "~/.gc/air-creds.json"
  #ex: if you have a directory where this file is called keys with your service account json file
  #saved there as my-creds.json you could use default = "./keys/my-creds.json"
}
