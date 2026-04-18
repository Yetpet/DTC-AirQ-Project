terraform {
  required_version = ">= 1.0"
  backend "local" {}
  required_providers {
    google = {
      source  = "hashicorp/google"
    }
  }
}

provider "google" {
  credentials = file(var.credentials)
  project = var.project
  region  = var.region
}

# 1. Enable Necessary APIs (Ensures the project is ready)
resource "google_project_service" "enabled_apis" {
  for_each = toset([
    "iam.googleapis.com",
    "storage.googleapis.com",
    "bigquery.googleapis.com",
    "cloudresourcemanager.googleapis.com"
  ])
  service            = each.key
  disable_on_destroy = false
}

# 2. Service Account for Ingestion (Kestra/Python)
resource "google_service_account" "ingestion_sa" {
  account_id   = "air-quality-ingestion-sa"
  display_name = "Air Quality Ingestion Service Account"
  depends_on   = [google_project_service.enabled_apis]
}

# 3. IAM Roles (Assigning specific permissions)
resource "google_project_iam_member" "sa_roles" {
  for_each = toset([
    "roles/storage.objectAdmin",    # Full control over GCS objects
    # "roles/storage.objectCreator",  # To upload objects to GCS
    # "roles/storage.objectViewer",   # To read objects from GCS
    "roles/bigquery.dataEditor",    # To write to BigQuery tables
    "roles/bigquery.jobUser",       # To run BigQuery jobs
  ])
  project = var.project
  role    = each.key
  member  = "serviceAccount:${google_service_account.ingestion_sa.email}"
}

# 4. Generate the JSON Key (To be used in Kestra)
resource "google_service_account_key" "sa_key" {
  service_account_id = google_service_account.ingestion_sa.name
}

# 5. Data Lake (GCS Bucket)
resource "google_storage_bucket" "data_lake" {
  name          = "${var.gcs_bucket_name}_${var.project}"
  location      = var.location
  force_destroy = true
  depends_on    = [google_project_service.enabled_apis]

  lifecycle_rule {
    condition { age = 1 }
    action    { type = "AbortIncompleteMultipartUpload" }
  }
}

# 6. Data Warehouse (BigQuery Dataset)
resource "google_bigquery_dataset" "air_quality_dataset" {
  dataset_id = var.bq_dataset_name
  location   = var.location
  depends_on = [google_project_service.enabled_apis]
}

# 7. Optimized Table
resource "google_bigquery_table" "fact_air_quality" {
  dataset_id = google_bigquery_dataset.air_quality_dataset.dataset_id
  table_id   = "fact_air_quality"

  schema = jsonencode([
    {
      name        = "date_local"
      type        = "TIMESTAMP"
      mode        = "NULLABLE"
      description = "Local date and time of the measurement"
    },
    {
      name        = "city"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "City name"
    },
    {
      name        = "parameter"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "Air quality parameter (e.g., PM2.5, O3, NO2)"
    },
    {
      name        = "value"
      type        = "FLOAT64"
      mode        = "NULLABLE"
      description = "Measured value"
    },
    {
      name        = "unit"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "Unit of measurement"
    },
    {
      name        = "latitude"
      type        = "FLOAT64"
      mode        = "NULLABLE"
      description = "Geographic latitude"
    },
    {
      name        = "longitude"
      type        = "FLOAT64"
      mode        = "NULLABLE"
      description = "Geographic longitude"
    },
    {
      name        = "created_at"
      type        = "TIMESTAMP"
      mode        = "NULLABLE"
      description = "Timestamp of when the record was created"
    }
  ])

  time_partitioning {
    type  = "DAY"
    field = "date_local"
  }

  clustering = ["city", "parameter"]
  deletion_protection = false
}

# OUTPUT: Service account email (use for authentication setup)
output "service_account_email" {
  value       = google_service_account.ingestion_sa.email
  description = "Use this email for Kestra authentication configuration"
}

output "gcs_bucket_name" {
  value       = google_storage_bucket.data_lake.name
  description = "GCS bucket for data lake storage"
}

output "bigquery_dataset_id" {
  value       = google_bigquery_dataset.air_quality_dataset.dataset_id
  description = "BigQuery dataset ID for data warehouse"
}

output "service_account_key" {
  value       = google_service_account_key.sa_key.private_key
  sensitive   = true
  description = "SENSITIVE: Service account private key. Store securely and never commit to version control."
}