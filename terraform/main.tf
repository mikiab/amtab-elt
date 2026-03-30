terraform {
  required_version = ">= 1.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_storage_bucket" "processed" {
  name          = "${var.gcs_bucket_prefix}-processed-${var.project_id}"
  location      = var.region
  force_destroy = true
  storage_class = "STANDARD"
}

resource "google_bigquery_dataset" "main" {
  dataset_id = var.bq_dataset_id
  location   = var.location
}

resource "google_service_account" "pipeline" {
  account_id   = "amtab-pipeline"
  display_name = "AMTAB ELT Pipeline"
}

resource "google_project_iam_member" "pipeline_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_bigquery_dataset_iam_member" "pipeline_bq_editor" {
  dataset_id = google_bigquery_dataset.main.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_storage_bucket_iam_member" "pipeline_processed" {
  bucket = google_storage_bucket.processed.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_service_account_key" "pipeline_key" {
  service_account_id = google_service_account.pipeline.name
}

# Write SA key to local file for mounting into Airflow container
resource "local_file" "sa_key" {
  content         = base64decode(google_service_account_key.pipeline_key.private_key)
  filename        = "${path.module}/../keys/sa-key.json"
  file_permission = "0600"
}
