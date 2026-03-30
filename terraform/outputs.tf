output "processed_bucket_name" {
  value = google_storage_bucket.processed.name
}

output "bq_dataset_id" {
  value = google_bigquery_dataset.main.dataset_id
}

output "service_account_email" {
  value = google_service_account.pipeline.email
}

output "sa_key_path" {
  value = local_file.sa_key.filename
}
