output "bucket_url" {
  value = "gs://${google_storage_bucket.ecommerce_data.name}"
}

output "dataset_id" {
  value = google_bigquery_dataset.ecommerce.dataset_id
}
