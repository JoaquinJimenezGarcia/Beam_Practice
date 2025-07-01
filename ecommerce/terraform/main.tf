provider "google" {
  project = var.project_id
  region  = var.region
}

# GCS Bucket
resource "google_storage_bucket" "ecommerce_data" {
  name     = var.bucket_name
  location = var.region
  force_destroy = true
}

# Upload CSV
resource "google_storage_bucket_object" "sales_csv" {
  name   = "sample_sales.csv"
  bucket = google_storage_bucket.ecommerce_data.name
  source = "${path.module}/data/sample_sales.csv"
  content_type = "text/csv"
}

# BigQuery dataset
resource "google_bigquery_dataset" "ecommerce" {
  dataset_id = "ecommerce"
  location   = var.region
}

# Table sales_summary
resource "google_bigquery_table" "sales_summary" {
  dataset_id = google_bigquery_dataset.ecommerce.dataset_id
  table_id   = "sales_summary"

  schema = file("${path.module}/schemas/sales_summary.json")
  deletion_protection = false
  time_partitioning {
    type = "DAY"
  }
}

# Table user_events_stats
resource "google_bigquery_table" "user_events_stats" {
  dataset_id = google_bigquery_dataset.ecommerce.dataset_id
  table_id   = "user_events_stats"

  schema = file("${path.module}/schemas/user_events_stats.json")
  deletion_protection = false
  time_partitioning {
    type = "DAY"
  }
}

# Pub/Sub topic for user events
resource "google_pubsub_topic" "user_events" {
  name = "user-events"
}

# Subscription to the topic for testing or debugging
resource "google_pubsub_subscription" "user_events_sub" {
  name  = "user-events-sub"
  topic = google_pubsub_topic.user_events.name

  ack_deadline_seconds = 20
}