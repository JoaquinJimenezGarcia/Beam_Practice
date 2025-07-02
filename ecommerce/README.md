# Beam E-commerce Example

This project simulates an e-commerce data processing pipeline using Apache Beam and Google Cloud.

---

## 🔁 Batch pipeline

- **Input:** CSV file with daily sales (`sample_sales.csv`)
- **Source:** Google Cloud Storage (GCS)
- **Output:** BigQuery table `ecommerce.sales_summary`
- **Action:** Aggregates total sales by `country` and `category`

## 🔁 Streaming pipeline

- **Input:** Real-time user events via Pub/Sub (`user-events` topic)
- **Output:** BigQuery table `ecommerce.user_events_stats`
- **Action:** Counts user events (e.g., product views, logins) per minute

---

## 🧱 Infrastructure Provisioning with Terraform

The project includes a `terraform/` folder to automatically provision all necessary resources on GCP:

### 📦 Resources Created by Terraform

| Resource Type         | Purpose                                                                 |
|------------------------|-------------------------------------------------------------------------|
| GCS Bucket             | Stores the CSV file, staging and temp files for Dataflow                |
| BigQuery Dataset       | Logical grouping of analytics tables (`ecommerce`)                      |
| BigQuery Tables        | `sales_summary` and `user_events_stats`, used as outputs for pipelines  |
| GCS Object (CSV)       | Uploads the sample CSV file (`sample_sales.csv`)                   |
| Pub/Sub Topic          | Topic `user-events` for simulating real-time user activity              |
| Pub/Sub Subscription   | (Optional) Subscription `user-events-sub` for testing and debugging     |

### 📁 Folder structure

```
terraform/
├── main.tf                 # Defines all resources
├── variables.tf            # Input variables
├── outputs.tf              # Useful outputs like bucket name
├── sample_sales.csv   # Sample sales data to upload
├── sales_summary.json      # BigQuery schema for batch pipeline
└── user_events_stats.json  # BigQuery schema for streaming pipeline
```

### 🚀 How to deploy the infrastructure

1. Enter the Terraform folder:

```bash
cd terraform/
```

2. Initialize Terraform:

```bash
terraform init
```

3. Plan the infrastructure:

```bash
terraform plan -var="project_id=your-gcp-project" -var="bucket_name=your-bucket-name"
```

Optional: You can create a `terraform.tfvars` file for your project and bucket:

```hcl
project_id   = "your-gcp-project"
bucket_name  = "your-bucket-name"
```

Then Apple:

```bash
terraform apply -var="project_id=your-gcp-project" -var="bucket_name=your-bucket-name"
```

Once applied, Terraform will:
- Create the GCS bucket
- Upload the sample CSV to `gs://your-bucket/daily_sales/sample_sales.csv`
- Create the BigQuery dataset `ecommerce`
- Create both output tables used by Beam
- Create a Pub/Sub topic for user events

---

## 🛠 Manual Setup (if not using Terraform)

If you prefer not to use Terraform, follow these manual steps:

```bash
# Set your project and bucket name
export PROJECT_ID=your-gcp-project
export BUCKET=your-bucket-name

# Upload CSV file
gsutil cp data/sample_sales.csv gs://$BUCKET/daily_sales/

# Create BigQuery dataset
bq mk ecommerce

# Create BigQuery tables
bq mk --table $PROJECT_ID:ecommerce.sales_summary schemas/sales_summary.json
bq mk --table $PROJECT_ID:ecommerce.user_events_stats schemas/user_events_stats.json

# Create Pub/Sub topic
gcloud pubsub topics create user-events

# (Optional) Create a subscription to test with CLI
gcloud pubsub subscriptions create user-events-sub --topic=user-events
```

---

## 🚀 Run the Pipelines

Make sure you've authenticated:

```bash
gcloud auth application-default login
```

Install dependencies:

```bash
pip install -r requirements.txt
```

### Set up your enviroment variables
```bash
# Set your project and bucket name
export PROJECT_ID=your-gcp-project
export BUCKET=your-bucket-name
```

### ▶️ Run batch pipeline

```bash
python batch_pipeline.py
```

### ▶️ Run streaming pipeline

Make sure the Pub/Sub topic `user-events` exists and you have a simulator sending JSON events.

```bash
python streaming_pipeline.py
```

---

## 🧪 Simulate Pub/Sub Events

To simulate user activity and send events to the `user-events` topic, run:

```bash
python simulate_pubsub_events.py
```

The script will randomly publish user activity events such as `product_view`, `add_to_cart`, `login`, etc., as JSON messages.

---

## ✅ GCP Services Involved

- Cloud Storage
- Pub/Sub
- BigQuery
- Dataflow
- Apache Beam (Python SDK)

---

## 📚 Learn More

- [Apache Beam Python SDK](https://beam.apache.org/documentation/sdks/python/)
- [Dataflow Documentation](https://cloud.google.com/dataflow/docs)
- [Terraform GCP Provider](https://registry.terraform.io/providers/hashicorp/google/latest/docs)
