terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.6.0"
    }
  }
}
provider "google" {
  credentials = "/home/dataeng/dataeng-2025-gar.json"
  project     = "dataeng-2025-gar"

}

resource "google_storage_bucket" "mygcp-storage" {
  name          = var.gcs_bucket_name
  location = var.location
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 3
    }

    action {
      type = "Delete"
    }
  }
}

resource "google_bigquery_dataset" "bigquery-dataset" {
  dataset_id = var.bq_dataset_name
  location = var.location
}
