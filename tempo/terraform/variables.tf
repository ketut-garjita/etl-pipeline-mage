variable "location" {
  description = "My location"
  default = "asia-southeast2"
}

variable "bq_dataset_name"{
    description = "Bigquery Dataset name"
    default = "supply_chain_data"
}

variable "gcs_storage_class" {
  description = "GCS storage class name"
  default = "raw_streaming"

}

variable "gcs_bucket_name" {
  description = "GCS storage bucket name"
  default = "supply-chain-data"
}

