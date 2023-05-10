variable "bucket_name_set" {
  description = "Your bucket name"
  type = list(string)
}

variable "project" {
  description = "Your GCP Project ID"
}

variable "region" {
  description = "europe-west6"
  type = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "trips_data_all"
}