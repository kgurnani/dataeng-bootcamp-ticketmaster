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
  project = var.project
  region = "us-east4"
}

resource "google_storage_bucket" "data-lake-bucket" {
  name          = "ticketmaster_${var.project}"
  location      = "us-east4"

  storage_class = "STANDARD"
  uniform_bucket_level_access = true

  versioning {
    enabled     = true
  }

  force_destroy = true
}

resource "google_bigquery_dataset" "dataset" {
  dataset_id = "ticketmaster"
  project    = var.project
  location   = "us-east4"
}