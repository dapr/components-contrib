terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 5.32.0"
    }
  }
}

variable "TIMESTAMP" {
  type        = string
  description = "Timestamp of the GitHub workflow run."
}

variable "UNIQUE_ID" {
  type        = string
  description = "Unique ID of the GitHub workflow run."
}

variable "GCP_PROJECT_ID" {
  type        = string
  description = "Google Cloud project ID of the GitHub workflow run."
}

provider "google" {
  project = var.GCP_PROJECT_ID
}

resource "google_storage_bucket" "bucket" {
  name                        = "dapr-binarystore-conformance-${var.UNIQUE_ID}"
  location                    = "US"
  force_destroy               = true
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  # Test data should not be retained after the job's teardown.
  soft_delete_policy {
    retention_duration_seconds = 0
  }

  labels = {
    purpose   = "conformance-testing"
    timestamp = var.TIMESTAMP
  }
}

output "GCPBucket" {
  value = google_storage_bucket.bucket.name
}
