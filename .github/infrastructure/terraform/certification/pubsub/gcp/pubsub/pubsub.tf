terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 4.34.0"
    }
  }
}

variable "TIMESTAMP" {
    type        = string
    description = "Timestamp of the github worklow run."
}

variable "UNIQUE_ID" {
    type        = string
    description = "Unique Id of the github worklow run."
}

variable "GCP_PROJECT_ID" {
    type        = string
    description = "Google Cloud Project Id of the github worklow run."
}

provider "google" {
  region = "us-east-1"
  project = var.GCP_PROJECT_ID
}

resource "google_pubsub_topic" "topic" {
  name = "gcpps-ct-tp-exists-${var.UNIQUE_ID}"
  labels = {
    purpose  = "certification-testing"
    timestamp = "${var.TIMESTAMP}"
  }
}

resource "google_pubsub_subscription" "subscription" {
  name    = "sub-${google_pubsub_topic.topic.name}"
  topic   = google_pubsub_topic.topic.name
}

// ###### OUTPUT VARIABLES ########
output "PUBSUB_GCP_TOPIC" {
  value = google_pubsub_topic.topic.name
}
output "PUBSUB_GCP_SUBSCRIPTION" {
  value = google_pubsub_subscription.subscription.name
}