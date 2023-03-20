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

provider "google" {
  region = "us-east-1"
  default_tags {
    tags = {
      Purpose  = "AutomatedTesting"
      Timestamp = "${var.TIMESTAMP}"
    }
  }
}

resource "google_pubsub_topic" "topic" {
  name = "testTopic-${var.UNIQUE_ID}"
}

resource "google_pubsub_topic" "multiTopic1" {
  name = "multiTopic1-${var.UNIQUE_ID}"
}

resource "google_pubsub_topic" "multiTopic2" {
  name = "multiTopic2-${var.UNIQUE_ID}"
}