/**
# ------------------------------------------------------------
# Copyright 2023 The Dapr Authors
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ------------------------------------------------------------
*/
variable "project_id" {
  type        = string
  description = "The project id that hosts the WIF pool and Dapr OSS SA"
}

variable "gh_repo" {
  type        = string
  description = "The Github Repo (username/repo_name) to associate with the WIF pool and Dapr SA"
}

variable "service_account" {
  type        = string
  description = "The Dapr OSS SA used for Github WIF OIDC"
}

variable "wif_pool_name" {
  type        = string
  description = "The Dapr OSS Workload Identity Pool Name"
}
