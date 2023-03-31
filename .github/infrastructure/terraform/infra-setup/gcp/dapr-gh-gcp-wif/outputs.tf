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

output "pool_name" {
  description = "Pool name"
  value       = module.oidc.pool_name
}

output "provider_name" {
  description = "Provider name"
  value       = module.oidc.provider_name
}

output "sa_email" {
  description = "WIF SA email"
  value       = google_service_account.sa.email
}
