# Dapr Components Contrib Certification Tests GitHub Actions Workflow

## Overview

This sets up the [Workload Identity Federation](https://cloud.google.com/iam/docs/workload-identity-federation) in the Components Contrib Gith Actions Workflow
for the Conformance and Certification Tests.

The Authn/Authz is handled by 2 resources already deployed in the GCP Project:

- Workload Identity Pool & Workload Identity Provider Configured **specifically** for this GitHub repository -  **(Authentication)**
- A GCP IAM Service Account (SA) used to impersonate this GitHub Actions workflow within the GCP Project **(Authorization)**. This SA requires **`roles/pubsub.admin`**, **`roles/datastore.owner`**, and **`roles/storage.admin`** for the GCP Pub/Sub, Firestore, and binary-store bucket tests. Storage Admin allows the bucket conformance job to provision a bucket, read and write test objects, and delete the bucket during teardown.

 **Note:** Changes to the roles for the SA should be made in the `roles` local variable in the file `service_account.tf`

After you change these roles, apply this infrastructure module before you run the affected tests. The conformance workflow does not update its own IAM permissions.

Always run `terraform plan` and read the plan before you apply it. The file `pubsub_roles.tf` uses `google_project_iam_binding`, which is authoritative for its role. An apply removes all other members of `roles/pubsub.publisher` and `roles/pubsub.subscriber` in the project.

The deployed resources use these names. Use the same names in the Terraform variables, or Terraform creates a second service account and pool:

| Resource | Name | Terraform variable |
|----------|------|--------------------|
| Service account | `dapr-contrib-cert-wif-sa` | `service_account=dapr-contrib-cert-wif-sa` |
| Workload Identity Pool | `cert-tests-gh-pool` | `wif_pool_name=cert-tests-gh` |
| Workload Identity Provider | `cert-tests-gh-provider` | `wif_pool_name=cert-tests-gh` |

The Terraform scripts follow steps similar to the suggested in the [Google GitHub Actions Auth](https://github.com/google-github-actions/auth#setting-up-workload-identity-federation)

The Terraform state is stored in [dapr-compoments-contrib-cert-tests](https://console.cloud.google.com/storage/browser/dapr-compoments-contrib-cert-tests?project=dapr-tests) Bucket
of the GCP GCS within the [dapr-tests](https://console.cloud.google.com/home/dashboard?project=dapr-tests) GCP Project.


## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| `project_id` | The project id that hosts the WIF pool and Dapr OSS SA | `string` | n/a | yes |
| `gh_repo`    | The GitHub Repo (username/repo_name) to associate with the WIF pool and Dapr SA | `string` | n/a | yes |
| `service_account` | The Dapr OSS SA used for GitHub WIF OIDC | `string` | n/a | yes |
| `wif_pool_name` | The Dapr OSS Workload Identity Pool Name | `string` | n/a | yes |

## Requirements

Before this module can be used on a project, you must ensure that the following pre-requisites are fulfilled:

1. Required APIs are activated

    ```
    "iam.googleapis.com",
    "cloudresourcemanager.googleapis.com",
    "iamcredentials.googleapis.com",
    "sts.googleapis.com",
    "storage.googleapis.com",
    ```

1. The GCP Account or Service Account used to deploy this module has the following roles

    ```
    roles/iam.workloadIdentityPoolAdmin
    roles/iam.serviceAccountAdmin
    roles/storage.admin
    ```

## Run Terraform

```
$ terraform init

$ terraform plan -out=tfplan -var="gh_repo=dapr/components-contrib" \
                 -var="project_id=dapr-tests" -var="service_account=dapr-contrib-cert-wif-sa" \
                 -var="wif_pool_name=cert-tests-gh"

# Read the plan. Apply it only if every change is expected.
$ terraform apply tfplan
```


## Outputs

```
$ terraform output                                                   
    
pool_name = "projects/***/locations/global/workloadIdentityPools/cert-tests-gh-pool"
provider_name = "projects/***/locations/global/workloadIdentityPools/cert-tests-gh-pool/providers/cert-tests-gh-provider"
sa_email = "***"
```
