# Dapr Components Contrib Certification Tests GitHub Actions Workflow

## Overview

This sets up the [Workload Identity Federation](https://cloud.google.com/iam/docs/workload-identity-federation) in the Components Contrib Gith Actions Workflow
for the Conformance and Certification Tests.

The Authn/Authz is handled by 2 resources already deployed in the GCP Project:

- Workload Identity Pool & Workload Identity Provider Configured **specifically** for this GitHub repository -  **(Authentication)**
- A GCP IAM Service Account (SA) used to impersonate this GitHub Actions workflow within the GCP Project **(Authorization)**. This SA requires **`roles/pubsub.admin`**, **`roles/datastore.owner`**, and **`roles/storage.admin`** for the GCP Pub/Sub, Firestore, and binary-store bucket tests. Storage Admin allows the bucket conformance job to provision a bucket, read and write test objects, and delete the bucket during teardown.

 **Note:** Changes to the roles for the SA should be made in the `roles` local variable in the file `service_account.tf`

After changing these roles, apply this infrastructure module before running the affected tests; the conformance workflow does not update its own IAM permissions.

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

$ terraform refresh -var="gh_repo=dapr/components-contrib" \
                 -var="project_id=dapr-tests" -var="service_account=dapr-contrib-wif-sa" \
                 -var="wif_pool_name=dapr-contrib-cert-tests"

$ terraform plan -var="gh_repo=dapr/components-contrib" \
                 -var="project_id=dapr-tests" -var="service_account=dapr-contrib-wif-sa" \
                 -var="wif_pool_name=dapr-contrib-cert-tests"

$ terraform apply --auto-approve -var="gh_repo=dapr/components-contrib" \
                 -var="project_id=dapr-tests" -var="service_account=dapr-contrib-wif-sa" \
                 -var="wif_pool_name=dapr-contrib-cert-tests"
```


## Outputs

```
$ terraform output                                                   
    
pool_name = "projects/***/locations/global/workloadIdentityPools/dapr-contrib-cert-tests-pool"
provider_name = "projects/***/locations/global/workloadIdentityPools/dapr-contrib-cert-tests-pool/providers/dapr-contrib-cert-tests-provider"
sa_email = "***"
```
