# HashiCorp Vault Secret Store certification testing

This project aims to test the [HashiCorp Vault Secret Store] component under various conditions.

This secret store [supports the following features][features]:
* Basic retrieve operations
* Multiple Keys under the same secret

# Test plan

## Basic Test for CRUD operations:
1. Able to create and test connection.
2. Able to do retrieve secrets.
3. Negative test to fetch record with key, that is not present.


## Test support for multiple keys under the same secret
1. Test retrieval of secrets with multiple keys under it


## Out of scope

1. Tests verifying writing and updating a secret since secret stores do not expose this functionality. 


## Running the tests

Under the current directory run:

```
go test -v vault_test.go
```

# References:

* [HashiCorp Vault Secret Store Component reference page][HashiCorp Vault Secret Store]
* [List of secret store components and their features][features]
* [PR with Conformance tests for Hashicorp Vault][conformance]
* [HashiCorp Vault API reference](https://www.vaultproject.io/api-docs)

[HashiCorp Vault Secret Store]: https://docs.dapr.io/reference/components-reference/supported-secret-stores/hashicorp-vault/
[features]: https://docs.dapr.io/reference/components-reference/supported-secret-stores/
[conformance]: https://github.com/dapr/components-contrib/pull/2031