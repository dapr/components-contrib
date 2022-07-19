# Table storage certification testing

This project aims to test the Azure Blob storage State Store component under various conditions.

## Test plan

## Basic Test for existing container for CRUD operations:
1. Able to create and test connection.
2. Able to do set, fetch, delete.

## Basic Test for non-existing container for CRUD operations:
1. Able to create and test connection.
2. Able to do set, fetch, delete data.
3. Delete the table

## Test for authentication using Azure Auth layer
1. Save Data retrieve data using AAD credentials