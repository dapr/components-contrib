# Table storage certification testing

This project aims to test the Azure Table storage State Store component under various conditions.

## Test plan

## Basic Test for created table for CRUD operations:
1. Able to create and test connection.
2. Able to do set, fetch, delete.

## Basic Test for non-created table for CRUD operations:
1. Able to create and test connection.
2. Able to do set, fetch, delete data.
3. Delete the table

## Test for authentication using Azure Auth layer
1. Save Data using Go SDK with already existing table
2. Retrieve data by using AAD credentials