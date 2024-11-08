#!/bin/bash

awslocal secretsmanager create-secret \
    --name conftestsecret \
    --secret-string "abcd"

awslocal secretsmanager create-secret \
    --name secondsecret \
    --secret-string "efgh"