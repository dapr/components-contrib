package aws

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// fakeCreds is a simple credentials provider used across multiple tests.
type fakeCreds struct{}

func (f *fakeCreds) Retrieve(ctx context.Context) (aws.Credentials, error) {
	return aws.Credentials{
		AccessKeyID:     "AKIDFAKE",
		SecretAccessKey: "SECRET",
		SessionToken:    "TOKEN",
	}, nil
}

func (f *fakeCreds) IsExpired() bool { return true }
