// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package commercetools

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"

	"github.com/labd/commercetools-go-sdk/commercetools"
)

type Binding struct {
	client *commercetools.Client
	logger logger.Logger
}

type Data struct {
	CommercetoolsAPI string
	Query            string
}

type commercetoolsMetadata struct {
	region       string
	provider     string
	projectKey   string
	clientID     string
	clientSecret string
	scopes       string
}

func NewCommercetools(logger logger.Logger) *Binding {
	return &Binding{logger: logger}
}

// Init does metadata parsing and connection establishment.
func (ct *Binding) Init(metadata bindings.Metadata) error {
	commercetoolsM, err := ct.getCommercetoolsMetadata(metadata)
	if err != nil {
		return err
	}

	// Create the new client. When an empty value is passed it will use the CTP_*
	// environment variables to get the value. The HTTPClient arg is optional,
	// and when empty will automatically be created using the env values.
	client, err := commercetools.NewClient(&commercetools.ClientConfig{
		ProjectKey: commercetoolsM.projectKey,
		Endpoints:  commercetools.NewClientEndpoints(commercetoolsM.region, commercetoolsM.provider),
		Credentials: &commercetools.ClientCredentials{
			ClientID:     commercetoolsM.clientID,
			ClientSecret: commercetoolsM.clientSecret,
			Scopes:       []string{commercetoolsM.scopes},
		},
	})
	if err != nil {
		ct.logger.Errorf("error creating commercetools client: %s", err)

		return err
	}

	ct.client = client

	return nil
}

func (ct *Binding) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}

// Invoke is triggered from Dapr.
func (ct *Binding) Invoke(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var reqData Data
	json.Unmarshal(req.Data, &reqData)
	query := reqData.Query

	ctx := context.Background()

	res := &bindings.InvokeResponse{Data: nil, Metadata: nil}
	var err error

	if len(reqData.CommercetoolsAPI) > 0 {
		ct.logger.Infof("commercetoolsAPI: %s", reqData.CommercetoolsAPI)
		if reqData.CommercetoolsAPI == "GraphQLQuery" {
			res, err = handleGraphQLQuery(ctx, ct, query)
			if err != nil {
				ct.logger.Errorf("error GraphQLQuery")

				return nil, err
			}
		}
	} else {
		return nil, errors.New("commercetools error: No commercetools API provided")
	}

	return res, nil
}

// handleGraphQLQuery executes the provided query against the commercetools backend.
func handleGraphQLQuery(ctx context.Context, ct *Binding, query string) (*bindings.InvokeResponse, error) {
	ct.logger.Infof("handleGraphQLQuery")

	res := &bindings.InvokeResponse{Data: nil, Metadata: nil}

	if len(query) > 0 {
		gql := ct.client.NewGraphQLQuery(query)
		var gqlResp interface{}
		errGQL := gql.Execute(&gqlResp)
		if errGQL != nil {
			return nil, errors.New("commercetools error: Error executing the provided GraphQL query")
		}

		bQuery, errM := json.Marshal(gqlResp)
		if errM != nil {
			return nil, errors.New("commercetools error: Error marshalling GraphQL query result")
		}

		res = &bindings.InvokeResponse{Data: bQuery, Metadata: nil}
	} else {
		return res, errors.New("commercetools error: No GraphQL query is provided")
	}

	return res, nil
}

// getCommercetoolsMetadata returns new commercetools metadata object.
func (ct *Binding) getCommercetoolsMetadata(metadata bindings.Metadata) (*commercetoolsMetadata, error) {
	meta := commercetoolsMetadata{}

	if val, ok := metadata.Properties["region"]; ok && val != "" {
		meta.region = val
	} else {
		return nil, errors.New("commercetools error: missing `region` configuration")
	}

	if val, ok := metadata.Properties["provider"]; ok && val != "" {
		meta.provider = val
	} else {
		return nil, errors.New("commercetools error: missing `provider` configuration")
	}

	if val, ok := metadata.Properties["projectKey"]; ok && val != "" {
		meta.projectKey = val
	} else {
		return nil, errors.New("commercetools error: missing `projectKey` configuration")
	}

	if val, ok := metadata.Properties["clientID"]; ok && val != "" {
		meta.clientID = val
	} else {
		return nil, errors.New("commercetools error: missing `clientID` configuration")
	}

	if val, ok := metadata.Properties["clientSecret"]; ok && val != "" {
		meta.clientSecret = val
	} else {
		return nil, errors.New("commercetools error: missing `clientSecret` configuration")
	}

	if val, ok := metadata.Properties["scopes"]; ok && val != "" {
		meta.scopes = val
	} else {
		return nil, errors.New("commercetools error: missing `scopes` configuration")
	}

	return &meta, nil
}

// Close releases the client.
func (ct *Binding) Close() error {
	ct.client = nil

	return nil
}
