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

const (
	region          = "region"
	provider        = "provider"
	projectKey      = "projectKey"
	clientID        = "clientID"
	clientSecret    = "clientSecret"
	scopes          = "scopes"
	productTypeName = "productTypeName"
	productTypeKey  = "productTypeKey"
	requestJSON     = "requestJSON"
)

type Binding struct {
	metadata commercetoolsMetadata
	client   *commercetools.Client
	logger   logger.Logger
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

// HandleGraphQLQuery executes the provided query against the commercetools backend.
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

// getCommercetoolsMetadata returns new commercetools metadata.
func (ct *Binding) getCommercetoolsMetadata(metadata bindings.Metadata) (*commercetoolsMetadata, error) {
	meta := commercetoolsMetadata{}

	val, ok := metadata.Properties["region"]
	if !ok {
		return nil, errors.New("commercetools error: missing 'region' attribute")
	}
	if val == "" {
		return nil, errors.New("commercetools error: 'region' attribute was empty")
	}
	meta.region = metadata.Properties["region"]

	val, ok = metadata.Properties["provider"]
	if !ok {
		return nil, errors.New("commercetools error: missing 'provider' attribute")
	}
	if val == "" {
		return nil, errors.New("commercetools error: 'provider' attribute was empty")
	}
	meta.provider = metadata.Properties["provider"]

	val, ok = metadata.Properties["projectKey"]
	if !ok {
		return nil, errors.New("commercetools error: missing 'projectKey' attribute")
	}
	if val == "" {
		return nil, errors.New("commercetools error: 'projectKey' attribute was empty")
	}
	meta.projectKey = metadata.Properties["projectKey"]

	val, ok = metadata.Properties["clientID"]
	if !ok {
		return nil, errors.New("commercetools error: missing 'clientID' attribute")
	}
	if val == "" {
		return nil, errors.New("commercetools error: 'clientID' attribute was empty")
	}
	meta.clientID = metadata.Properties["clientID"]

	val, ok = metadata.Properties["clientSecret"]
	if !ok {
		return nil, errors.New("commercetools error: missing 'clientSecret' attribute")
	}
	if val == "" {
		return nil, errors.New("commercetools error: 'clientSecret' attribute was empty")
	}
	meta.clientSecret = metadata.Properties["clientSecret"]

	val, ok = metadata.Properties["scopes"]
	if !ok {
		return nil, errors.New("commercetools error: missing 'scopes' attribute")
	}
	if val == "" {
		return nil, errors.New("commercetools error: 'scopes' attribute was empty")
	}
	meta.scopes = metadata.Properties["scopes"]

	return &meta, nil
}

func (ct *Binding) Close() error {
	ct.client = nil

	return nil
}
