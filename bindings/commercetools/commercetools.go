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
	Region          = "Region"
	Provider        = "Provider"
	ProjectKey      = "ProjectKey"
	ClientID        = "ClientID"
	ClientSecret    = "ClientSecret"
	Scopes          = "Scopes"
	ProductTypeName = "ProductTypeName"
	ProductTypeKey  = "ProductTypeKey"
	RequestJSON     = "RequestJSON"
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
	Region       string
	Provider     string
	ProjectKey   string
	ClientID     string
	ClientSecret string
	Scopes       string
}

func NewCommercetools(logger logger.Logger) *Binding {
	return &Binding{logger: logger}
}

// Init does metadata parsing and connection establishment.
func (ct *Binding) Init(metadata bindings.Metadata) error {
	commercetoolsM := commercetoolsMetadata{}

	commercetoolsM.Region = metadata.Properties[Region]
	commercetoolsM.Provider = metadata.Properties[Provider]
	commercetoolsM.ProjectKey = metadata.Properties[ProjectKey]
	commercetoolsM.ClientID = metadata.Properties[ClientID]
	commercetoolsM.ClientSecret = metadata.Properties[ClientSecret]
	commercetoolsM.Scopes = metadata.Properties[Scopes]

	// Create the new client. When an empty value is passed it will use the CTP_*
	// environment variables to get the value. The HTTPClient arg is optional,
	// and when empty will automatically be created using the env values.
	client, err := commercetools.NewClient(&commercetools.ClientConfig{
		ProjectKey: commercetoolsM.ProjectKey,
		Endpoints:  commercetools.NewClientEndpoints(commercetoolsM.Region, commercetoolsM.Provider),
		Credentials: &commercetools.ClientCredentials{
			ClientID:     commercetoolsM.ClientID,
			ClientSecret: commercetoolsM.ClientSecret,
			Scopes:       []string{commercetoolsM.Scopes},
		},
	})
	if err != nil {
		ct.logger.Errorf("error creating commercetools client: %s", err)

		return err
	}

	ct.metadata = commercetoolsM
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
			res, err = HandleGraphQLQuery(ctx, ct, query)
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
func HandleGraphQLQuery(ctx context.Context, ct *Binding, query string) (*bindings.InvokeResponse, error) {
	ct.logger.Infof("HandleGraphQLQuery")

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
	meta.Region = metadata.Properties["Region"]
	meta.Provider = metadata.Properties["Provider"]
	meta.ProjectKey = metadata.Properties["ProjectKey"]
	meta.ClientID = metadata.Properties["ClientID"]
	meta.ClientSecret = metadata.Properties["ClientSecret"]
	meta.Scopes = metadata.Properties["Scopes"]

	return &meta, nil
}
