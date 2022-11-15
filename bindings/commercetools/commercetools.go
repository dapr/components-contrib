/*
Copyright 2021 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package commercetools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"

	"github.com/labd/commercetools-go-sdk/platform"
	"golang.org/x/oauth2/clientcredentials"
)

type Binding struct {
	client     *platform.Client
	logger     logger.Logger
	projectKey string
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

func NewCommercetools(logger logger.Logger) bindings.OutputBinding {
	return &Binding{logger: logger}
}

// Init does metadata parsing and connection establishment.
func (ct *Binding) Init(metadata bindings.Metadata) error {
	commercetoolsM, err := ct.getCommercetoolsMetadata(metadata)
	if err != nil {
		return err
	}
	ct.projectKey = commercetoolsM.projectKey

	// The helper method NewClientEndpoint no longer exists, so URLs need to be manually constructed.
	// Reference: https://github.com/labd/commercetools-go-sdk/blob/15f4e7e85260cf206301504dced00a8bbf4d8682/commercetools/client.go#L115

	baseURLdomain := fmt.Sprintf("%s.%s.commercetools.com", commercetoolsM.region, commercetoolsM.provider)
	authURL := fmt.Sprintf("https://auth.%s/oauth/token", baseURLdomain)
	apiURL := fmt.Sprintf("https://api.%s", baseURLdomain)

	// Create the new client. When an empty value is passed it will use the CTP_*
	// environment variables to get the value. The HTTPClient arg is optional,
	// and when empty will automatically be created using the env values.
	client, err := platform.NewClient(&platform.ClientConfig{
		URL: apiURL,
		Credentials: &clientcredentials.Config{
			TokenURL:     authURL,
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
func (ct *Binding) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var reqData Data
	err := json.Unmarshal(req.Data, &reqData)
	if err != nil {
		return nil, err
	}
	query := reqData.Query

	res := &bindings.InvokeResponse{Data: nil, Metadata: nil}

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
		gql := ct.client.WithProjectKey(ct.projectKey).Graphql().Post(platform.GraphQLRequest{
			Query: query,
		})
		gqlResp, errGQL := gql.Execute(ctx)
		if errGQL != nil {
			return nil, errors.New("commercetools error: Error executing the provided GraphQL query")
		}

		bQuery, errM := json.Marshal(gqlResp.Data)
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
