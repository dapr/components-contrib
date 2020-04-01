// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package eventgrid

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"

	"time"

	"github.com/Azure/azure-sdk-for-go/services/eventgrid/mgmt/2019-06-01/eventgrid"
	"github.com/Azure/go-autorest/autorest/azure/auth"
	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/valyala/fasthttp"
)

// AzureEventGrid allows sending/receiving Azure Event Grid events
type AzureEventGrid struct {
	metadata *azureEventGridMetadata
	logger   logger.Logger
}

type azureEventGridMetadata struct {
	TenantID                  string `json:"tenantId"`
	SubscriptionID            string `json:"subscriptionId"`
	ResourceGroupName         string `json:"resourceGroupName"`
	TopicName                 string `json:"topicName"`
	EventGridSubscriptionName string `json:"eventGridSubscriptionName"`
	SubscriberEndpoint        string `json:"subscriberEndpoint"`
	TopicEndpoint             string `json:"topicEndpoint"`
	ClientID                  string `json:"clientId"`
	ClientSecret              string `json:"clientSecret"`
}

// NewAzureEventGrid returns a new Azure Event Grid instance
func NewAzureEventGrid(logger logger.Logger) *AzureEventGrid {
	logger.Debug("NewAzureEventGrid() called...")
	return &AzureEventGrid{logger: logger}
}

// Init performs metadata init
func (a *AzureEventGrid) Init(metadata bindings.Metadata) error {
	a.logger.Debugf("Parsing Event Grid metadata(%s)...", metadata.Name)

	m, err := a.parseMetadata(metadata)
	if err != nil {
		return err
	}
	a.metadata = m

	a.logger.Debug("Metadata parsed successfully.")

	return nil
}

func (a *AzureEventGrid) Read(handler func(*bindings.ReadResponse) error) error {
	a.logger.Debug("Read() called...")

	err := a.createSubscription()
	if err != nil {
		return err
	}

	m := func(ctx *fasthttp.RequestCtx) {
		switch string(ctx.Path()) {
		case "/api/events":
			if string(ctx.Method()) == "OPTIONS" {
				ctx.Response.Header.Add("WebHook-Allowed-Origin", string(ctx.Request.Header.Peek("WebHook-Request-Origin")))
				ctx.Response.Header.Add("WebHook-Allowed-Rate", "*")
				ctx.Response.Header.SetStatusCode(fasthttp.StatusOK)
				_, err = ctx.Response.BodyWriter().Write([]byte(""))
				if err != nil {
					a.logger.Error(err.Error())
				}
			} else if string(ctx.Method()) == "POST" {
				bodyBytes := ctx.PostBody()

				a.logger.Debug(string(bodyBytes))
				err = handler(&bindings.ReadResponse{
					Data: bodyBytes,
				})
				if err != nil {
					a.logger.Error(err.Error())
				}
			}
		}
	}

	fasthttp.ListenAndServe(":8080", m)

	a.logger.Debug("listening for Event Grid events at http://localhost:8080/api/events")

	return nil
}

func (a *AzureEventGrid) Write(req *bindings.WriteRequest) error {
	a.logger.Debug("Write() called. data: %s", string(req.Data))

	request := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(request)
	request.Header.SetMethod(fasthttp.MethodPost)
	request.Header.Set("Content-Type", "application/cloudevents+json")
	request.Header.Set("aeg-sas-key", a.metadata.ClientSecret)
	request.SetRequestURI(a.metadata.TopicEndpoint)
	request.SetBody(req.Data)

	response := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(response)

	client := &fasthttp.Client{WriteTimeout: time.Second * 10}
	err := client.Do(request, response)
	if err != nil {
		a.logger.Error(err.Error())
	}

	if response.StatusCode() != fasthttp.StatusOK {
		body := response.Body()
		a.logger.Error(string(body))
		return errors.New(string(body))
	}

	return nil
}

func (a *AzureEventGrid) parseMetadata(metadata bindings.Metadata) (*azureEventGridMetadata, error) {
	b, err := json.Marshal(metadata.Properties)
	if err != nil {
		return nil, err
	}

	var eventGridMetadata azureEventGridMetadata
	err = json.Unmarshal(b, &eventGridMetadata)
	if err != nil {
		return nil, err
	}
	return &eventGridMetadata, nil
}

func (a *AzureEventGrid) createSubscription() error {
	clientCredentialsConfig := auth.NewClientCredentialsConfig(a.metadata.ClientID, a.metadata.ClientSecret, a.metadata.TenantID)

	subscriptionClient := eventgrid.NewEventSubscriptionsClient(a.metadata.SubscriptionID)
	authorizer, err := clientCredentialsConfig.Authorizer()
	if err != nil {
		return err
	}
	subscriptionClient.Authorizer = authorizer

	scope := fmt.Sprintf("/subscriptions/%s/resourceGroups/%s/providers/Microsoft.EventGrid/topics/%s", a.metadata.SubscriptionID, a.metadata.ResourceGroupName, a.metadata.TopicName)

	eventInfo := eventgrid.EventSubscription{
		EventSubscriptionProperties: &eventgrid.EventSubscriptionProperties{
			Destination: eventgrid.WebHookEventSubscriptionDestination{
				EndpointType: eventgrid.EndpointTypeWebHook,
				WebHookEventSubscriptionDestinationProperties: &eventgrid.WebHookEventSubscriptionDestinationProperties{
					EndpointURL: &a.metadata.SubscriberEndpoint,
				},
			},
		},
	}

	a.logger.Debugf("Attempting to create or update Event Grid subscription. scope=%s endpointURL=%s", scope, a.metadata.SubscriberEndpoint)
	result, err := subscriptionClient.CreateOrUpdate(context.Background(), scope, a.metadata.EventGridSubscriptionName, eventInfo)
	if err != nil {
		return err
	}

	res := result.Future.Response()

	if res.StatusCode != fasthttp.StatusCreated {
		bodyBytes, err := ioutil.ReadAll(res.Body)
		if err != nil {
			return err
		}
		return errors.New(string(bodyBytes))
	}

	return nil
}
