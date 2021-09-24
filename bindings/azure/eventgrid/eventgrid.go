// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
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

	"github.com/Azure/azure-sdk-for-go/services/preview/eventgrid/mgmt/2020-04-01-preview/eventgrid"
	"github.com/Azure/go-autorest/autorest/azure/auth"
	"github.com/valyala/fasthttp"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

// AzureEventGrid allows sending/receiving Azure Event Grid events.
type AzureEventGrid struct {
	metadata *azureEventGridMetadata
	logger   logger.Logger
}

type azureEventGridMetadata struct {
	// Component Name
	Name string

	// Required Input Binding Metadata
	TenantID           string `json:"tenantId"`
	SubscriptionID     string `json:"subscriptionId"`
	ClientID           string `json:"clientId"`
	ClientSecret       string `json:"clientSecret"`
	SubscriberEndpoint string `json:"subscriberEndpoint"`
	HandshakePort      string `json:"handshakePort"`
	Scope              string `json:"scope"`

	// Optional Input Binding Metadata
	EventSubscriptionName string `json:"eventSubscriptionName"`

	// Required Output Binding Metadata
	AccessKey     string `json:"accessKey"`
	TopicEndpoint string `json:"topicEndpoint"`
}

// NewAzureEventGrid returns a new Azure Event Grid instance.
func NewAzureEventGrid(logger logger.Logger) *AzureEventGrid {
	return &AzureEventGrid{logger: logger}
}

// Init performs metadata init.
func (a *AzureEventGrid) Init(metadata bindings.Metadata) error {
	m, err := a.parseMetadata(metadata)
	if err != nil {
		return err
	}
	a.metadata = m

	return nil
}

func (a *AzureEventGrid) Read(handler func(*bindings.ReadResponse) ([]byte, error)) error {
	err := a.ensureInputBindingMetadata()
	if err != nil {
		return err
	}

	err = a.createSubscription()
	if err != nil {
		return err
	}

	m := func(ctx *fasthttp.RequestCtx) {
		if string(ctx.Path()) == "/api/events" {
			switch string(ctx.Method()) {
			case "OPTIONS":
				ctx.Response.Header.Add("WebHook-Allowed-Origin", string(ctx.Request.Header.Peek("WebHook-Request-Origin")))
				ctx.Response.Header.Add("WebHook-Allowed-Rate", "*")
				ctx.Response.Header.SetStatusCode(fasthttp.StatusOK)
				_, err = ctx.Response.BodyWriter().Write([]byte(""))
				if err != nil {
					a.logger.Error(err.Error())
				}
			case "POST":
				bodyBytes := ctx.PostBody()

				_, err = handler(&bindings.ReadResponse{
					Data: bodyBytes,
				})
				if err != nil {
					a.logger.Error(err.Error())
					ctx.Error(err.Error(), fasthttp.StatusInternalServerError)
				}
			}
		}
	}

	a.logger.Debugf("About to start listening for Event Grid events at http://localhost:%s/api/events", a.metadata.HandshakePort)
	err = fasthttp.ListenAndServe(fmt.Sprintf(":%s", a.metadata.HandshakePort), m)
	if err != nil {
		return err
	}

	return nil
}

func (a *AzureEventGrid) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}

func (a *AzureEventGrid) Invoke(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	err := a.ensureOutputBindingMetadata()
	if err != nil {
		a.logger.Error(err.Error())

		return nil, err
	}

	request := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(request)
	request.Header.SetMethod(fasthttp.MethodPost)
	request.Header.Set("Content-Type", "application/cloudevents+json")
	request.Header.Set("aeg-sas-key", a.metadata.AccessKey)
	request.SetRequestURI(a.metadata.TopicEndpoint)
	request.SetBody(req.Data)

	response := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(response)

	client := &fasthttp.Client{WriteTimeout: time.Second * 10}
	err = client.Do(request, response)
	if err != nil {
		a.logger.Error(err.Error())

		return nil, err
	}

	if response.StatusCode() != fasthttp.StatusOK {
		body := response.Body()
		a.logger.Error(string(body))

		return nil, errors.New(string(body))
	}

	a.logger.Debugf("Successfully posted event to %s", a.metadata.TopicEndpoint)

	return nil, nil
}

func (a *AzureEventGrid) ensureInputBindingMetadata() error {
	if a.metadata.TenantID == "" {
		return errors.New("metadata field 'TenantID' is empty in EventGrid binding")
	}
	if a.metadata.SubscriptionID == "" {
		return errors.New("metadata field 'SubscriptionID' is empty in EventGrid binding")
	}
	if a.metadata.ClientID == "" {
		return errors.New("metadata field 'ClientID' is empty in EventGrid binding")
	}
	if a.metadata.ClientSecret == "" {
		return errors.New("metadata field 'ClientSecret' is empty in EventGrid binding")
	}
	if a.metadata.SubscriberEndpoint == "" {
		return errors.New("metadata field 'SubscriberEndpoint' is empty in EventGrid binding")
	}
	if a.metadata.HandshakePort == "" {
		return errors.New("metadata field 'HandshakePort' is empty in EventGrid binding")
	}
	if a.metadata.Scope == "" {
		return errors.New("metadata field 'Scope' is empty in EventGrid binding")
	}

	return nil
}

func (a *AzureEventGrid) ensureOutputBindingMetadata() error {
	if a.metadata.AccessKey == "" {
		msg := fmt.Sprintf("metadata field 'AccessKey' is empty in EventGrid binding (%s)", a.metadata.Name)

		return errors.New(msg)
	}
	if a.metadata.TopicEndpoint == "" {
		msg := fmt.Sprintf("metadata field 'TopicEndpoint' is empty in EventGrid binding (%s)", a.metadata.Name)

		return errors.New(msg)
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

	eventGridMetadata.Name = metadata.Name

	if eventGridMetadata.HandshakePort == "" {
		eventGridMetadata.HandshakePort = "8080"
	}

	if eventGridMetadata.EventSubscriptionName == "" {
		eventGridMetadata.EventSubscriptionName = metadata.Name
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

	eventInfo := eventgrid.EventSubscription{
		EventSubscriptionProperties: &eventgrid.EventSubscriptionProperties{
			Destination: eventgrid.WebHookEventSubscriptionDestination{
				EndpointType: eventgrid.EndpointTypeWebHook,
				WebHookEventSubscriptionDestinationProperties: &eventgrid.WebHookEventSubscriptionDestinationProperties{
					EndpointURL: &a.metadata.SubscriberEndpoint,
				},
			},
			EventDeliverySchema: eventgrid.CloudEventSchemaV10,
		},
	}

	a.logger.Debugf("Attempting to create or update Event Grid subscription. scope=%s endpointURL=%s", a.metadata.Scope, a.metadata.SubscriberEndpoint)
	result, err := subscriptionClient.CreateOrUpdate(context.Background(), a.metadata.Scope, a.metadata.EventSubscriptionName, eventInfo)
	if err != nil {
		a.logger.Debugf("Failed to create or update Event Grid subscription: %v", err)

		return err
	}

	res := result.FutureAPI.Response()

	if res.StatusCode != fasthttp.StatusCreated {
		bodyBytes, err := ioutil.ReadAll(res.Body)
		if err != nil {
			a.logger.Debugf("Failed reading error body when creating or updating Event Grid subscription: %v", err)

			return err
		}

		bodyStr := string(bodyBytes)
		a.logger.Debugf("Code HTTP %d in response to create or update Event Grid subscription: %s", res.StatusCode, bodyStr)

		return errors.New(bodyStr)
	}

	a.logger.Debugf("Succeeded to create or update Event Grid subscription. scope=%s endpointURL=%s", a.metadata.Scope, a.metadata.SubscriberEndpoint)

	return nil
}
