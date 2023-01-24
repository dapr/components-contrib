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

package eventgrid

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	armeventgrid "github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/eventgrid/armeventgrid/v2"
	"github.com/valyala/fasthttp"

	"github.com/dapr/components-contrib/bindings"
	azauth "github.com/dapr/components-contrib/internal/authentication/azure"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"
)

const armOperationTimeout = 30 * time.Second

// AzureEventGrid allows sending/receiving Azure Event Grid events.
type AzureEventGrid struct {
	metadata *azureEventGridMetadata
	logger   logger.Logger
}

type azureEventGridMetadata struct {
	// Component Name
	Name string `json:"-" mapstructure:"-"`

	// Required Input Binding Metadata
	TenantID           string `json:"tenantId" mapstructure:"tenantId"`
	SubscriptionID     string `json:"subscriptionId" mapstructure:"subscriptionId"`
	ClientID           string `json:"clientId" mapstructure:"clientId"`
	ClientSecret       string `json:"clientSecret" mapstructure:"clientSecret"`
	SubscriberEndpoint string `json:"subscriberEndpoint" mapstructure:"subscriberEndpoint"`
	HandshakePort      string `json:"handshakePort" mapstructure:"handshakePort"`
	Scope              string `json:"scope" mapstructure:"scope"`

	// Optional Input Binding Metadata
	EventSubscriptionName string `json:"eventSubscriptionName" mapstructure:"eventSubscriptionName"`

	// Required Output Binding Metadata
	AccessKey     string `json:"accessKey" mapstructure:"accessKey"`
	TopicEndpoint string `json:"topicEndpoint" mapstructure:"topicEndpoint"`

	// Internal
	properties map[string]string
}

// NewAzureEventGrid returns a new Azure Event Grid instance.
func NewAzureEventGrid(logger logger.Logger) bindings.InputOutputBinding {
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

func (a *AzureEventGrid) Read(ctx context.Context, handler bindings.Handler) error {
	err := a.ensureInputBindingMetadata()
	if err != nil {
		return err
	}

	m := func(ctx *fasthttp.RequestCtx) {
		if string(ctx.Path()) == "/api/events" {
			switch string(ctx.Method()) {
			case "OPTIONS":
				ctx.Response.Header.Add("WebHook-Allowed-Origin", string(ctx.Request.Header.Peek("WebHook-Request-Origin")))
				ctx.Response.Header.Add("WebHook-Allowed-Rate", "*")
				ctx.Response.Header.SetStatusCode(http.StatusOK)
				_, err = ctx.Response.BodyWriter().Write([]byte(""))
				if err != nil {
					a.logger.Error(err.Error())
				}
			case "POST":
				bodyBytes := ctx.PostBody()

				_, err = handler(ctx, &bindings.ReadResponse{
					Data: bodyBytes,
				})
				if err != nil {
					a.logger.Error(err.Error())
					ctx.Error(err.Error(), http.StatusInternalServerError)
				}
			}
		}
	}

	srv := &fasthttp.Server{
		Handler: m,
	}

	// Run the server in background
	go func() {
		a.logger.Debugf("About to start listening for Event Grid events at http://localhost:%s/api/events", a.metadata.HandshakePort)
		err := srv.ListenAndServe(":" + a.metadata.HandshakePort)
		if err != nil {
			a.logger.Errorf("Error starting server: %v", err)
		}
	}()

	// Close the server when context is canceled
	go func() {
		<-ctx.Done()
		err := srv.Shutdown()
		if err != nil {
			a.logger.Errorf("Error shutting down server: %v", err)
		}
	}()

	err = a.createSubscription(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (a *AzureEventGrid) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}

func (a *AzureEventGrid) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
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
	request.Header.Set("User-Agent", "dapr/"+logger.DaprVersion)
	request.SetRequestURI(a.metadata.TopicEndpoint)
	request.SetBody(req.Data)

	response := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(response)

	client := &fasthttp.Client{WriteTimeout: time.Second * 10}
	err = client.Do(request, response)
	if err != nil {
		err = fmt.Errorf("request error: %w", err)
		a.logger.Error(err.Error())
		return nil, err
	}

	if response.StatusCode() != http.StatusOK {
		body := response.Body()
		err = fmt.Errorf("invalid status code (%d) - response: %s", response.StatusCode(), string(body))
		a.logger.Error(err.Error())
		return nil, err
	}

	//a.logger.Debugf("Successfully posted event to %s", a.metadata.TopicEndpoint)

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
		return fmt.Errorf("metadata field 'AccessKey' is empty in EventGrid binding (%s)", a.metadata.Name)
	}
	if a.metadata.TopicEndpoint == "" {
		return fmt.Errorf("metadata field 'TopicEndpoint' is empty in EventGrid binding (%s)", a.metadata.Name)
	}

	return nil
}

func (a *AzureEventGrid) parseMetadata(md bindings.Metadata) (*azureEventGridMetadata, error) {
	var eventGridMetadata azureEventGridMetadata
	err := metadata.DecodeMetadata(md.Properties, &eventGridMetadata)
	if err != nil {
		return nil, fmt.Errorf("error decoding metadata: %w", err)
	}

	// Save the raw properties in the object as we'll need them to authenticate with Azure AD
	eventGridMetadata.properties = md.Properties

	// Sanitize "SubscriberEndpoint"
	// If there's no path at the end of the subscriber endpoint, add "/api/events"
	u, err := url.Parse(eventGridMetadata.SubscriberEndpoint)
	if err != nil {
		return nil, errors.New("property 'subscriberEndpoint' is not a valid URL")
	}
	if u.Path == "" || u.Path == "/" {
		a.logger.Info("property 'subscriberEndpoint' does not include a path; adding '/api/events' automatically")
		u.Path = "/api/events"
	} else if u.Path != "/api/events" && u.Path != "api/events" {
		// Show a warning but still continue
		a.logger.Warn("property 'subscriberEndpoint' includes a path different from '/api/events': this is probably a mistake")
	}
	eventGridMetadata.SubscriberEndpoint = u.String()

	eventGridMetadata.Name = md.Name

	if eventGridMetadata.HandshakePort == "" {
		eventGridMetadata.HandshakePort = "8080"
	}

	if eventGridMetadata.EventSubscriptionName == "" {
		eventGridMetadata.EventSubscriptionName = md.Name
	}

	return &eventGridMetadata, nil
}

func (a *AzureEventGrid) createSubscription(parentCtx context.Context) error {
	// Get Azure Management plane credentials object
	settings, err := azauth.NewEnvironmentSettings("azure", a.metadata.properties)
	if err != nil {
		return err
	}
	creds, err := settings.GetTokenCredential()
	if err != nil {
		return fmt.Errorf("failed to obtain Azure AD management credentials: %w", err)
	}

	// Create a client
	client, err := armeventgrid.NewEventSubscriptionsClient(a.metadata.SubscriptionID, creds, &arm.ClientOptions{
		ClientOptions: policy.ClientOptions{
			Telemetry: policy.TelemetryOptions{
				ApplicationID: "dapr-" + logger.DaprVersion,
			},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to create Azure Resource Manager client: %w", err)
	}

	// First, check if the event subscription already exists so we don't override it
	ctx, cancel := context.WithTimeout(parentCtx, armOperationTimeout)
	defer cancel()
	res, err := client.Get(ctx, a.metadata.Scope, a.metadata.EventSubscriptionName, nil)
	if err == nil {
		// If there's no error, the subscription already exists, but check if the endpoint URL matches
		if res.Properties != nil && res.Properties.Destination != nil &&
			res.Properties.EventDeliverySchema != nil && *res.Properties.EventDeliverySchema == armeventgrid.EventDeliverySchemaCloudEventSchemaV10 &&
			*res.Properties.Destination.GetEventSubscriptionDestination().EndpointType == armeventgrid.EndpointTypeWebHook {
			webhookDestination, ok := res.Properties.Destination.(*armeventgrid.WebHookEventSubscriptionDestination)
			if ok && webhookDestination != nil && webhookDestination.Properties != nil {
				props := webhookDestination.Properties
				// Could either be endpointURL or EndpointBaseURL
				if (props.EndpointURL != nil && *props.EndpointURL == a.metadata.SubscriberEndpoint) ||
					(props.EndpointBaseURL != nil && *props.EndpointBaseURL == a.metadata.SubscriberEndpoint) {
					a.logger.Debug("Event subscription already exists with the correct endpoint")
					return nil
				}
			}
		}
	} else {
		// Check if the error is a "not found" (just means we have to create the subscription) or something else
		resErr := &azcore.ResponseError{}
		if !errors.As(err, &resErr) || resErr.StatusCode != http.StatusNotFound {
			// Error is not a "Not found" but something else
			return fmt.Errorf("failed to check existing event subscription: %w", err)
		}
	}

	// Need to create or update the subscription
	// There can be a "race condition" here if multiple instances of Dapr are creating this at the same time
	// This may cause a failure but it will make Dapr un-healthy, and a restart of Dapr should fix it
	// It should only happen on the first startup anyways
	a.logger.Infof("Event subscription needs to be created or updated for endpoint URL '%s'", a.metadata.SubscriberEndpoint)
	var properties *armeventgrid.EventSubscriptionProperties
	if res.Properties != nil {
		properties = res.Properties
	} else {
		properties = &armeventgrid.EventSubscriptionProperties{}
	}
	properties.EventDeliverySchema = ptr.Of(armeventgrid.EventDeliverySchemaCloudEventSchemaV10)
	properties.Destination = &armeventgrid.WebHookEventSubscriptionDestination{
		EndpointType: ptr.Of(armeventgrid.EndpointTypeWebHook),
		Properties: &armeventgrid.WebHookEventSubscriptionDestinationProperties{
			EndpointURL: &a.metadata.SubscriberEndpoint,
		},
	}
	ctx, cancel = context.WithTimeout(parentCtx, armOperationTimeout)
	defer cancel()
	poller, err := client.BeginCreateOrUpdate(ctx, a.metadata.Scope, a.metadata.EventSubscriptionName, armeventgrid.EventSubscription{Properties: properties}, nil)
	if err != nil {
		return fmt.Errorf("failed to create or update event subscription: %w", err)
	}
	// Need to use parentCtx here because it can be long-lasting
	_, err = poller.PollUntilDone(parentCtx, &runtime.PollUntilDoneOptions{Frequency: 10 * time.Second})
	if err != nil {
		return fmt.Errorf("failed to create or update event subscription: %w", err)
	}
	a.logger.Info("Event subscription has been updated")

	return nil
}
