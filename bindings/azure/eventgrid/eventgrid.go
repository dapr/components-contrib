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
	"reflect"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	armeventgrid "github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/eventgrid/armeventgrid/v2"
	"github.com/lestrrat-go/jwx/v2/jwk"
	"github.com/lestrrat-go/jwx/v2/jws"
	"github.com/lestrrat-go/jwx/v2/jwt"
	"github.com/valyala/fasthttp"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/contenttype"
	azauth "github.com/dapr/components-contrib/internal/authentication/azure"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"
)

const (
	// Timeout for operations to Azure Resource Manager
	armOperationTimeout = 30 * time.Second
	// Format for the "jwks_uri" endpoint
	// The %s refers to the tenant ID
	jwksURIFormat = "https://login.microsoftonline.com/%s/discovery/v2.0/keys"
	// Format for the "iss" claim in the JWT
	// The %s refers to the tenant ID
	jwtIssuerFormat = "https://login.microsoftonline.com/%s/v2.0"
)

// AzureEventGrid allows sending/receiving Azure Event Grid events.
type AzureEventGrid struct {
	metadata *azureEventGridMetadata
	logger   logger.Logger
	jwks     jwk.Set
	closeCh  chan struct{}
	closed   atomic.Bool
	wg       sync.WaitGroup
}

type azureEventGridMetadata struct {
	// Component Name
	Name string `json:"-" mapstructure:"-"`

	// Required Input Binding Metadata
	SubscriberEndpoint string `json:"subscriberEndpoint" mapstructure:"subscriberEndpoint"`
	HandshakePort      string `json:"handshakePort" mapstructure:"handshakePort"`
	Scope              string `json:"scope" mapstructure:"scope"`

	// Optional Input Binding Metadata
	EventSubscriptionName string `json:"eventSubscriptionName" mapstructure:"eventSubscriptionName"`

	// Required Output Binding Metadata
	AccessKey     string `json:"accessKey" mapstructure:"accessKey"`
	TopicEndpoint string `json:"topicEndpoint" mapstructure:"topicEndpoint"`

	// Internal
	azureTenantID       string // Accepted values include: azureTenantID or tenantID
	azureClientID       string // Accepted values include: azureClientID or clientID
	azureSubscriptionID string // Accepted values: azureSubscriptionID or subscriptionID
	subscriberPath      string
	properties          map[string]string
}

// NewAzureEventGrid returns a new Azure Event Grid instance.
func NewAzureEventGrid(logger logger.Logger) bindings.InputOutputBinding {
	return &AzureEventGrid{
		logger:  logger,
		closeCh: make(chan struct{}),
	}
}

// Init performs metadata init.
func (a *AzureEventGrid) Init(_ context.Context, metadata bindings.Metadata) error {
	m, err := a.parseMetadata(metadata)
	if err != nil {
		return err
	}
	a.metadata = m

	return nil
}

var matchAuthHeader = regexp.MustCompile(`(?i)^(Bearer )?(([A-Za-z0-9_-]+\.){2}[A-Za-z0-9_-]+)$`)

func (a *AzureEventGrid) validateAuthHeader(ctx context.Context, authorizationHeader string) bool {
	// Extract the bearer token from the header
	if authorizationHeader == "" {
		a.logger.Error("Incoming webhook request does not contain an Authorization header")
		return false
	}
	match := matchAuthHeader.FindStringSubmatch(authorizationHeader)
	if len(match) < 3 {
		a.logger.Error("Incoming webhook request does not contain a valid bearer token in the Authorization header")
		return false
	}
	token := match[2]

	// Validate the JWT
	_, err := jwt.ParseString(
		token,
		jwt.WithKeySet(a.jwks, jws.WithInferAlgorithmFromKey(true)),
		jwt.WithAudience(a.metadata.azureClientID),
		jwt.WithIssuer(fmt.Sprintf(jwtIssuerFormat, a.metadata.azureTenantID)),
		jwt.WithAcceptableSkew(5*time.Minute),
		jwt.WithContext(ctx),
	)
	if err != nil {
		a.logger.Errorf("Failed to validate JWT in the incoming webhook request: %v", err)
		return false
	}

	return true
}

// Initializes the JWKS cache
func (a *AzureEventGrid) initJWKSCache(ctx context.Context) error {
	// Init the cache with the given URL
	jwkURL := fmt.Sprintf(jwksURIFormat, a.metadata.azureTenantID)

	c := jwk.NewCache(ctx)
	c.Register(jwkURL, jwk.WithMinRefreshInterval(15*time.Minute))

	// Do a first refresh to validate the JWKS keybag
	_, err := c.Refresh(ctx, jwkURL)
	if err != nil {
		return fmt.Errorf("failed to perform initial refresh of JWKS: %w", err)
	}

	a.jwks = jwk.NewCachedSet(c, jwkURL)
	return nil
}

func (a *AzureEventGrid) Read(ctx context.Context, handler bindings.Handler) error {
	if a.closed.Load() {
		return errors.New("binding is closed")
	}

	err := a.ensureInputBindingMetadata()
	if err != nil {
		return err
	}

	err = a.initJWKSCache(ctx)
	if err != nil {
		return err
	}

	srv := &fasthttp.Server{
		Handler: a.requestHandler(handler),
	}

	// Run the server in background
	a.wg.Add(2)
	go func() {
		defer a.wg.Done()
		a.logger.Infof("Listening for Event Grid events at http://localhost:%s%s", a.metadata.HandshakePort, a.metadata.subscriberPath)
		srvErr := srv.ListenAndServe(":" + a.metadata.HandshakePort)
		if err != nil {
			a.logger.Errorf("Error starting server: %v", srvErr)
		}
	}()
	// Close the server when context is canceled or binding closed.
	go func() {
		defer a.wg.Done()
		select {
		case <-ctx.Done():
		case <-a.closeCh:
		}
		srvErr := srv.Shutdown()
		if err != nil {
			a.logger.Errorf("Error shutting down server: %v", srvErr)
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

func (a *AzureEventGrid) Close() error {
	if a.closed.CompareAndSwap(false, true) {
		close(a.closeCh)
	}
	a.wg.Wait()
	return nil
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
	request.Header.Set("Content-Type", contenttype.CloudEventContentType)
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
		a.logger.Errorf("Error sending message: %v", err)
		return nil, err
	}

	if response.StatusCode() != http.StatusOK {
		body := response.Body()
		err = fmt.Errorf("invalid status code (%d) - response: %s", response.StatusCode(), string(body))
		a.logger.Errorf("Error sending message: %v", err)
		return nil, err
	}

	// a.logger.Debugf("Successfully posted event to %s", a.metadata.TopicEndpoint)

	return nil, nil
}

// Returns the fasthttp handler for the server
func (a *AzureEventGrid) requestHandler(handler bindings.Handler) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		var err error
		method := string(ctx.Method())

		// Only respond to requests on subscriberPath for methods POST and OPTIONS
		if method != http.MethodPost && method != http.MethodOptions {
			ctx.Response.Header.SetStatusCode(http.StatusMethodNotAllowed)
			_, err = ctx.Response.BodyWriter().Write([]byte("405 Method Not Allowed"))
			if err != nil {
				a.logger.Errorf("Error writing response: %v", err)
			}
			return
		}
		if string(ctx.Path()) != a.metadata.subscriberPath {
			ctx.Response.Header.SetStatusCode(http.StatusNotFound)
			_, err = ctx.Response.BodyWriter().Write([]byte("404 Not found"))
			if err != nil {
				a.logger.Errorf("Error writing response: %v", err)
			}
			return
		}

		// Validate the Authorization header
		authorizationHeader := string(ctx.Request.Header.Peek("authorization"))
		// Note that ctx is a fasthttp context so it's actually tied to the server's lifecycle and not the request's
		if !a.validateAuthHeader(ctx, authorizationHeader) {
			ctx.Response.Header.SetStatusCode(http.StatusUnauthorized)
			_, err = ctx.Response.BodyWriter().Write([]byte("401 Unauthorized"))
			if err != nil {
				a.logger.Errorf("Error writing response: %v", err)
			}
			return
		}

		switch method {
		case http.MethodOptions:
			ctx.Response.Header.Add("WebHook-Allowed-Origin", string(ctx.Request.Header.Peek("WebHook-Request-Origin")))
			ctx.Response.Header.Add("WebHook-Allowed-Rate", "*")
			ctx.Response.Header.SetStatusCode(http.StatusOK)
			_, err = ctx.Response.BodyWriter().Write([]byte(""))
			if err != nil {
				a.logger.Errorf("Error writing response: %v", err)
			}
		case http.MethodPost:
			_, err = handler(ctx, &bindings.ReadResponse{
				Data: ctx.PostBody(),
			})
			if err != nil {
				a.logger.Errorf("Error writing response: %v", err)
				ctx.Error(err.Error(), http.StatusInternalServerError)
			}
		}
	}
}

func (a *AzureEventGrid) ensureInputBindingMetadata() error {
	a.metadata.azureTenantID, _ = metadata.GetMetadataProperty(a.metadata.properties, azauth.MetadataKeys["TenantID"]...)
	if a.metadata.azureTenantID == "" {
		return errors.New("metadata field 'azureTenantID' is empty in EventGrid binding")
	}
	a.metadata.azureClientID, _ = metadata.GetMetadataProperty(a.metadata.properties, azauth.MetadataKeys["ClientID"]...)
	if a.metadata.azureClientID == "" {
		return errors.New("metadata field 'azureClientID' is empty in EventGrid binding")
	}
	a.metadata.azureSubscriptionID, _ = metadata.GetMetadataProperty(a.metadata.properties, "subscriptionID", "azureSubscriptionID")
	if a.metadata.azureSubscriptionID == "" {
		return errors.New("metadata field 'azureSubscriptionID' is empty in EventGrid binding")
	}
	if a.metadata.SubscriberEndpoint == "" {
		return errors.New("metadata field 'subscriberEndpoint' is empty in EventGrid binding")
	}
	if a.metadata.HandshakePort == "" {
		return errors.New("metadata field 'handshakePort' is empty in EventGrid binding")
	}
	if a.metadata.Scope == "" {
		return errors.New("metadata field 'scope' is empty in EventGrid binding")
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
	}

	eventGridMetadata.SubscriberEndpoint = u.String()
	eventGridMetadata.subscriberPath = u.Path
	if !strings.HasPrefix(eventGridMetadata.subscriberPath, "/") {
		eventGridMetadata.subscriberPath = "/" + eventGridMetadata.subscriberPath
	}

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
	settings, err := azauth.NewEnvironmentSettings(a.metadata.properties)
	if err != nil {
		return err
	}
	creds, err := settings.GetTokenCredential()
	if err != nil {
		return fmt.Errorf("failed to obtain Azure AD management credentials: %w", err)
	}

	// Create a client
	client, err := armeventgrid.NewEventSubscriptionsClient(a.metadata.azureSubscriptionID, creds, &arm.ClientOptions{
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
		// If there's no error, the subscription already exists, but check if it is up-to-date
		if !a.subscriptionNeedsUpdating(res) {
			a.logger.Debug("Event subscription already exists with the correct endpoint")
			return nil
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
		// If the destination is not a webhook, override the entire destination
		if properties.Destination == nil || properties.Destination.GetEventSubscriptionDestination() == nil || *properties.Destination.GetEventSubscriptionDestination().EndpointType != armeventgrid.EndpointTypeWebHook {
			// Will be overridden later
			properties.Destination = nil
		} else {
			// Surgically override only the properties we care about
			destination, ok := properties.Destination.(*armeventgrid.WebHookEventSubscriptionDestination)
			if ok && destination != nil {
				destination.EndpointType = ptr.Of(armeventgrid.EndpointTypeWebHook)
				if destination.Properties == nil {
					destination.Properties = &armeventgrid.WebHookEventSubscriptionDestinationProperties{}
				}
				destination.Properties.AzureActiveDirectoryTenantID = &a.metadata.azureTenantID
				destination.Properties.AzureActiveDirectoryApplicationIDOrURI = &a.metadata.azureClientID
				destination.Properties.EndpointURL = &a.metadata.SubscriberEndpoint
			}
		}
	} else {
		properties = &armeventgrid.EventSubscriptionProperties{}
	}
	// Override EventDeliverySchema regardless
	properties.EventDeliverySchema = ptr.Of(armeventgrid.EventDeliverySchemaCloudEventSchemaV10)
	// Set Destination if not yet set
	if properties.Destination == nil {
		properties.Destination = &armeventgrid.WebHookEventSubscriptionDestination{
			EndpointType: ptr.Of(armeventgrid.EndpointTypeWebHook),
			Properties: &armeventgrid.WebHookEventSubscriptionDestinationProperties{
				AzureActiveDirectoryTenantID:           &a.metadata.azureTenantID,
				AzureActiveDirectoryApplicationIDOrURI: &a.metadata.azureClientID,
				EndpointURL:                            &a.metadata.SubscriberEndpoint,
			},
		}
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

// Checks an existing Event Subscription to see if it needs to be updated.
func (a *AzureEventGrid) subscriptionNeedsUpdating(res armeventgrid.EventSubscriptionsClientGetResponse) bool {
	// Quick sanity check
	if res.Properties == nil {
		return true
	}

	// Schema must use Cloud Event 1.0
	if res.Properties.EventDeliverySchema == nil || *res.Properties.EventDeliverySchema != armeventgrid.EventDeliverySchemaCloudEventSchemaV10 {
		return true
	}

	// Destination must be a webhook
	if res.Properties.Destination == nil || res.Properties.Destination.GetEventSubscriptionDestination() == nil || *res.Properties.Destination.GetEventSubscriptionDestination().EndpointType != armeventgrid.EndpointTypeWebHook {
		return true
	}
	webhookDestination, ok := res.Properties.Destination.(*armeventgrid.WebHookEventSubscriptionDestination)
	if !ok || webhookDestination == nil || webhookDestination.Properties == nil {
		return true
	}
	props := webhookDestination.Properties

	// Check endpoint
	// Could either be endpointURL or EndpointBaseURL
	if (props.EndpointURL == nil || *props.EndpointURL != a.metadata.SubscriberEndpoint) &&
		(props.EndpointBaseURL == nil || *props.EndpointBaseURL != a.metadata.SubscriberEndpoint) {
		return true
	}

	// Azure AD credentials
	if props.AzureActiveDirectoryApplicationIDOrURI == nil || *props.AzureActiveDirectoryApplicationIDOrURI != a.metadata.azureClientID {
		return true
	}
	if props.AzureActiveDirectoryTenantID == nil || *props.AzureActiveDirectoryTenantID != a.metadata.azureTenantID {
		return true
	}

	// All good
	return false
}

// GetComponentMetadata returns the metadata of the component.
func (a *AzureEventGrid) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := azureEventGridMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.BindingType)
	return
}
