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
	"net/http"

	"github.com/Azure/azure-sdk-for-go/services/eventgrid/mgmt/2019-06-01/eventgrid"
	"github.com/Azure/go-autorest/autorest/azure/auth"
	"github.com/dapr/components-contrib/bindings"
	log "github.com/sirupsen/logrus"
)

// AzureEventGrid allows sending/receiving Azure Event Grid events
type AzureEventGrid struct {
	metadata *azureEventGridMetadata
}

type azureEventGridMetadata struct {
	TenantID                  string `json:"tenantId"`
	SubscriptionID            string `json:"subscriptionId"`
	ResourceGroupName         string `json:"resourceGroupName"`
	TopicName                 string `json:"topicName"`
	EventGridSubscriptionName string `json:"eventGridSubscriptionName"`
	SubscriberEndpoint        string `json:"subscriberEndpoint"`
	ClientID                  string `json:"clientId"`
	ClientSecret              string `json:"clientSecret"`
}

// NewAzureEventGrid returns a new Azure Event Grid instance
func NewAzureEventGrid() *AzureEventGrid {
	log.Info("NewAzureEventGrid() called...")
	return &AzureEventGrid{}
}

// Init performs metadata init
func (a *AzureEventGrid) Init(metadata bindings.Metadata) error {
	log.Info("Reading Event Grid metadata...")
	m, err := a.parseMetadata(metadata)
	if err != nil {
		return err
	}
	a.metadata = m

	err = a.createSubscription()
	if err != nil {
		return err
	}

	return nil
}

func (a *AzureEventGrid) Read(handler func(*bindings.ReadResponse) error) error {
	http.HandleFunc("/api/events", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "OPTIONS" {
			w.Header().Add("WebHook-Allowed-Origin", r.Header.Get("WebHook-Request-Origin"))
			w.Header().Add("WebHook-Allowed-Rate", "*")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(""))
		} else if r.Method == "POST" {
			bodyBytes, err := ioutil.ReadAll(r.Body)
			if err != nil {
				log.Error(err)
			}

			log.Info(string(bodyBytes))
			err = handler(&bindings.ReadResponse{
				Data: bodyBytes,
			})
		}
	})

	go http.ListenAndServe(":8080", nil)

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

// func messageHandler(w http.ResponseWriter, r *http.Request) {
// 	if r.Method == "OPTIONS" {
// 		w.Header().Add("WebHook-Allowed-Origin", r.Header.Get("WebHook-Request-Origin"))
// 		w.Header().Add("WebHook-Allowed-Rate", "*")
// 		w.WriteHeader(http.StatusOK)
// 		w.Write([]byte(""))
// 	} else if r.Method == "POST" {
// 		bodyBytes, err := ioutil.ReadAll(r.Body)
// 		if err != nil {
// 			log.Error(err)
// 		}

// 		log.Info(string(bodyBytes))
// 	}
// }

func (a *AzureEventGrid) createSubscription() error {
	clientCredentialsConfig := auth.NewClientCredentialsConfig(a.metadata.ClientID, a.metadata.ClientSecret, a.metadata.TenantID)

	subscriptionClient := eventgrid.NewEventSubscriptionsClient(a.metadata.SubscriptionID)
	authorizer, err := clientCredentialsConfig.Authorizer()
	if err != nil {
		return err
	}
	subscriptionClient.Authorizer = authorizer

	filter := fmt.Sprintf("name eq '%s'", a.metadata.EventGridSubscriptionName)
	top := int32(10)
	result, err := subscriptionClient.ListByResource(context.Background(), a.metadata.ResourceGroupName, "Microsoft.EventGrid", "topics", a.metadata.TopicName, filter, &top)
	if err != nil {
		return err
	}

	subscriptions := result.Values()

	//TODO: May not need to check this, instead just update it if it already exists
	if len(subscriptions) > 0 {
		log.WithFields(log.Fields{"subscription": *subscriptions[0].Name}).Info("Subscription already exists. Skipping creation.")
	} else {
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

		result, err := subscriptionClient.CreateOrUpdate(context.Background(), scope, a.metadata.EventGridSubscriptionName, eventInfo)
		if err != nil {
			return err
		}

		res := result.Future.Response()

		if res.StatusCode != http.StatusCreated {
			bodyBytes, err := ioutil.ReadAll(res.Body)
			if err != nil {
				return err
			}
			return errors.New(string(bodyBytes))
		}
	}

	return nil
}
