/*
Copyright 2022 The Dapr Authors
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

// Package apns implements an output binding for Dapr that allows services to
// send push notifications to Apple devices and Mac computers using Apple's
// Push Notification Service (APNS).
//
// Configuring the Binding
//
// To use the APNS output binding, you will need to create the binding
// configuration and add it to your components directory. The binding
// configuration will contain parameters that will allow the binding to
// connect to the APNS service specified as metadata.
//
// The APNS binding will need a cryptographic private key in order to generate
// authentication tokens for the APNS service. The private key can be generated
// from the Apple Developer Portal and is provided as a PKCS #8 file with the
// private key stored in PEM format. The private key should be stored in the
// Dapr secret store and not stored directly in the binding's configuration
// file.
//
// A sample configuration file for the APNS binding is shown below:
//
//     apiVersion: dapr.io/v1alpha1
//     kind: Component
//     metadata:
//       name: apns
//       namespace: default
//     spec:
//       type: bindings.apns
//       metadata:
//       - name: development
//         value: false
//       - name: key-id
//         value: PUT-KEY-ID-HERE
//       - name: team-id
//         value: PUT-APPLE-TEAM-ID-HERE
//       - name: private-key
//         secretKeyRef:
//           name: apns-secrets
//           key: private-key
//
// If using Kubernetes, a sample secret configuration may look like this:
//
//     apiVersion: v1
//     kind: Secret
//     metadata:
//         name: apns-secrets
//         namespace: default
//     stringData:
//         private-key: |
//             -----BEGIN PRIVATE KEY-----
//             KEY-DATA-GOES-HERE
//             -----END PRIVATE KEY-----
//
// The development parameter can be either "true" or "false". The development
// parameter controls which APNS service is used. If development is set to
// true, then the sandbox APNS service will be used to send push notifications
// to devices. If development is set to false, the production APNS service will
// be used to send push notifications. If not specified, the production service
// will be chosen by default.
//
// Push Notification Format
//
// The APNS binding is a pass-through wrapper over the Apple Push Notification
// Service. The APNS binding will send the request directly to the APNS service
// without any translation. It is therefore important to understand the payload
// for push notifications expected by the APNS service. The payload format is
// documented at https://developer.apple.com/documentation/usernotifications/setting_up_a_remote_notification_server/generating_a_remote_notification.
//
// Requests sent to the APNS binding should be a JSON object. A simple push
// notification appears below:
//
//     {
//         "aps": {
//             "alert": {
//                 "title": "New Updates!",
//                 "body": "New updates are now available for your review."
//             }
//         }
//     }
//
// The aps child object contains the push notification details that are used
// by the Apple Push Notification Service and target devices to route and show
// the push notification. Additional objects or values can be added to the push
// notification envelope for use by applications to handle the push
// notification.
//
// The APNS binding accepts several metadata values that are mapped directly
// to HTTP headers in the APNS publish request. Below is a summary of the valid
// metadata fields. For more information, please see
// https://developer.apple.com/documentation/usernotifications/setting_up_a_remote_notification_server/generating_a_remote_notification.
//
// * apns-push-type: Identifies the content of the notification payload. One of
// alert, background, voip, complication, fileprovider, mdm.
//
// * apns-id: a UUID that uniquely identifies the push notification. This value
// is returned by APNS if provided and can be used to track notifications.
//
// * apns-expiration: The date/time at which the notification is no longer
// valid and should not be delivered. This value is the number of seconds
// since the UNIX epoch (January 1, 1970 at 00:00 UTC). If not specified or
// if 0, the message is sent once immediately and then discarded.
//
// * apns-priority: If 10, the notification is sent immediately. If 5, the
// notification is sent based on power conditions of the user's device.
// Defaults to 10.
//
// * apns-topic: The topic for the notification. Typically this is the bundle
// identifier of the target app.
//
// * apns-collapse-id: A correlation identifier that will cause notifications
// to be displayed as a group on the target device. For example, multiple
// notifications from a chat room may have the same identifier causing them
// to show up together in the device's notifications list.
//
// Sending a Push Notification Using the APNS Binding
//
// A simple request to the APNS binding looks like this:
//
//     {
//         "data": {
//             "aps": {
//                 "alert": {
//                     "title": "New Updates!",
//                     "body": "New updates are available for your review."
//                 }
//             }
//         },
//         "metadata": {
//             "device-token": "PUT-DEVICE-TOKEN-HERE",
//             "apns-push-type": "alert",
//             "apns-priority": "10",
//             "apns-topic": "com.example.helloworld"
//         },
//         "operation": "create"
//     }
//
// The device-token metadata field is required and should contain the token
// for the device that will receive the push notification. Only one device
// can be specified per request to the APNS binding.
//
// The APNS binding only supports one operation: create. Specifying any other
// operation name will result in a runtime error.
//
// If the push notification is successfully sent, the response will be a JSON
// object containing the message ID. If a message ID was not specified using
// the apns-id metadata value, then the Apple Push Notification Serivce will
// generate a unique ID and will return it.
//
//     {
//         "messageID": "12345678-1234-1234-1234-1234567890AB"
//     }
//
// If the push notification could not be sent due to an authentication error
// or payload error, the error code returned by Apple will be returned. For
// a list of error codes and their meanings, see
// https://developer.apple.com/documentation/usernotifications/setting_up_a_remote_notification_server/handling_notification_responses_from_apns.
package apns
