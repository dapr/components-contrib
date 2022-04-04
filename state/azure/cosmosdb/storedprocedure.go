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

package cosmosdb

const spVersion = 2

const spDefinition string = `// operations - an array of objects to upsert or delete
function dapr_multi_v2(operations) {
    if (typeof operations === "string") {
        throw new Error("arg is a string, expected array of objects");
    }

    var context = getContext();
    var collection = context.getCollection();
    var collectionLink = collection.getSelfLink();
    var response = context.getResponse();

    // Upserts do not reflect until the transaction is committed,
    // as a result of which SELECT will not return the new values.
    // We need to store document URLs (_self) in order to do deletions.
    var documentMap = {}

    var operationCount = 0;
    if (operations.length > 0) {
        tryExecute(operations[operationCount], callback);
    }

    function tryExecute(operation, callback) {
        switch (operation["type"]) {
            case "upsert":
                tryCreate(operation["item"], callback);
                break;
            case "delete":
                tryQueryAndDelete(operation["item"], callback);
                break;
            default:
                throw new Error("operation type not supported - should be 'upsert' or 'delete'");
        }
    }

    function tryCreate(doc, callback) {
        var isAccepted = collection.upsertDocument(collectionLink, doc, callback);

        // Fail if we hit execution bounds.
        if (!isAccepted) {
            throw new Error("upsertDocument() not accepted, please retry");
        }
    }

    function tryQueryAndDelete(doc, callback) {
        // Check the cache first. We expect to find the document if it was upserted.
        var documentLink = documentMap[doc["id"]];
        if (documentLink) {
            tryDelete(documentLink, callback);
            return;
        }

        // Not found in cache, query for it.
        var query = "select n._self from n where n.id='" + doc["id"] + "'";
        console.log("query: " + query)

        var requestOptions = {};
        var isAccepted = collection.queryDocuments(collectionLink, query, requestOptions, function (err, retrievedDocs, _responseOptions) {
            if (err) throw err;

            if (retrievedDocs == null || retrievedDocs.length == 0) {
                // Nothing to delete.
                response.setBody(JSON.stringify("success"));
            } else {
                tryDelete(retrievedDocs[0]._self, callback);
            }
        });

        // fail if we hit execution bounds
        if (!isAccepted) {
            throw new Error("queryDocuments() not accepted, please retry");
        }
    }

    function tryDelete(documentLink, callback) {
        // Delete the first document in the array.
        var requestOptions = {};
        var isAccepted = collection.deleteDocument(documentLink, requestOptions, (err, _responseOptions) => {
            callback(err, null, _responseOptions);
        });

        // Fail if we hit execution bounds.
        if (!isAccepted) {
            throw new Error("deleteDocument() not accepted, please retry");
        }
    }

    function callback(err, doc, _options) {
        if (err) throw err;

        // Document references are stored for all upserts.
        // This can be used for further deletes in this transaction.
        if (doc && doc._self) documentMap[doc.id] = doc._self;

        operationCount++;

        if (operationCount >= operations.length) {
            // Operations are done.
            response.setBody(JSON.stringify("success"));
        } else {
            tryExecute(operations[operationCount], callback);
        }
    }
}`

const spVersionDefinition = `function daprSpVersion(prefix) {
    var response = getContext().getResponse();
    response.setBody(2);
}`
