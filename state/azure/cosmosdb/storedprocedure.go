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

const spDefinition string = `// operations - an array of objects to upsert or delete
function dapr_multi_v2(operations) {
    if (typeof operations === "string") {
        throw new Error("arg is a string, expected array of objects");
    }

    var context = getContext();
    var container = context.getCollection();
    var response = context.getResponse();
    var collectionLink = container.getSelfLink();

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
                tryDelete(operation["item"], callback);
                break;
            default:
                throw new Error("operation type not supported - should be 'upsert' or 'delete'");
        }
    }

    function tryCreate(doc, callback) {        
        var isAccepted = container.upsertDocument(collectionLink, doc, callback);
        
        // fail if we hit execution bounds
        if (!isAccepted) {                        
            throw new Error("upsertDocument() not accepted, please retry");
        }
    }

    function tryDelete(doc, callback) {
        // Delete the first document in the array.
        var isAccepted = container.deleteDocument(doc, {}, callback);

        // fail if we hit execution bounds
        if (!isAccepted) {
            throw new Error("deleteDocument() not accepted, please retry");
        }
    } 

    function callback(err, _doc, _options) {        
        if (err) throw err;

        operationCount++;

        if (operationCount >= operations.length) {
            // operations are done
            response.setBody(JSON.stringify("success"));
        } else {            
            tryExecute(operations[operationCount], callback);
        }
    }
}`
