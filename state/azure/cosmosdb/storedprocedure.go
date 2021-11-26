// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package cosmosdb

const spDefinition string = `// upserts - an array of objects to upsert
// deletes - an array of objects to delete

function dapr_multi(upserts, deletes) {
    var context = getContext();
    var container = context.getCollection();
    var response = context.getResponse();
    
    if (typeof upserts === "string") {
        throw new Error("first arg is a string, expected array of objects");
    }

    if (typeof deletes === "string") {
        throw new Error("second arg is a string, expected array of objects");
    }

    // create the query string used to look up deletes    
    var query = "select * from n where n.id in ";
    if (deletes.length > 0) {        
        query += ("('" + deletes[0].item.id + "'");

        for (let j = 1; j < deletes.length; j++) {            
            query += ", '" + deletes.item[j].id + "'" 
        }
    }

    query += ')'
    console.log("query" + query)
    var upsertCount = 0;
    var deleteCount = 0;
      
    var collectionLink = container.getSelfLink();

    // do the upserts first    
    if (upserts.length != 0) {
        tryCreate(upserts[upsertCount], callback);
    } else {
        tryQueryAndDelete();
    }

    function tryCreate(doc, callback) {        
        var isAccepted = container.upsertDocument(collectionLink, doc.item, { etag: doc.etag }, callback);
        
        // fail if we hit execution bounds
        if (!isAccepted) {                        
            throw new Error("upsertDocument() not accepted, please retry");
        }
    }

    function callback(err, doc, options) {        
        if (err) throw err;

        upsertCount++;

        if (upsertCount >= upserts.length) {
            
            // upserts are done, start the deletes, if any
            if (deletes.length > 0) {
                tryQueryAndDelete()
            }
        } else {            
            tryCreate(upserts[upsertCount], callback);
        }
    }

    function tryQueryAndDelete() {    
		var requestOptions = {};            
        var isAccepted = container.queryDocuments(collectionLink, query, requestOptions, function (err, retrievedDocs, responseOptions) {
            if (err) {
                throw err;
            }

            if (retrievedDocs == null) {                
                response.setBody(JSON.stringify("success"));
            } else if (retrievedDocs.length > 0) {                
                tryDelete(retrievedDocs);			
            } else {                
                // done with all deletes                
                response.setBody(JSON.stringify("success"));
            }
        });

        // fail if we hit execution bounds
        if (!isAccepted) {
            throw new Error("queryDocuments() not accepted, please retry");
        }
    }

    function tryDelete(documents) {
        if (documents.length > 0) {        
            var del = deletes.find(d => d.item.id === documents[0].id);
            
            // Delete the first document in the array.
            var isAccepted = container.deleteDocument(documents[0]._self, { etag: del ? del.etag : undefined  }, function (err, responseOptions) {
                if (err) throw err;

                deleteCount++;
                documents.shift();
                // Delete the next document in the array.
                tryDelete(documents);
            });

            // fail if we hit execution bounds
            if (!isAccepted) {
                throw new Error("deleteDocument() not accepted, please retry");
            }
        } else {
            // If the document array is empty, query for more documents.
            tryQueryAndDelete();
        }
    }
}`
