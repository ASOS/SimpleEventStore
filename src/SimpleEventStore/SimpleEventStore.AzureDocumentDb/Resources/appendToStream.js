function appendToStream(documents) {
    var context = getContext();
    var collection = context.getCollection();
    var collectionLink = collection.getSelfLink();
    var streamId = documents[0].streamId;
    var expectedEventNumber = documents[0].eventNumber - 1;
    var bucket = documents[0].bucket;

    var concurrencyQuery = {
        query: "SELECT TOP 1 c.eventNumber FROM Commits c WHERE c.streamId = @streamId AND (c.bucket ?? 'default') = @bucket ORDER BY c.eventNumber DESC",
        parameters: [{ name: "@streamId", value: streamId }, { name: "@bucket", value: bucket }]
    };

    var accepted = collection.queryDocuments(collectionLink, concurrencyQuery, {}, onConcurrencyQueryCompleted);

    if (!accepted) throw new Error("Concurrency query not accepted.");

    function onConcurrencyQueryCompleted(err, doc, options) {
        if (err) throw err;

        var actualEventNumber = doc.length > 0 ? doc[0].eventNumber : 0;

        if (actualEventNumber !== expectedEventNumber) {
            throw new Error("Concurrency conflict. Expected revision " + expectedEventNumber + ", actual " + actualEventNumber + ")");
        }

        createDocuments();
    }

    function createDocuments() {
        var index = 0;
        createDocument(documents[index]);

        // NOTE: isAccepted states if the write is going to be processed
        function createDocument(document) {
            var accepted = collection.createDocument(collectionLink, document, onDocumentCreated);

            if (!accepted) throw new Error("Document not accepted for creation.");
        }

        function onDocumentCreated(err, doc, options) {
            if (err) throw err;

            index++;

            if (index < documents.length) {
                createDocument(documents[index]);
            }
            else {
                context.getResponse().setBody("Events written successfully");
            }
        }
    }
}