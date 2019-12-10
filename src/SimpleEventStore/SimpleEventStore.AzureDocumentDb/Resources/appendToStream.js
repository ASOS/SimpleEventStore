function appendToStream(events) {
    var context = getContext();
    var collection = context.getCollection();
    var streamId = events[0].streamId;
    var firstEventNumber = events[0].eventNumber;

    if (firstEventNumber === 1) {
        createEvents();
    }
    else {
        createEventsOnlyIfPreviousEventExists();
    }

    function createEventsOnlyIfPreviousEventExists() {
        var previousEventNumber = firstEventNumber - 1;
        var previousDocId = streamId + ":" + previousEventNumber;
        var previousDocLink = collection.getAltLink() + '/docs/' + previousDocId;

        var isAccepted = collection.readDocument(previousDocLink, {}, onPreviousEventCompleted);
        if (!isAccepted) {
            throw new Error(500, "Existing event read not accepted for creation.");
        }

        function onPreviousEventCompleted(err, doc, options) {
            if (err) {
                if (err.number === 404) {
                    throw new Error(409, "Previous Event" + previousEventNumber + " not found for stream '" + streamId + "'")
                }
                throw err;
            }

            createEvents();
        }
    }

    function createEvents() {
        var index = 0;
        var collectionLink = collection.getSelfLink();
        createDocument(events[index]);

        function createDocument(document) {
            var isAccepted = collection.createDocument(collectionLink, document, onDocumentCreated);

            if (!isAccepted) {
                throw new Error(500, "Event at index " + index + "not accepted for creation.");
            }
        }

        function onDocumentCreated(err, doc, options) {
            if (err) {
                if (err.number === 409) {
                    throw new Error(409, "Failed to create event at index " + index + ". Event Number " + events[index].eventNumber + " already exists for stream '" + streamId + "'")
                }
                throw err;
            }

            index++;

            if (index < events.length) {
                createDocument(events[index]);
            }
            else {
                context.getResponse().setBody("Events written successfully");
            }
        }
    }
}