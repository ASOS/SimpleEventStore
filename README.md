# Simple Event Store

Simple Event Store (SES) provides a lightweight event sourcing abstraction.

## Key Features
- Full support for async/await for all persistence engines
- Optimistic concurrency for append operations
- Reading streams forward

## Persistence Engines
SES supports the following
- Azure Cosmos DB
- In Memory

All persistence engines run the same test scenarios to ensure feature parity.  At present only the Cosmos DB persistence engine should be considered for production usage.

## Usage

### Creation
```csharp
var storageEngine = new InMemoryStorageEngine();
var eventStore = new EventStore(storageEngine);
```
Do not use storage engines directly, only interact with the event store using the EventStore class.  There should only be a single instance of the EventStore per process.  Creating transient instances will lead to a decrease in performance.

### Appending Events
```csharp
var expectedRevision = 0;

await eventStore.AppendToStream(streamId, expectedRevision, new EventData(Guid.NewGuid(), new OrderCreated(streamId)));
```
The expected stream revision would either be set to 0 for a new stream, or to the expected event number.  If the latest event number in the database differs then a concurrency exception will be thrown.

### Reading Events
```csharp
var events = await eventStore.ReadStreamForwards(streamId, startPosition: 2, numberOfEventsToRead: 1);
// or
var events = await subject.ReadStreamForwards(streamId);
```
You can either read all events in a stream, or a subset of events.  Only read all events if you know the maximum size of a stream is going to be low and that you always need to read all events as part of your workload e.g. replaying events to project current state for a DDD aggregate.

## Cosmos DB
```csharp
DocumentClient client; // Set this up as required

// If UseCollection isn't specified, sensible defaults for development are used.
// If UseSubscriptions isn't supplied the subscription feature is disabled.
// If UseSharedThroughput isn't supplied the throughput is set only at a collection level
return await new AzureDocumentDbStorageEngineBuilder(client, databaseName)
	.UseSharedThroughput(o => {
		o.DatabaseRequestUnits = 400;
	})
	.UseCollection(o =>
   	{
		o.ConsistencyLevel = consistencyLevelEnum;
		o.CollectionRequestUnits = 400;
	})
	.UseLogging(o =>
	{
		o.Success = onSuccessCallback;
	})
	.UseTypeMap(new ConfigurableSerializationTypeMap()
		.RegisterTypes(
			typeof(OrderCreated).GetTypeInfo().Assembly,
			t => t.Namespace.EndsWith("Events"),
			t => t.Name))
	.UseJsonSerializerSettings(settings)
	.Build()
	.Initialise();
```
### Database Options
Use this only if you want throughput to be set at a database level

Allows you to specify
- The number of RUs for the database


### CollectionOptions
Allows you to specify
- The consistency level of the database
- The number of RUs for the collection - if throughput is set at a database level this cannot be greater than database throughput
- The collection name

Only use one of the following consistency levels
- Strong
- Bounded Staleness - use this if you need to geo-replicate the database

### UseLogging
Sets up callbacks per Cosmos DB operation performed.  This is useful if you want to record per call data e.g. RU cost of each operation.

### UseTypeMap
Allows you to control the event body/metadata type names.  Built in implementations
- DefaultSerializationTypeMap - uses the AssemblyQualifiedName of the type. (default)
- ConfigurableSerializationTypeMap - provides full control.

While the default implementation is simple, this isn't great for versioning as contract assembly version number changes will render events unreadable.  Therefore the configurable implementation or your own implementation is recommended.

### UseJsonSerializerSettings
Allows you to customise JSON serialization of the event body and metadata.  If you do not call this method a default JsonSerializerSettings instance is used.

For consistent serialization provide the same serialzer settings to your DocumentClient.

### Initialise
Calling the operation creates the underlying collection based on the DatabaseOptions.  This ensures the required stored procedure is present too.  It is safe to call this multiple times.
