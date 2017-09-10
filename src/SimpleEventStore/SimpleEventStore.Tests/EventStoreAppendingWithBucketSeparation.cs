using System;
using System.Linq;
using System.Threading.Tasks;
using SimpleEventStore.Tests.Events;
using Xunit;

namespace SimpleEventStore.Tests
{
    public abstract class EventStoreAppendingWithBucketSeparation : EventStoreWithBucketBase
    {
        [Fact]
        public async Task when_appending_to_an_existing_stream_identifier_but_in_a_different_bucket_the_event_is_saved_to_the_correct_bucket()
        {
            var streamId = Guid.NewGuid().ToString();

            var ordersEventStore = await GetOrdersEventStore();
            var orderEvent = new EventData(Guid.NewGuid(), new OrderCreated(OrdersBucket));
            await ordersEventStore.AppendToStream(streamId, 0, orderEvent);

            var paymentsEventStore = await GetPaymentsEventStore();
            var paymentEvent = new EventData(orderEvent.EventId, new PaymentTaken(new Random().Next(1, 100000)));
            await paymentsEventStore.AppendToStream(streamId, 0, paymentEvent);

            var events = await ordersEventStore.ReadStreamForwards(streamId);
            Assert.Equal(1, events.Count);
            Assert.Equal(OrdersBucket, events.Select(e => (OrderCreated)e.EventBody).First().OrderId);

            events = await paymentsEventStore.ReadStreamForwards(streamId);
            Assert.Equal(1, events.Count);
            Assert.Equal(((PaymentTaken)paymentEvent.Body).Amount, events.Select(e => (PaymentTaken)e.EventBody).First().Amount);
        }

    }
}