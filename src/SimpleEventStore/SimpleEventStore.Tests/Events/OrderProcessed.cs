using System;

namespace SimpleEventStore.Tests.Events
{
    public class OrderProcessed
    {
        public string OrderId { get; private set; }

        public Version OrderProcessorVersion { get; set; }

        public OrderProcessed(string orderId, Version orderProcessorVersion)
        {
            OrderId = orderId;
            OrderProcessorVersion = orderProcessorVersion;
        }
    }
}