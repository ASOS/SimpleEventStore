using System;

namespace SimpleEventStore.AzureDocumentDb
{
    public class SubscriptionOptions
    {
        public SubscriptionOptions()
            : this(100, TimeSpan.FromSeconds(5))
        {
        }

        public SubscriptionOptions(int maxItemCount, TimeSpan pollEvery)
        {
            this.MaxItemCount = maxItemCount;
            this.PollEvery = pollEvery;
        }

        public int MaxItemCount { get; }

        public TimeSpan PollEvery { get; }
    }
}