namespace SimpleEventStore.Tests.Events
{
    public class PaymentTaken
    {
        public decimal Amount { get; }

        public PaymentTaken(decimal amount)
        {
            Amount = amount;
        }
    }
}