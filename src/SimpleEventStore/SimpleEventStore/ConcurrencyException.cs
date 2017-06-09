using System;

namespace SimpleEventStore
{
    public class ConcurrencyException : Exception
    {
        public ConcurrencyException()
        {
        }

        public ConcurrencyException(string message) : base(message)
        {
        }

        public ConcurrencyException(string message, Exception inner) : base(message, inner)
        {
        }
    }
}