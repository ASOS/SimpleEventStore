using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Net;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using NUnit.Framework;

namespace SimpleEventStore.AzureDocumentDb.Tests
{
    [TestFixture]
    public class ResponseInformationBuilding
    {
        [Test]
        public void when_building_from_a_write_response_all_target_fields_are_mapped()
        {
            var result = ResponseInformation.FromWriteResponse(Expected.RequestIdentifier, new FakeStoredProcedureResponse<dynamic>());

            Assert.That(result.RequestIdentifier, Is.EqualTo(Expected.RequestIdentifier));
            Assert.That(result.CurrentResourceQuotaUsage, Is.EqualTo(Expected.CurrentResourceQuotaUsage));
            Assert.That(result.MaxResourceQuota, Is.EqualTo(Expected.MaxResourceQuota));
            Assert.That(result.RequestCharge, Is.EqualTo(Expected.RequestCharge));
            Assert.That(result.ResponseHeaders, Is.EqualTo(Expected.ResponseHeaders));
        }

        [Test]
        public void when_building_from_a_read_response_all_target_fields_are_mapped()
        {
            var result = ResponseInformation.FromReadResponse(Expected.RequestIdentifier, new FakeFeedResponse<DocumentDbStorageEvent>());

            Assert.That(result.RequestIdentifier, Is.EqualTo(Expected.RequestIdentifier));
            Assert.That(result.CurrentResourceQuotaUsage, Is.EqualTo(Expected.CurrentResourceQuotaUsage));
            Assert.That(result.MaxResourceQuota, Is.EqualTo(Expected.MaxResourceQuota));
            Assert.That(result.RequestCharge, Is.EqualTo(Expected.RequestCharge));
            Assert.That(result.ResponseHeaders, Is.EqualTo(Expected.ResponseHeaders));
        }

        [Test]
        public void when_building_from_a_subscription_read_response_all_target_fields_are_mapped()
        {
            var result = ResponseInformation.FromSubscriptionReadResponse(Expected.RequestIdentifier, new FakeFeedResponse<Document>());

            Assert.That(result.RequestIdentifier, Is.EqualTo(Expected.RequestIdentifier));
            Assert.That(result.CurrentResourceQuotaUsage, Is.EqualTo(Expected.CurrentResourceQuotaUsage));
            Assert.That(result.MaxResourceQuota, Is.EqualTo(Expected.MaxResourceQuota));
            Assert.That(result.RequestCharge, Is.EqualTo(Expected.RequestCharge));
            Assert.That(result.ResponseHeaders, Is.EqualTo(Expected.ResponseHeaders));
        }

        private static class Expected
        {
            internal const string RequestIdentifier = "TEST-Identifier";
            internal const string CurrentResourceQuotaUsage = "TEST-CurrentResourceQuotaUsage";
            internal const string MaxResourceQuota = "TEST-MaxResourceQuota";
            internal const double RequestCharge = 100d;
            internal static NameValueCollection ResponseHeaders = new NameValueCollection();
        }

        private class FakeStoredProcedureResponse<TValue> : IStoredProcedureResponse<TValue>
        {
            internal FakeStoredProcedureResponse()
            {
                CurrentResourceQuotaUsage = Expected.CurrentResourceQuotaUsage;
                MaxResourceQuota = Expected.MaxResourceQuota;
                RequestCharge = Expected.RequestCharge;
                ResponseHeaders = Expected.ResponseHeaders;
            }

            public string ActivityId { get; }

            public string CurrentResourceQuotaUsage { get; }

            public string MaxResourceQuota { get; }

            public double RequestCharge { get; }

            public TValue Response { get; }

            public NameValueCollection ResponseHeaders { get; }

            public string SessionToken { get; }

            public string ScriptLog { get; }

            public HttpStatusCode StatusCode { get; }
        }

        private class FakeFeedResponse<TValue> : IFeedResponse<TValue>
        {
            internal FakeFeedResponse()
            {
                CurrentResourceQuotaUsage = Expected.CurrentResourceQuotaUsage;
                MaxResourceQuota = Expected.MaxResourceQuota;
                RequestCharge = Expected.RequestCharge;
                ResponseHeaders = Expected.ResponseHeaders;
            }

            public long DatabaseQuota { get; }

            public long DatabaseUsage { get; }

            public long CollectionQuota { get; }

            public long CollectionUsage { get; }

            public long UserQuota { get; }

            public long UserUsage { get; }

            public long PermissionQuota { get; }

            public long PermissionUsage { get; }

            public long CollectionSizeQuota { get; }

            public long CollectionSizeUsage { get; }

            public long StoredProceduresQuota { get; }

            public long StoredProceduresUsage { get; }

            public long TriggersQuota { get; }

            public long TriggersUsage { get; }

            public long UserDefinedFunctionsQuota { get; }

            public long UserDefinedFunctionsUsage { get; }

            public int Count { get; }

            public string MaxResourceQuota { get; }

            public string CurrentResourceQuotaUsage { get; }

            public double RequestCharge { get; }

            public string ActivityId { get; }

            public string ResponseContinuation { get; }

            public string SessionToken { get; }

            public string ContentLocation { get; }

            public NameValueCollection ResponseHeaders { get; }

            public IEnumerator<TValue> GetEnumerator()
            {
                throw new NotImplementedException();
            }
        }
    }
}
