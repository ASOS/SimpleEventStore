using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Net;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Scripts;
using NUnit.Framework;

namespace SimpleEventStore.CosmosDb.Tests
{
    [TestFixture]
    public class ResponseInformationBuilding
    {
        [Test]
        public void when_building_from_a_write_response_all_target_fields_are_mapped()
        {
            var result = ResponseInformation.FromWriteResponse(Expected.RequestIdentifier, new FakeStoredProcedureResponse<dynamic>());

            Assert.That(result.RequestIdentifier, Is.EqualTo(Expected.RequestIdentifier));
            Assert.That(result.RequestCharge, Is.EqualTo(Expected.RequestCharge));
            Assert.That(result.ResponseHeaders, Is.EqualTo(Expected.ResponseHeaders));
        }

        [Test]
        public void when_building_from_a_read_response_all_target_fields_are_mapped()
        {
            var result = ResponseInformation.FromReadResponse(Expected.RequestIdentifier, new FakeFeedResponse<CosmosDbStorageEvent>());

            Assert.That(result.RequestIdentifier, Is.EqualTo(Expected.RequestIdentifier));
            Assert.That(result.CurrentResourceQuotaUsage, Is.EqualTo(Expected.CurrentResourceQuotaUsage));
            Assert.That(result.MaxResourceQuota, Is.EqualTo(Expected.MaxResourceQuota));
            Assert.That(result.RequestCharge, Is.EqualTo(Expected.RequestCharge));
            Assert.That(result.ResponseHeaders, Is.EqualTo(Expected.ResponseHeaders));
        }

        [Test]
        public void when_building_from_a_subscription_read_response_all_target_fields_are_mapped()
        {
            var result = ResponseInformation.FromSubscriptionReadResponse(Expected.RequestIdentifier, new FakeFeedResponse<CosmosDbStorageEvent>());

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

            static Expected()
            {
                ResponseHeaders.Add("Location", "");
                ResponseHeaders.Add("Session", "");
                ResponseHeaders.Add("RequestCharge", "0");
                ResponseHeaders.Add("ActivityId", "");
                ResponseHeaders.Add("ContentLength", "");
                ResponseHeaders.Add("ContentType", "");
                ResponseHeaders.Add("ContinuationToken", "");
                ResponseHeaders.Add("ETag", "");
            }
        }

        private class FakeStoredProcedureResponse<TValue> : StoredProcedureExecuteResponse<TValue>
        {
            internal FakeStoredProcedureResponse()
            {
                RequestCharge = Expected.RequestCharge;
                ResponseHeaders = Expected.ResponseHeaders;
            }

            public override string ActivityId { get; }

            public override double RequestCharge { get; }

            public TValue Response { get; }

            public NameValueCollection ResponseHeaders { get; }

            public override string SessionToken { get; }

            public override string ScriptLog { get; }

            public override HttpStatusCode StatusCode { get; }
        }

        private class FakeFeedResponse<TValue> : FeedResponse<TValue>
        {
            internal FakeFeedResponse()
            {
                RequestCharge = Expected.RequestCharge;
                ResponseHeaders = Expected.ResponseHeaders;
                Headers = new Headers
                {
                    {"x-ms-resource-quota", Expected.MaxResourceQuota},
                    {"x-ms-resource-usage", Expected.CurrentResourceQuotaUsage}
                };
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

            public override string ContinuationToken { get; }
            public override int Count { get; }

            public override Headers Headers { get; }
            public override IEnumerable<TValue> Resource { get; }
            public override HttpStatusCode StatusCode { get; }
            public override double RequestCharge { get; }

            public override string ActivityId { get; }
            public override CosmosDiagnostics Diagnostics { get; }

            public string ResponseContinuation { get; }

            public string SessionToken { get; }

            public string ContentLocation { get; }

            public NameValueCollection ResponseHeaders { get; }

            public override string IndexMetrics => string.Empty;

            public override IEnumerator<TValue> GetEnumerator()
            {
                yield return Activator.CreateInstance<TValue>();
            }
        }
    }
}
