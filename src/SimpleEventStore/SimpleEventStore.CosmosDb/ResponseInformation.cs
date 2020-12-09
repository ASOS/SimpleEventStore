using System.Collections.Specialized;
using System.Globalization;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Scripts;

namespace SimpleEventStore.CosmosDb
{
    public class ResponseInformation
    {
        public string RequestIdentifier { get; private set; }

        public string CurrentResourceQuotaUsage { get; private set; }

        public string MaxResourceQuota { get; private set; }

        public double RequestCharge { get; private set; }

        public NameValueCollection ResponseHeaders { get; private set; }

        public static ResponseInformation FromWriteResponse(string requestIdentifier, StoredProcedureExecuteResponse<dynamic> response)
        {
            return new ResponseInformation
            {
                RequestIdentifier = requestIdentifier,
                CurrentResourceQuotaUsage = GetCurrentResourceQuotaUsage(response),
                MaxResourceQuota = GetMaxResourceQuota(response),
                RequestCharge = response.RequestCharge,
                ResponseHeaders = HeaderToNamedValueCollection(response.Headers)
            };
        }

        public static ResponseInformation FromWriteResponse(string requestIdentifier, TransactionalBatchResponse response)
        {
            return new ResponseInformation
            {
                RequestIdentifier = requestIdentifier,
                CurrentResourceQuotaUsage = GetCurrentResourceQuotaUsage(response),
                RequestCharge = response.RequestCharge
            };
        }

        private static string GetCurrentResourceQuotaUsage(TransactionalBatchResponse response)
        {
            return response.RequestCharge.ToString();
        }

        private static string GetCurrentResourceQuotaUsage<T>(Response<T> response)
        {
            return response.Headers?.GetValueOrDefault("x-ms-resource-usage");
        }
        
        private static string GetMaxResourceQuota<T>(Response<T> response)
        {
            return response.Headers?.GetValueOrDefault("x-ms-resource-quota");
        }

        public static ResponseInformation FromReadResponse(string requestIdentifier, FeedResponse<CosmosDbStorageEvent> response)
        {
            return new ResponseInformation
            {
                RequestIdentifier = requestIdentifier,
                CurrentResourceQuotaUsage = GetCurrentResourceQuotaUsage(response),
                MaxResourceQuota = GetMaxResourceQuota(response),
                RequestCharge = response.RequestCharge,
                ResponseHeaders = HeaderToNamedValueCollection(response.Headers)
            };
        }

        public static ResponseInformation FromSubscriptionReadResponse<T>(string requestIdentifier, Response<T> response)
        {
            return new ResponseInformation
            {
                RequestIdentifier = requestIdentifier,
                CurrentResourceQuotaUsage = GetCurrentResourceQuotaUsage(response),
                MaxResourceQuota = GetMaxResourceQuota(response),
                RequestCharge = response.RequestCharge,
                ResponseHeaders = HeaderToNamedValueCollection(response.Headers)
            };
        }

        private static NameValueCollection HeaderToNamedValueCollection(Headers headers)
        {
            return new NameValueCollection()
            {
                {"Location", headers?.Location},
                {"Session", headers?.Session},
                {"RequestCharge", headers?.RequestCharge.ToString(CultureInfo.InvariantCulture)},
                {"ActivityId", headers?.ActivityId},
                {"ContentLength", headers?.ContentLength},
                {"ContentType", headers?.ContentType},
                {"ContinuationToken", headers?.ContinuationToken},
                {"ETag", headers?.ETag}
            };
        }
    }
}