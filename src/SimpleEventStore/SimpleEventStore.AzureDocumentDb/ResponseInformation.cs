using System.Collections.Specialized;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;

namespace SimpleEventStore.AzureDocumentDb
{
    public class ResponseInformation
    {
        public string RequestIdentifier { get; private set; }

        public string CurrentResourceQuotaUsage { get; private set; }

        public string MaxResourceQuota { get; private set; }

        public double RequestCharge { get; private set; }

        public NameValueCollection ResponseHeaders { get; private set; }

        public static ResponseInformation FromWriteResponse(string requestIdentifier, IStoredProcedureResponse<dynamic> response)
        {
            return new ResponseInformation
            {
                RequestIdentifier = requestIdentifier,
                CurrentResourceQuotaUsage = response.CurrentResourceQuotaUsage,
                MaxResourceQuota = response.MaxResourceQuota,
                RequestCharge = response.RequestCharge,
                ResponseHeaders = response.ResponseHeaders
            };
        }

        public static ResponseInformation FromReadResponse(string requestIdentifier, IFeedResponse<DocumentDbStorageEvent> response)
        {
            return new ResponseInformation
            {
                RequestIdentifier = requestIdentifier,
                CurrentResourceQuotaUsage = response.CurrentResourceQuotaUsage,
                MaxResourceQuota = response.MaxResourceQuota,
                RequestCharge = response.RequestCharge,
                ResponseHeaders = response.ResponseHeaders
            };
        }

        public static ResponseInformation FromSubscriptionReadResponse(string requestIdentifier, IFeedResponse<Document> response)
        {
            return new ResponseInformation
            {
                RequestIdentifier = requestIdentifier,
                CurrentResourceQuotaUsage = response.CurrentResourceQuotaUsage,
                MaxResourceQuota = response.MaxResourceQuota,
                RequestCharge = response.RequestCharge,
                ResponseHeaders = response.ResponseHeaders
            };
        }
    }
}