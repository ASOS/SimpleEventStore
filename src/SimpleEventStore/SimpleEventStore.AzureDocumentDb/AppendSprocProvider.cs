using System.Linq;
using System.Security.Cryptography;
using System.Text;

namespace SimpleEventStore.AzureDocumentDb
{
    internal static class AppendSprocProvider
    {
        public static (string Name, string Body) GetAppendSprocData()
        {
            var body = Resources.GetString("appendToStream.js");
            var version = CalculateVersion(body);
            var name = "appendToStream-" + version;

            return (name, body);
        }

        private static string CalculateVersion(string body)
        {
            var bytes = Encoding.Unicode.GetBytes(body);

            using (var hashAlgorithm = new SHA1CryptoServiceProvider())
            {
                var hashBytes = hashAlgorithm.ComputeHash(bytes);

                var versionChars = bytes
                    .Take(4)
                    .Select(x => x.ToString("X2"));

                return string.Concat(versionChars);
            }
        }

    }
}
