using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;

namespace SimpleEventStore.AzureDocumentDb
{
    static class Resources
    {
        public static Stream GetStream(string resourceName)
        {
            resourceName = $"{typeof(Resources).FullName}.{resourceName}";
            return typeof(Resources).GetTypeInfo().Assembly.GetManifestResourceStream(resourceName);
        }

        public static IEnumerable<string> GetStreamNames()
        {
            var prefix = typeof(Resources).FullName + ".";
            var results = typeof(Resources).GetTypeInfo().Assembly.GetManifestResourceNames().Select(n => n.Substring(prefix.Length));
            return results;
        }

        public static string GetString(string resourceName)
        {
            using (var reader = new StreamReader(GetStream(resourceName)))
            {
                return reader.ReadToEnd();
            }
        }
    }
}