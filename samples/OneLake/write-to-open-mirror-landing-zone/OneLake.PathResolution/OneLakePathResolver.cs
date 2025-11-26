using Azure.Core;
using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.Storage.Files.DataLake;
using PathResolution.Configuration;
using PathResolution.Core;

namespace OneLake.PathResolution
{
    public static class OneLakePathResolver
    {
        public async static Task InstallPathResolverAsync(this DataLakeClientOptions options, Uri oneLakeUri)
        {
            var policy = await CreatePathResolutionPolicy(oneLakeUri);
            options.AddPolicy(policy, HttpPipelinePosition.PerRetry);
        }

        public async static Task InstallPathResolverAsync (this BlobClientOptions options, Uri oneLakeUri)
        {
            var policy = await CreatePathResolutionPolicy(oneLakeUri);
            options.AddPolicy(policy, HttpPipelinePosition.PerRetry);
        }

        static async Task<PathResolutionPolicy> CreatePathResolutionPolicy(Uri uri)
        {
            var http = new HttpClient { BaseAddress = uri };

            // todo - fix this so the client gets the token dynamically (this will eventually break when the credential expires)
            http.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", await GetDefaultTokenAsync());
            
            var options = new PathResolutionClientOptions { ApiVersion = "2024-01-01" };
            var pathClient = new PathResolutionClient(http, options);
            return new PathResolutionPolicy(pathClient, options);
        }

        static async Task<string> GetDefaultTokenAsync()
        {
            var creds = new DefaultAzureCredential();
            var token = await creds.GetTokenAsync(
                new Azure.Core.TokenRequestContext(
                    ["https://storage.azure.com/.default"]
                )
            );
            return token.Token;
        }
    }
}
