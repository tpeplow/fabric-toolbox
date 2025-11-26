using Azure.Core;
using Azure.Core.Pipeline;
using PathResolution.Abstractions;
using PathResolution.Models;
using PathResolution.Configuration;

namespace PathResolution.Core;

// Azure.Core pipeline policy to intercept OneLake proxy URIs and rewrite to resolved storage endpoints.
// Placed at PerRetry so Authorization header is present; removes it if SAS signature present.
internal sealed class PathResolutionPolicy : HttpPipelinePolicy
{
    private readonly IPathResolutionClient _client;
    private readonly PathResolutionClientOptions _options;

    public PathResolutionPolicy(IPathResolutionClient client, PathResolutionClientOptions? options = null)
    {
        _client = client;
        _options = options ?? new PathResolutionClientOptions();
    }

    public override void Process(HttpMessage message, ReadOnlyMemory<HttpPipelinePolicy> pipeline)
    {
        ProcessCoreAsync(message).GetAwaiter().GetResult();
        ProcessNext(message, pipeline);
    }

    public override async ValueTask ProcessAsync(HttpMessage message, ReadOnlyMemory<HttpPipelinePolicy> pipeline)
    {
        await ProcessCoreAsync(message).ConfigureAwait(false);
        await ProcessNextAsync(message, pipeline).ConfigureAwait(false);
    }

    private async Task ProcessCoreAsync(HttpMessage message)
    {
        var originalUri = message.Request.Uri.ToUri();
        if (!IsOneLakeProxyHost(originalUri)) return;
        if (!TryParseOneLakePath(originalUri, out var workspaceNameOrId, out var itemNameOrId, out var itemType, out var relativePath)) return;

        PathRewriteOptions? options;
        try
        {
            options = await _client.ResolvePathsAsync(workspaceNameOrId!, itemNameOrId!, relativePath!, itemType, cancellationToken: message.CancellationToken).ConfigureAwait(false);
        }
        catch { return; }
        if (options == null) return;

        var endpoint = options.Resolved ?? options.Proxy;
        var rewritten = endpoint.AbsoluteUri;
        if (string.IsNullOrEmpty(rewritten) || string.Equals(rewritten, originalUri.AbsoluteUri, StringComparison.OrdinalIgnoreCase)) return;

        try
        {
            message.Request.Uri = new RequestUriBuilder();
            message.Request.Uri.Reset(new Uri(rewritten));
        }
        catch { return; }

        if ((endpoint.HasCredential || !string.IsNullOrEmpty(_options.SasToken)) && message.Request.Headers.TryGetValue("Authorization", out _))
            message.Request.Headers.Remove("Authorization");
    }

    private static bool IsOneLakeProxyHost(Uri uri) => uri.Host.Contains("onelake", StringComparison.OrdinalIgnoreCase);

    private static bool TryParseOneLakePath(Uri uri, out string? workspaceNameOrId, out string? itemNameOrId, out string? itemType, out string? relativePath)
    {
        workspaceNameOrId = itemNameOrId = itemType = relativePath = null;
        var segments = uri.AbsolutePath.Trim('/').Split('/', StringSplitOptions.RemoveEmptyEntries);
        if (segments.Length < 2) return false;
        var first = segments[0];
        var second = segments[1];
        var dotIdx = second.LastIndexOf('.');
        var secondLooksGuid = Guid.TryParse(second, out _);
        var firstLooksGuid = Guid.TryParse(first, out _);
        if (firstLooksGuid && secondLooksGuid && dotIdx < 0)
        {
            workspaceNameOrId = first;
            itemNameOrId = second;
            relativePath = segments.Length > 2 ? string.Join('/', segments.Skip(2)) : string.Empty;
            return true;
        }
        if (dotIdx <= 0 || dotIdx == second.Length - 1) return false;
        workspaceNameOrId = first;
        itemNameOrId = second[..dotIdx];
        itemType = second[(dotIdx + 1)..];
        relativePath = segments.Length > 2 ? string.Join('/', segments.Skip(2)) : string.Empty;
        return true;
    }
}
