using System.Net.Http.Headers;
using System.Text.Json;
using PathResolution.Configuration;
using PathResolution.Internal;
using PathResolution.Models;
using PathResolution.Abstractions;

namespace PathResolution.Core;

public sealed class PathResolutionClient : IPathResolutionClient
{
    private readonly HttpClient _http;
    private readonly PathResolutionClientOptions _options;
    private readonly ISecurityContextProvider _securityContextProvider;
    private readonly IPathCache _cache;

    private static readonly JsonSerializerOptions _jsonOptions = new(JsonSerializerDefaults.Web);

    public PathResolutionClient(HttpClient httpClient, PathResolutionClientOptions? options = null,
                         ISecurityContextProvider? securityContextProvider = null,
                         IPathCache? pathCache = null)
    {
        _http = httpClient;
        _options = options ?? new PathResolutionClientOptions();
        _http.BaseAddress ??= _options.BaseAddress;
        _securityContextProvider = securityContextProvider ?? new SecurityContextProvider(_http);
        _cache = pathCache ?? new TriePathCache(_options.MaxConcurrentBackgroundRefreshes);
    }

    public async Task<PathRewriteOptions> ResolvePathsAsync(string workspaceIdOrName, string itemIdOrName, string path, string? itemType = null, string? supportedStorageTypes = null, CancellationToken cancellationToken = default)
    {
        if (Guid.TryParse(workspaceIdOrName, out var wsId) && Guid.TryParse(itemIdOrName, out var itId))
        {
            return await ResolvePathsByIdsAsync(wsId, itId, path, supportedStorageTypes, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            if (string.IsNullOrWhiteSpace(itemType))
                throw new ArgumentException("Item type required when using names.", nameof(itemType));
            return await ResolvePathsByNamesAsync(workspaceIdOrName, itemIdOrName, itemType!, path, supportedStorageTypes, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task<PathRewriteOptions> ResolvePathsByNamesAsync(string workspace, string item, string itemType, string path, string? supportedStorageTypes = null, CancellationToken cancellationToken = default)
    {
        ValidateNames(workspace, item, itemType);
        path = NormalizePath(path ?? string.Empty);
        var contextKey = _securityContextProvider.GetContextKey();
        var supported = ParseSupported(supportedStorageTypes);

        var cacheResult = await _cache.GetOrAddAsync(
            contextKey,
            path!,
            () => {
                string buildRequestUri() => BuildRequestUriNames(workspace, item, itemType, path!, supportedStorageTypes);
                return FetchAsync(buildUri: buildRequestUri, cancellationToken);
            },
            _options.RefreshSkew,
            cancellationToken).ConfigureAwait(false);

        var rewriteOptions = BuildRewriteOptions((TriePathCache)_cache, contextKey, path!, supported, cacheResult);

        string? matchedLogical = (cacheResult.ResolvedNode is TrieNode rn) ? rn.Resolved?.Path : null;
        Console.WriteLine($"[PathResolutionClient] Cache {(cacheResult.FromCache ? "HIT" : "MISS")} context='{contextKey}' requestPath='{path}' matchedLogical='{matchedLogical ?? "<none>"}' resolvedEndpointPath='{rewriteOptions.Resolved?.Path ?? "<none>"}' proxyEndpointPath='{rewriteOptions.Proxy.Path}'");

        return rewriteOptions;
    }

    private async Task<PathRewriteOptions> ResolvePathsByIdsAsync(Guid workspaceId, Guid itemId, string path, string? supportedStorageTypes = null, CancellationToken cancellationToken = default)
    {
        path = NormalizePath(path ?? string.Empty);
        var contextKey = _securityContextProvider.GetContextKey();
        var supported = ParseSupported(supportedStorageTypes);

        var cacheResult = await _cache.GetOrAddAsync(
            contextKey,
            path!,
            () => {
                string buildRequestUri() => BuildRequestUriIds(workspaceId, itemId, path!, supportedStorageTypes);
                return FetchAsync(buildUri: buildRequestUri, cancellationToken);
            },
            _options.RefreshSkew,
            cancellationToken).ConfigureAwait(false);

        var rewriteOptions = BuildRewriteOptions((TriePathCache)_cache, contextKey, path!, supported, cacheResult);

        string? matchedLogical = (cacheResult.ResolvedNode is TrieNode rn) ? rn.Resolved?.Path : null;
        Console.WriteLine($"[PathResolutionClient] Cache {(cacheResult.FromCache ? "HIT" : "MISS")} context='{contextKey}' requestPath='{path}' matchedLogical='{matchedLogical ?? "<none>"}' resolvedEndpointPath='{rewriteOptions.Resolved?.Path ?? "<none>"}' proxyEndpointPath='{rewriteOptions.Proxy.Path}'");

        return rewriteOptions;
    }

    private static HashSet<string> ParseSupported(string? supported)
    {
        if (string.IsNullOrWhiteSpace(supported)) return new HashSet<string>(StringComparer.OrdinalIgnoreCase){ "dfs" };
        var parts = supported.Split(new[]{',',';',' '}, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        return parts.Length == 0 ? new HashSet<string>(StringComparer.OrdinalIgnoreCase){ "dfs" } : new HashSet<string>(parts, StringComparer.OrdinalIgnoreCase);
    }

    private PathRewriteOptions BuildRewriteOptions(TriePathCache trie, string originalContext, string originalPath, HashSet<string> supported, PathCacheResult cacheResult)
    {
        var (bestResolvedNode, bestProxyNode) = trie.GetBestNodes(originalContext, originalPath, supported);
        string? resolvedRaw = bestResolvedNode != null ? BuildFromResolved(bestResolvedNode.Resolved!, originalPath) : null;
        string proxyRaw = bestProxyNode != null ? BuildProxyFromNode(bestProxyNode.Proxy!, originalPath) : (resolvedRaw ?? originalPath);
        if (resolvedRaw != null && string.Equals(resolvedRaw, proxyRaw, StringComparison.OrdinalIgnoreCase)) resolvedRaw = null;

        // Append SAS to proxy if configured and absent
        if (!string.IsNullOrEmpty(_options.SasToken) && proxyRaw.IndexOf("sig=", StringComparison.OrdinalIgnoreCase) < 0)
        {
            proxyRaw += (proxyRaw.Contains('?') ? '&' : '?') + _options.SasToken.TrimStart('?');
        }

        (string pathOnly, string? cred, PathCredentialType type) Split(string raw)
        {
            if (string.IsNullOrEmpty(raw)) return (raw, null, PathCredentialType.Default);
            try
            {
                var uri = new Uri(raw);
                var q = uri.Query.TrimStart('?');
                var pathOnly = new UriBuilder(uri) { Query = string.Empty }.Uri.ToString().TrimEnd('?');
                if (string.IsNullOrEmpty(q)) return (pathOnly, null, PathCredentialType.Default);
                var type = q.IndexOf("sig=", StringComparison.OrdinalIgnoreCase) >= 0 ? PathCredentialType.Sas : PathCredentialType.Default;
                return (pathOnly, type == PathCredentialType.Default ? null : q, type);
            }
            catch { return (raw, null, PathCredentialType.Default); }
        }

        var proxySplit = Split(proxyRaw);
        var resolvedSplit = resolvedRaw != null ? Split(resolvedRaw) : (null, null, PathCredentialType.Default);

        var proxyInfo = new PathInfo
        {
            Path = proxySplit.pathOnly,
            CredentialValue = proxySplit.cred,
            CredentialType = proxySplit.type,
            IsResolved = false
        };

        PathInfo? resolvedInfo = resolvedSplit.pathOnly == null ? null : new PathInfo
        {
            Path = resolvedSplit.pathOnly!,
            CredentialValue = resolvedSplit.cred,
            CredentialType = resolvedSplit.type,
            IsResolved = true
        };

        return new PathRewriteOptions { Proxy = proxyInfo, Resolved = resolvedInfo };
    }

    private static string BuildFromResolved(ResolvedPath resolvedNode, string originalPath)
    {
        var basePath = resolvedNode.Path.Trim().TrimStart('/');
        var logical = originalPath.Trim().TrimStart('/');
        var suffix = logical.Length > basePath.Length ? logical[basePath.Length..].TrimStart('/') : string.Empty;
        var endpoint = NormalizeEndpointForDataLake(resolvedNode.Endpoint).TrimEnd('/');
        var rewritten = string.IsNullOrEmpty(suffix) ? endpoint : endpoint + "/" + suffix;
        var cred = resolvedNode.Credentials.FirstOrDefault();
        if (cred != null && !string.IsNullOrEmpty(cred.Secret))
            rewritten += (rewritten.Contains('?') ? '&' : '?') + cred.Secret;
        return rewritten;
    }

    private static string BuildProxyFromNode(OneLakePath proxyNode, string originalPath)
    {
        var basePath = proxyNode.Path.Trim().TrimStart('/');
        var logical = originalPath.Trim().TrimStart('/');
        var suffix = logical.StartsWith(basePath, StringComparison.OrdinalIgnoreCase) ? logical[basePath.Length..].TrimStart('/') : logical;
        return proxyNode.Endpoint.TrimEnd('/') + (suffix.Length > 0 ? "/" + suffix : string.Empty);
    }

    private async Task<PathResolutionResponse> FetchAsync(Func<string> buildUri, CancellationToken ct)
    {
        var requestUri = buildUri();
        if (!string.IsNullOrEmpty(_options.SasToken))
        {
            var sas = _options.SasToken.TrimStart('?');
            if (!requestUri.Contains(sas, StringComparison.Ordinal))
                requestUri += (requestUri.Contains('?') ? "&" : "?") + sas;
        }

        using var request = new HttpRequestMessage(HttpMethod.Get, requestUri);
        var auth = _http.DefaultRequestHeaders.Authorization;
        if (auth is not null && request.Headers.Authorization is null)
            request.Headers.Authorization = auth;
        if (!string.IsNullOrEmpty(_options.ApiVersion))
            request.Headers.TryAddWithoutValidation("x-ms-version", _options.ApiVersion);
        request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

        try
        {
            Console.WriteLine($"[PathResolutionClient] GET {requestUri}");
            var response = await _http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, ct).ConfigureAwait(false);
            if (!response.IsSuccessStatusCode)
            {
                string errBody = string.Empty;
                try { errBody = await response.Content.ReadAsStringAsync(ct).ConfigureAwait(false); } catch { }
                Console.WriteLine($"[PathResolutionClient] ERROR {(int)response.StatusCode} {response.ReasonPhrase} Body: {errBody}");
                throw new HttpRequestException($"ResolvePaths failed {(int)response.StatusCode} {response.ReasonPhrase} for '{requestUri}'. Body: {Truncate(errBody, 512)}");
            }
            
            var responseContent = string.Empty;
            using (StreamReader reader = new(await response.Content.ReadAsStreamAsync(ct)))
            {
                responseContent = await reader.ReadToEndAsync(ct);
            }

            if (string.IsNullOrEmpty(responseContent))
            {
                Console.WriteLine($"[PathResolutionClient] ERROR Empty response body for {requestUri}");
                throw new HttpRequestException($"ResolvePaths returned empty body for '{requestUri}'.");
            }

            return JsonSerializer.Deserialize<PathResolutionResponse>(responseContent, _jsonOptions)
                   ?? throw new JsonException($"Failed to deserialize PathResolutionResponse for '{requestUri}'.");
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            Console.WriteLine($"[PathResolutionClient] CANCEL {requestUri}");
            throw;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[PathResolutionClient] EXCEPTION {ex.GetType().Name}: {ex.Message} for {requestUri}");
            throw;
        }
    }

    private static string NormalizeEndpointForDataLake(string endpoint)
    {
        if (string.IsNullOrWhiteSpace(endpoint)) return endpoint;
        try
        {
            var uri = new Uri(endpoint);
            if (uri.Host.Contains(".dfs.", StringComparison.OrdinalIgnoreCase)) return endpoint;
            if (uri.Host.Contains(".blob.", StringComparison.OrdinalIgnoreCase))
            {
                var dfsHost = uri.Host.Replace(".blob.", ".dfs.", StringComparison.OrdinalIgnoreCase);
                return new UriBuilder(uri.Scheme, dfsHost, uri.Port, uri.AbsolutePath, uri.Query) { }.Uri.ToString();
            }
        }
        catch { }
        return endpoint;
    }

    private static string Truncate(string value, int max)
        => string.IsNullOrEmpty(value) || value.Length <= max ? value : value[..max];

    private static void ValidateNames(string workspace, string item, string itemType)
    {
        if (string.IsNullOrWhiteSpace(workspace)) throw new ArgumentException("Workspace required", nameof(workspace));
        if (string.IsNullOrWhiteSpace(item)) throw new ArgumentException("Item required", nameof(item));
        if (string.IsNullOrWhiteSpace(itemType)) throw new ArgumentException("Item type required", nameof(itemType));
    }

    private static string NormalizePath(string path) => path.Trim().TrimStart('/');

    private static string BuildRequestUriNames(string workspace, string item, string itemType, string path, string? supportedStorageTypes)
    {
        var baseSegment = Uri.EscapeDataString(workspace) + "/" + Uri.EscapeDataString(item + "." + itemType) + "/" + path;
        return AppendCommonQuery(baseSegment, supportedStorageTypes);
    }

    private static string BuildRequestUriIds(Guid workspaceId, Guid itemId, string path, string? supportedStorageTypes)
    {
        var baseSegment = workspaceId + "/" + itemId + "/" + path;
        return AppendCommonQuery(baseSegment, supportedStorageTypes);
    }

    private static string AppendCommonQuery(string baseSegment, string? supportedStorageTypes)
    {
        var uri = baseSegment + "?oneLakeAction=ResolvePaths";
        if (!string.IsNullOrWhiteSpace(supportedStorageTypes))
            uri += "&olSupportedStorageTypes=" + Uri.EscapeDataString(supportedStorageTypes);
        return uri;
    }

    public void PrintCachedResolvedPaths()
    {
        var all = _cache.EnumerateAllResolved();
        Console.WriteLine("\n=== Cached Resolved Paths ===");
        int count = 0;
        foreach (var r in all)
        {
            count++;
            Console.WriteLine($"[{count}] path='{r.Path}' endpoint='{r.Endpoint}' expiry='{r.EarliestExpiry:u}' hasCredential={r.HasCredential}");
        }
        if (count == 0) Console.WriteLine("(none)");
    }
}
