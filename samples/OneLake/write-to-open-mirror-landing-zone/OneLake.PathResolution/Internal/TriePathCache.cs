using System.Collections.Concurrent;
using PathResolution.Models;
using PathResolution.Abstractions;

namespace PathResolution.Internal;

internal sealed class TriePathCache : IPathCache
{
    private readonly ConcurrentDictionary<string, SecurityContextCache> _contextCaches = new(StringComparer.OrdinalIgnoreCase);
    private readonly SemaphoreSlim _backgroundLimiter;
    private readonly int _maxConcurrentBackground;

    public TriePathCache(int maxConcurrentBackground)
    {
        _maxConcurrentBackground = maxConcurrentBackground;
        _backgroundLimiter = new SemaphoreSlim(maxConcurrentBackground);
    }

    public PathCacheResult? TryGet(string contextKey, string normalizedPath)
    {
        if (!_contextCaches.TryGetValue(contextKey, out var cache)) return null;
        var now = DateTimeOffset.UtcNow;
        var (bestResolved, bestProxy) = GetBestNodesInternal(cache.Root, normalizedPath, new HashSet<string>(StringComparer.OrdinalIgnoreCase){"dfs","blob"});
        if (bestResolved == null || bestResolved.EarliestExpiry <= now) return null;
        return new PathCacheResult { FromCache = true, ProxyNode = bestProxy, ResolvedNode = bestResolved, CachedResolvedPath = bestResolved.Resolved?.Path };
    }

    public async Task<PathCacheResult> GetOrAddAsync(string contextKey, string normalizedPath, Func<Task<PathResolutionResponse>> valueFactory, TimeSpan refreshSkew, CancellationToken cancellationToken)
    {
        var cache = _contextCaches.GetOrAdd(contextKey, _ => new SecurityContextCache());
        var node = GetOrCreateNode(cache.Root, normalizedPath);
        var now = DateTimeOffset.UtcNow;

        // First attempt longest-prefix cache hit (ancestor or exact)
        var (bestResolved, bestProxy) = GetBestNodesInternal(cache.Root, normalizedPath, new HashSet<string>(StringComparer.OrdinalIgnoreCase){"dfs","blob"});
        if (bestResolved != null && bestResolved.EarliestExpiry > now)
        {
            var remaining = bestResolved.EarliestExpiry - now;
            if (remaining <= refreshSkew)
            {
                // Refresh resolved ancestor (not the child path) in background
                QueueBackgroundRefresh(contextKey, bestResolved.Resolved!.Path, bestResolved, valueFactory, refreshSkew);
            }
            return new PathCacheResult { FromCache = true, ProxyNode = bestProxy, ResolvedNode = bestResolved, CachedResolvedPath = bestResolved.Resolved?.Path };
        }

        // No valid ancestor; acquire gate on target node (to prevent duplicate fetches)
        await node.Gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            // Re-evaluate after acquiring gate (could have been populated meanwhile)
            var (br2, bp2) = GetBestNodesInternal(cache.Root, normalizedPath, new HashSet<string>(StringComparer.OrdinalIgnoreCase){"dfs","blob"});
            if (br2 != null && br2.EarliestExpiry > DateTimeOffset.UtcNow)
            {
                var remaining2 = br2.EarliestExpiry - DateTimeOffset.UtcNow;
                if (remaining2 <= refreshSkew)
                    QueueBackgroundRefresh(contextKey, br2.Resolved!.Path, br2, valueFactory, refreshSkew);
                return new PathCacheResult { FromCache = true, ProxyNode = bp2, ResolvedNode = br2, CachedResolvedPath = br2.Resolved?.Path };
            }

            // Fetch and index
            var model = await valueFactory().ConfigureAwait(false);
            IndexAllPaths(cache.Root, model);
            var (br3, bp3) = GetBestNodesInternal(cache.Root, normalizedPath, new HashSet<string>(StringComparer.OrdinalIgnoreCase){"dfs","blob"});
            return new PathCacheResult { FromCache = false, ProxyNode = bp3, ResolvedNode = br3, CachedResolvedPath = br3?.Resolved?.Path };
        }
        finally { node.Gate.Release(); }
    }

    public void MaybeRefreshInBackground(string contextKey, string normalizedPath, Func<Task<PathResolutionResponse>> valueFactory, TimeSpan refreshSkew, TimeSpan timeout)
    {
        if (!_contextCaches.TryGetValue(contextKey, out var cache)) return;
        var now = DateTimeOffset.UtcNow;
        var (bestResolved, _) = GetBestNodesInternal(cache.Root, normalizedPath, new HashSet<string>(StringComparer.OrdinalIgnoreCase){"dfs","blob"});
        if (bestResolved == null || bestResolved.Resolved == null) return;
        var remaining = bestResolved.EarliestExpiry - now;
        if (remaining > refreshSkew) return;
        QueueBackgroundRefresh(contextKey, bestResolved.Resolved.Path, bestResolved, valueFactory, refreshSkew, timeout);
    }

    public IEnumerable<CachedResolvedPath> EnumerateAllResolved()
    {
        foreach (var kvp in _contextCaches)
        {
            var root = kvp.Value.Root;
            foreach (var item in EnumerateNode(root, prefix: string.Empty))
                yield return item;
        }
    }

    private IEnumerable<CachedResolvedPath> EnumerateNode(TrieNode node, string prefix)
    {
        if (node.Resolved != null)
        {
            yield return new CachedResolvedPath
            {
                Path = node.Resolved.Path,
                Endpoint = node.Resolved.Endpoint,
                EarliestExpiry = node.EarliestExpiry,
                HasCredential = node.Resolved.Credentials.Any(c => !string.IsNullOrEmpty(c.Secret))
            };
        }
        foreach (var child in node.Children)
        {
            foreach (var item in EnumerateNode(child.Value, prefix))
                yield return item;
        }
    }

    internal (TrieNode? bestResolved, TrieNode? bestProxy) GetBestNodes(string contextKey, string normalizedPath, ISet<string> supportedFileSystems)
    {
        if (!_contextCaches.TryGetValue(contextKey, out var cache)) return (null, null);
        return GetBestNodesInternal(cache.Root, normalizedPath, supportedFileSystems);
    }

    private (TrieNode? bestResolved, TrieNode? bestProxy) GetBestNodesInternal(TrieNode root, string normalizedPath, ISet<string> supportedFileSystems)
    {
        var segments = Split(normalizedPath);
        TrieNode current = root;
        TrieNode? bestResolved = null;
        TrieNode? bestProxy = null;
        int depth = 0;
        foreach (var seg in segments)
        {
            if (!current.Children.TryGetValue(seg, out var next)) break;
            current = next;
            depth++;
            if (current.Proxy != null && supportedFileSystems.Contains(current.Proxy.FileSystem))
                bestProxy = current;
            if (current.Resolved != null && supportedFileSystems.Contains(current.Resolved.FileSystem))
            {
                int suffixSegments = segments.Length - depth;
                // Allow ancestor resolved for any deeper path (longest prefix) by relaxing suffixSegments condition.
                bestResolved = current;
                // Keep going to possibly find deeper resolved node; deeper overrides previous.
            }
        }
        return (bestResolved, bestProxy);
    }

    private static string[] Split(string path) => path.Split('/', StringSplitOptions.RemoveEmptyEntries);

    private static TrieNode GetOrCreateNode(TrieNode root, string path)
    {
        var segments = Split(path.Trim().TrimStart('/'));
        TrieNode current = root;
        foreach (var seg in segments)
            current = current.Children.GetOrAdd(seg, _ => new TrieNode());
        return current;
    }

    private void IndexAllPaths(TrieNode root, PathResolutionResponse model)
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        static DateTimeOffset ComputeMinExpiry(ResolvedPath r, DateTimeOffset now)
        {
            DateTimeOffset min = DateTimeOffset.MaxValue;
            foreach (var c in r.Credentials)
            {
                if (c.ExpiresAt != default && c.ExpiresAt > now && c.ExpiresAt < min)
                    min = c.ExpiresAt;
            }
            return min == DateTimeOffset.MaxValue ? now + TimeSpan.FromMinutes(5) : (min > now + TimeSpan.FromMinutes(5) ? now + TimeSpan.FromMinutes(5) : min);
        }

        foreach (var p in model.OneLakePaths)
        {
            var node = GetOrCreateNode(root, p.Path);
            node.Proxy = p;
        }
        foreach (var r in model.ResolvedPaths)
        {
            var node = GetOrCreateNode(root, r.Path);
            node.Resolved = r;
            node.EarliestExpiry = ComputeMinExpiry(r, now);
        }
    }

    private void QueueBackgroundRefresh(string contextKey, string path, TrieNode node, Func<Task<PathResolutionResponse>> valueFactory, TimeSpan refreshSkew, TimeSpan? timeout = null)
    {
        if (node.RefreshInProgress) return;
        if (!node.Gate.Wait(0)) return;
        node.RefreshInProgress = true;
        _ = Task.Run(async () =>
        {
            try
            {
                if (!await _backgroundLimiter.WaitAsync(TimeSpan.Zero).ConfigureAwait(false)) return;
                using var cts = timeout.HasValue ? new CancellationTokenSource(timeout.Value) : new CancellationTokenSource(TimeSpan.FromSeconds(10));
                var cache = _contextCaches[contextKey];
                var model = await valueFactory().ConfigureAwait(false);
                IndexAllPaths(cache.Root, model);
            }
            catch { }
            finally
            {
                node.RefreshInProgress = false;
                node.Gate.Release();
                _backgroundLimiter.Release();
            }
        });
    }
}
