using PathResolution.Models;

namespace PathResolution.Abstractions;

public interface IPathCache
{
    PathCacheResult? TryGet(string contextKey, string normalizedPath);
    Task<PathCacheResult> GetOrAddAsync(string contextKey, string normalizedPath, Func<Task<PathResolutionResponse>> valueFactory, TimeSpan refreshSkew, CancellationToken cancellationToken);
    void MaybeRefreshInBackground(string contextKey, string normalizedPath, Func<Task<PathResolutionResponse>> valueFactory, TimeSpan refreshSkew, TimeSpan timeout);
    IEnumerable<CachedResolvedPath> EnumerateAllResolved();
}
