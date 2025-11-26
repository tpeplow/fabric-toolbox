namespace PathResolution.Models;

public sealed class PathCacheResult
{
    public bool FromCache { get; init; }
    public bool HasResolved => ResolvedNode != null;
    // Raw nodes the client can use to build rewrite options
    public object? ProxyNode { get; init; } // internal TrieNode reference (typed as object to avoid exposing internal type)
    public object? ResolvedNode { get; init; }
    public string? CachedResolvedPath { get; init; } // Added for logging
}
