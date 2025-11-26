using System.Collections.Concurrent;
using PathResolution.Models;

namespace PathResolution.Internal;

internal sealed class TrieNode
{
    public ConcurrentDictionary<string, TrieNode> Children { get; } = new(StringComparer.OrdinalIgnoreCase);
    public OneLakePath? Proxy;          // proxy path info
    public ResolvedPath? Resolved;      // resolved path info (credentials & endpoint)
    public DateTimeOffset EarliestExpiry; // expiry relevant when Resolved present
    public bool RefreshInProgress;        // guard against multiple background refreshes
    public SemaphoreSlim Gate { get; } = new(1,1); // sync refresh gate for this node
}
