namespace PathResolution.Internal;

internal sealed class SecurityContextCache
{
    public TrieNode Root { get; } = new();
}
