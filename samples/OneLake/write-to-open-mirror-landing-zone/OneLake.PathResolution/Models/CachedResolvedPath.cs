namespace PathResolution.Models;

public sealed class CachedResolvedPath
{
    public required string Path { get; init; }
    public required string Endpoint { get; init; }
    public DateTimeOffset EarliestExpiry { get; init; }
    public bool HasCredential { get; init; }
}
