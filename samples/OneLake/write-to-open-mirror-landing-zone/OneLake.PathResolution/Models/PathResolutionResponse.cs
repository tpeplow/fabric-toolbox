using System.Text.Json.Serialization;

namespace PathResolution.Models;

public sealed class PathResolutionResponse
{
    [JsonPropertyName("resolutionResult")]
    public ResolutionResult ResolutionResult { get; init; }

    [JsonPropertyName("oneLakePaths")]
    public IReadOnlyList<OneLakePath> OneLakePaths { get; init; } = Array.Empty<OneLakePath>();
    
    [JsonPropertyName("resolvedPaths")]
    public IReadOnlyList<ResolvedPath> ResolvedPaths { get; init; } = Array.Empty<ResolvedPath>();
}
