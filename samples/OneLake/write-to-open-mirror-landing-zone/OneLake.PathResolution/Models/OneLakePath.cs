namespace PathResolution.Models;

public sealed class OneLakePath
{
    public required string Path { get; init; }
    public required string FileSystem { get; init; }
    public required string Endpoint { get; init; }
}
