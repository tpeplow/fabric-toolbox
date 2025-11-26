namespace PathResolution.Models;

public sealed class ResolvedPath
{
    public required string Path { get; init; }
    public required string FileSystem { get; init; }
    public required string Endpoint { get; init; }
    public required string CredentialType { get; init; }
    public required IReadOnlyList<OneLakeCredential> Credentials { get; init; }
}
