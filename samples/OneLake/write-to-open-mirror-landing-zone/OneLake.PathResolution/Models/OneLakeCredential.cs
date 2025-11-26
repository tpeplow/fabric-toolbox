namespace PathResolution.Models;

public sealed class OneLakeCredential
{
    public required string[] AllowedOperations { get; init; }
    public required string Secret { get; init; }
    public DateTimeOffset ExpiresAt { get; init; }
}
