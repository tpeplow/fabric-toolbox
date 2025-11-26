namespace PathResolution.Models;

public enum PathCredentialType
{
    Default = 0,
    Sas = 1
}

public sealed class PathInfo
{
    public required string Path { get; init; }               // Absolute endpoint path (no credential query)
    public string? CredentialValue { get; init; }             // Raw credential data (e.g. SAS query without leading '?')
    public PathCredentialType CredentialType { get; init; }   // Type of credential
    public bool IsResolved { get; init; }                     // True if this is a resolved direct storage endpoint

    public bool HasCredential => CredentialType != PathCredentialType.Default && !string.IsNullOrEmpty(CredentialValue);
    public string AbsoluteUri => HasCredential ? Path + (Path.Contains('?') ? '&' : '?') + CredentialValue : Path;
}

public sealed class PathRewriteOptions
{
    public required PathInfo Proxy { get; init; }     // Always present logical/proxy endpoint
    public PathInfo? Resolved { get; init; }          // Optional resolved direct endpoint
}