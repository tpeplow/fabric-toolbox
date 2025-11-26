using PathResolution.Models;

namespace PathResolution.Abstractions;

public interface IPathResolutionClient
{
    // Returns possible rewrite targets: always proxy path plus optional resolved direct path.
    Task<PathRewriteOptions> ResolvePathsAsync(string workspaceIdOrName, string itemIdOrName, string path, string? itemType = null, string? supportedStorageTypes = null, CancellationToken cancellationToken = default);
}
