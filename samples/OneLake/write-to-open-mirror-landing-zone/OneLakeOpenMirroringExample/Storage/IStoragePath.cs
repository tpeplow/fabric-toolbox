using Azure.Storage.Blobs.Models;

namespace OneLakeOpenMirroringExample.Storage;

public interface IStoragePath
{
    Task<BlobFile> CreateFileAsync(string path);
    Task<Stream> OpenRead(BlobItem path);
    IStoragePath GetChildPath(string path);
    IAsyncEnumerable<BlobItem?> GetBlobsAsync(string? prefix = null);
    Task DeleteIfExistsAsync();
    Task RenameAsync(string newName);
    Task<bool> ExistsAsync();
}