using Azure.Core;
using Azure.Core.Pipeline;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Files.DataLake;
using OneLake.PathResolution;
using System.Threading.Tasks;

namespace OneLakeOpenMirroringExample.Storage;

public class StorageClient
{
    private readonly BlobServiceClient blobServiceClient;
    private readonly DataLakeServiceClient? dataLakeServiceClient;

    public StorageClient(BlobServiceClient blobServiceClient)
    {
        this.blobServiceClient = blobServiceClient;
    }
    
    public StorageClient(BlobServiceClient blobServiceClient, DataLakeServiceClient dataLakeServiceClient)
    {
        this.blobServiceClient = blobServiceClient;
        this.dataLakeServiceClient = dataLakeServiceClient;
    }

    public static async Task<StorageClient> CreateOneLakeClient(TokenCredential tokenCredential, bool usePathResolution = true, Action<BlobClientOptions, DataLakeClientOptions>? updateOptions = null)
    {
        BlobClientOptions blobClientOptions = new();
        DataLakeClientOptions dataLakeClientOptions = new();
        updateOptions?.Invoke(blobClientOptions, dataLakeClientOptions);

        if (usePathResolution)
        {
            await blobClientOptions.InstallPathResolverAsync(new Uri("https://msit-onelake.blob.fabric.microsoft.com/"));
            await dataLakeClientOptions.InstallPathResolverAsync(new Uri("https://msit-onelake.dfs.fabric.microsoft.com/"));
        }

        var blobServiceClient = new BlobServiceClient(new Uri("https://msit-onelake.blob.fabric.microsoft.com/"), tokenCredential, blobClientOptions);
        var dataLakeServiceClient = new DataLakeServiceClient(new Uri("https://msit-onelake.dfs.fabric.microsoft.com/"), tokenCredential, dataLakeClientOptions);

        return new(blobServiceClient, dataLakeServiceClient);
    }

    public async Task<IStoragePath> CreateContainerAsync(string containerName)
    {
        await blobServiceClient.CreateBlobContainerAsync(containerName);

        return new StoragePath(this, containerName, null);
    }

    public IStoragePath GetPath(string containerName, string? path = null) => new StoragePath(this, containerName, path);

    private class StoragePath(StorageClient client, string containerName, string? path) : IStoragePath
    {
        public async Task<BlobFile> CreateFileAsync(string filePath)
        {
            if (filePath is null)
            {
                throw new ArgumentNullException(nameof(filePath), "File path cannot be null.");
            }
            var containerClient = client.blobServiceClient.GetBlobContainerClient(containerName);
            var temporaryPath = $"_{filePath}.temp"!;
            var blobClient = containerClient.GetBlobClient(Combine(temporaryPath));
            var blobStream = await blobClient.OpenWriteAsync(overwrite: true);
            return new BlobFile(blobStream, GetChildPath(temporaryPath), Combine(filePath)!);
        }

        public IStoragePath GetChildPath(string childPath) => new StoragePath(client, containerName, Combine(childPath));
        
        public IAsyncEnumerable<BlobItem?> GetBlobsAsync(string? prefix = null)
        {
            var containerClient = client.blobServiceClient.GetBlobContainerClient(containerName);
            prefix = Combine(prefix);
            return containerClient.GetBlobsAsync(prefix: prefix);
        }

        public async Task DeleteIfExistsAsync()
        {
            async Task DeleteDirectoryAsync(DataLakeServiceClient dataLakeServiceClient)
            {
                var fileSystemClient = dataLakeServiceClient.GetFileSystemClient(containerName);
                var directoryClient = fileSystemClient.GetDirectoryClient(path);
                await directoryClient.DeleteIfExistsAsync();
            }

            async Task DeleteAllBlobsAsync(BlobServiceClient blobServiceClient)
            {
                var blobs = await GetBlobsAsync().ToArrayAsync();
                foreach (var blob in blobs)
                {
                    if (blob is null) continue;
                    
                    var blobClient = client.blobServiceClient
                        .GetBlobContainerClient(containerName)
                        .GetBlobClient(blob.Name);
                    await blobClient.DeleteIfExistsAsync();
                }
            }
            
            StorageOperation operation = new()
            {
                WithFlatNamespace = DeleteAllBlobsAsync,
                WithHierarchicalNamespace = DeleteDirectoryAsync
            };

            await operation.Execute(client);
        }

        public async Task RenameAsync(string newPath)
        {
            async Task RenamePath(DataLakeServiceClient dataLakeServiceClient)
            {
                var fileSystemClient = dataLakeServiceClient.GetFileSystemClient(containerName);
                var directoryClient = fileSystemClient.GetDirectoryClient(path);
                await directoryClient.RenameAsync(newPath);
            }

            async Task CopyThenDeleteAsync(BlobServiceClient blobServiceClient)
            {
                var sourceBlob = blobServiceClient.GetBlobContainerClient(containerName).GetBlobClient(path);
                var destinationBlob = blobServiceClient.GetBlobContainerClient(containerName).GetBlobClient(newPath);
                
                await destinationBlob.StartCopyFromUriAsync(sourceBlob.Uri);
                await sourceBlob.DeleteIfExistsAsync();
            }

            StorageOperation operation = new()
            {
                WithFlatNamespace = CopyThenDeleteAsync,
                WithHierarchicalNamespace = RenamePath
            };

            await operation.Execute(client);
        }

        private string? Combine(string? childPath)
        {
            if (string.IsNullOrEmpty(path))
            {
                return childPath;
            }
            if (string.IsNullOrEmpty(childPath))
            {
                return path;
            }
            return path.EndsWith('/') ? $"{path}{childPath}" : $"{path}/{childPath}";
        }

        public Task<Stream> OpenRead(BlobItem blob)
        {
            var containerClient = client.blobServiceClient.GetBlobContainerClient(containerName);
            var blobClient = containerClient.GetBlobClient(blob.Name);
            return blobClient.OpenReadAsync();
        }
    }
    
    class StorageOperation
    {
        public required Func<BlobServiceClient, Task> WithFlatNamespace { get; init; }
        public required Func<DataLakeServiceClient, Task> WithHierarchicalNamespace { get; init; }
        
        public async Task Execute(StorageClient client)
        {
            if (client.dataLakeServiceClient is not null)
            {
                await WithHierarchicalNamespace(client.dataLakeServiceClient);
            }
            else
            {
                await WithFlatNamespace(client.blobServiceClient);
            }
        }
    }
}