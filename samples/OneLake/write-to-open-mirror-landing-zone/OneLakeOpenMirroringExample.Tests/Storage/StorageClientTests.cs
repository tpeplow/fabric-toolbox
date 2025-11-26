using Azure.Identity;
using Azure.Storage.Blobs.Models;
using OneLakeOpenMirroringExample.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OneLakeOpenMirroringExample.Tests.Storage
{
    public abstract class StorageClientTests
    {
        protected class Setup
        {
            readonly StorageClient? storageClient;
            readonly IStoragePath? storagePath;
            
            public Setup()
            {

            }

            public Setup(StorageClient storageClient, IStoragePath storagePath)
            {
                this.storageClient = storageClient;
                this.storagePath = storagePath;
            }

            public StorageClient Client => GetInitialized(() => storageClient);
            public IStoragePath StoragePath => GetInitialized(() => storagePath);

            T GetInitialized<T>(Func<T?> getProperty)
            {
                var result = getProperty();
                if (result == null)
                {
                    throw new InvalidOperationException($"Property of type {typeof(T).Name} has not been initialized.");
                }
                return result;
            }
        }

        protected Setup setup = new();


        [Test]
        public async Task when_uploading_a_file_it_should_download_correctly()
        {
            await using var path = await setup.StoragePath.CreateFileAsync("test-file.txt");
            await path.WriteData(async stream =>
            {
                var data = Encoding.UTF8.GetBytes("Hello, OneLake!");
                await stream.WriteAsync(data, 0, data.Length);
            });
            await path.DisposeAsync();

            var blobs = await setup.StoragePath.GetBlobsAsync("test-file.txt").ToArrayAsync();
            Assert.That(blobs.Length == 1);
            
            await using var blob = await setup.StoragePath.OpenRead(blobs[0]!);
            using var reader = new StreamReader(blob);
            var contents = await reader.ReadToEndAsync();
            Assert.That(contents, Is.EqualTo("Hello, OneLake!"));
        }
    }

    public class when_using_the_emulator : StorageClientTests
    {
        [SetUp]
        public async Task UsingEmulator()
        {
            var storageClient = await AzuriteHost.GetStorageClientUsingAzurite();
            var path = await storageClient.CreateContainerAsync(Guid.NewGuid().ToString());
            setup = new Setup(storageClient, path);
        }
    }

    public class when_using_OneLakeProxy : StorageClientTests
    {
        [SetUp]
        public async Task UsingOneLakeProxy()
        {
            var storageClient = StorageClient.CreateOneLakeClient(new DefaultAzureCredential());
            var path = storageClient.GetPath(OneLakePaths.WorkspaceName, OneLakePaths.GetLakehouseFilesPath());
            setup = new Setup(storageClient, path);
        }
    }
}
