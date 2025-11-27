using Azure.Core;
using Azure.Core.Pipeline;
using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.Storage.Files.DataLake;
using OneLakeOpenMirroringExample.Storage;
using System.Text;

namespace OneLakeOpenMirroringExample.Tests.Storage
{
    public abstract class StorageClientTests
    {
        [Test]
        [Order(1)]
        public async Task when_uploading_a_file_it_should_download_correctly()
        {
            var path = await setup.StoragePath.CreateFileAsync("test-file.txt");
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
        protected class Transport : HttpClientTransport
        {
            public HashSet<string?> StorageAccounts { get; } = new();

            public void Register(BlobClientOptions blobClientOptions, DataLakeClientOptions dataLakeClientOptions)
            {
                blobClientOptions.Transport = this;
                dataLakeClientOptions.Transport = this;
            }

            public override ValueTask ProcessAsync(HttpMessage message)
            {
                StorageAccounts.Add(message.Request.Uri.Host);

                return base.ProcessAsync(message);
            }
        }

        protected Setup setup = new();
        protected Transport transport;

        [OneTimeSetUp]
        public void Init()
        {
            transport = new Transport();
        }

        [OneTimeTearDown]
        public void Teardown()
        {
            transport.Dispose();
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
        [OneTimeSetUp]
        public async Task UsingOneLakeProxy()
        {
            var storageClient = await StorageClient.CreateOneLakeClient(new DefaultAzureCredential(), usePathResolution: false, transport.Register);
            var path = storageClient.GetPath(OneLakePaths.WorkspaceName, OneLakePaths.GetLakehouseFilesPath());
            setup = new Setup(storageClient, path);
        }

        [Test]
        [Order(2)]
        public void it_should_send_all_requests_to_OneLake()
        {
            Assert.That(transport.StorageAccounts, Is.EquivalentTo(new[]
            {
                "msit-onelake.dfs.fabric.microsoft.com",
                "msit-onelake.blob.fabric.microsoft.com"
            }));
        }
    }

    public class when_using_OneLake_with_path_resolution : StorageClientTests
    {
        [SetUp]
        public async Task UsingOneLakeWithPathResolution()
        {
            var storageClient = await StorageClient.CreateOneLakeClient(new DefaultAzureCredential(), usePathResolution: true, transport.Register);
            var path = storageClient.GetPath(OneLakePaths.WorkspaceName, OneLakePaths.GetLakehouseFilesPath());
            setup = new Setup(storageClient, path);
        }

        [Test]
        [Order(2)]
        public void it_should_go_direct_to_underlying_storage_account()
        {
            Assert.That(transport.StorageAccounts.Count, Is.EqualTo(3));
        }
    }
}
