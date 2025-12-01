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
        public const int RunAfterAction = 3;

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

        [Test]
        [Order(2)]
        public async Task when_deleting_file_it_should_be_deleted()
        {
            var pathToDelete = setup.StoragePath.GetChildPath("test-file.txt");
            await pathToDelete.DeleteIfExistsAsync();

            var exists = await pathToDelete.ExistsAsync();

            Assert.That(exists, Is.False);
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
            public Dictionary<string, List<(Uri uri, RequestMethod httpMethod, Dictionary<string, string> headers)>> Requests { get; } = new();

            public void Register(BlobClientOptions blobClientOptions, DataLakeClientOptions dataLakeClientOptions)
            {
                blobClientOptions.Transport = this;
                dataLakeClientOptions.Transport = this;
            }

            public override ValueTask ProcessAsync(HttpMessage message)
            {
                if (message.Request.Uri.Host != null)
                {
                    if (StorageAccounts.Add(message.Request.Uri.Host))
                    {
                        Requests.Add(message.Request.Uri.Host, []);
                    }
                    Requests[message.Request.Uri.Host].Add((message.Request.Uri.ToUri(), message.Request.Method, message.Request.Headers.ToDictionary(x => x.Name, x => x.Value)));
                }
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
        [Order(RunAfterAction)]
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
        [OneTimeSetUp]
        public async Task UsingOneLakeWithPathResolution()
        {
            var storageClient = await StorageClient.CreateOneLakeClient(new DefaultAzureCredential(), usePathResolution: true, transport.Register);
            var path = storageClient.GetPath(OneLakePaths.WorkspaceName, OneLakePaths.GetLakehouseFilesPath());
            setup = new Setup(storageClient, path);
        }

        [Test]
        [Order(RunAfterAction)]
        public void it_should_go_direct_to_underlying_storage_account()
        {
            Assert.That(transport.StorageAccounts.Count, Is.EqualTo(3));
        }

        [Test]
        [Order(RunAfterAction)]
        public void it_should_not_redirect_renames()
        {
            var rename = transport.Requests["msit-onelake.dfs.fabric.microsoft.com"]
                .Any(r => r.headers.ContainsKey("x-ms-rename-source"));
            Assert.That(rename, Is.True);
        }

        [Test]
        [Order(RunAfterAction)]
        public void it_should_not_redirect_lists()
        {
            var list = transport.Requests["msit-onelake.blob.fabric.microsoft.com"]
                .Any(r => r.httpMethod == RequestMethod.Get && r.uri.Query.Contains("comp=list"));
            Assert.That(list, Is.True);
        }

        [Test]
        [Order(RunAfterAction)]
        public void it_should_not_redirect_deletes()
        {
            var delete = transport.Requests["msit-onelake.dfs.fabric.microsoft.com"]
                .Any(r => r.httpMethod == RequestMethod.Delete && r.uri.AbsolutePath.EndsWith("test-file.txt"));
            Assert.That(delete, Is.True);
        }


        [Test]
        [Order(RunAfterAction)]
        public void it_should_redirect_create_file()
        {
            var createFile = transport.Requests[UnderlyingStorageAccount]
                .Count(r => r.httpMethod == RequestMethod.Put && r.uri.AbsolutePath.EndsWith("_test-file.txt.temp"));
            Assert.That(createFile, Is.EqualTo(3)); // create, add block, commit block
        }


        [Test]
        [Order(RunAfterAction)]
        public void it_should_redirect_writing_blocks()
        {
            var createFile = transport.Requests[UnderlyingStorageAccount]
                .Any(r => r.httpMethod == RequestMethod.Put && r.uri.Query.Contains("&comp=block"));
            Assert.That(createFile, Is.True);
        }


        [Test]
        [Order(RunAfterAction)]
        public void it_should_redirect_committing_blocks()
        {
            var createFile = transport.Requests[UnderlyingStorageAccount]
                .Any(r => r.httpMethod == RequestMethod.Put && r.uri.Query.Contains("&comp=blocklist"));
            Assert.That(createFile, Is.True);
        }

        [Test]
        [Order(RunAfterAction)]
        public void it_should_redirect_get_blob_properties()
        {
            var createFile = transport.Requests[UnderlyingStorageAccount]
                .Any(r => r.httpMethod == RequestMethod.Head);
            Assert.That(createFile, Is.True);
        }


        [Test]
        [Order(RunAfterAction)]
        public void it_should_redirect_get_blob()
        {
            var createFile = transport.Requests[UnderlyingStorageAccount]
                .Any(r => r.httpMethod == RequestMethod.Get);
            Assert.That(createFile, Is.True);
        }

        string UnderlyingStorageAccount => transport.StorageAccounts.Single(s => s != "msit-onelake.blob.fabric.microsoft.com" && s != "msit-onelake.dfs.fabric.microsoft.com");
    }
}
