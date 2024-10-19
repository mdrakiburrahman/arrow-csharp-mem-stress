namespace ArrowMemStress
{
    using Apache.Arrow;
    using Azure.Core;
    using Azure.Identity;
    using Azure.Storage.Blobs;
    using DeltaLake.Errors;
    using DeltaLake.Runtime;
    using DeltaLake.Table;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text.RegularExpressions;

    public class ThreadSafeDeltaTableClient
    {
        private static string envVarIfRunningInVisualStudio = Environment.GetEnvironmentVariable("VisualStudioVersion");
        private const string dirDeltaTransactionLog = "_delta_log";
        private const string rustObjectStoreBearerTokenIdentifier = "bearer_token";
        private const string storageAccountDfsEndpoint = "dfs.core.windows.net";
        private const string storageAccountBlobEndpoint = "blob.core.windows.net";
        private static readonly string[] StorageScope =
        [
            "https://storage.azure.com/.default"
        ];

        private readonly DeltaRuntime deltaRuntime = new(RuntimeOptions.Default);

        private readonly TokenCredential tokenCredential;
        private readonly BlobContainerClient storageClient;
        private readonly string locationAbfss;
        private readonly Schema schema;
        private readonly string storageAccountName;
        private readonly string storageContainerName;
        private readonly string storageAccountRelativePath;

        private readonly object deltaTableClientLock = new();
        private DeltaTable deltaTableClient;



        private static readonly string[] SafeCreateTableErrors = new[]
        {
            @"Generic error: SaveMode `\w+` is not allowed for create operation",
            @"Delta transaction failed, version \d+ already exists"
        };

        public ThreadSafeDeltaTableClient(
            string storageAccountName,
            string storageContainerName,
            string storageAccountRelativePath,
            Schema schema
        )
        {
            this.storageAccountName = storageAccountName;
            this.storageContainerName = storageContainerName;
            this.storageAccountRelativePath = storageAccountRelativePath;
            this.tokenCredential = GetSuitableTokenCredential();
            this.storageClient = new(new Uri($"https://{storageAccountName}.{storageAccountBlobEndpoint}/{storageContainerName}"), tokenCredential);
            this.locationAbfss = $"abfss://{storageContainerName}@{storageAccountName}.{storageAccountDfsEndpoint}/{storageAccountRelativePath}";
            this.schema = schema;
        }

        public DeltaTable GetDeltaTableClient()
        {
            lock (deltaTableClientLock)
            {
                if (this.deltaTableClient != null)
                    return this.deltaTableClient;

                Console.WriteLine($"Delta Table Client for does not exist, creating new Delta Table client.");
                try
                {
                    if (!this.DoesDeltaTableExist())
                    {
                        try
                        {
                            this.deltaTableClient = this.CreateTable(this.deltaRuntime);
                            return this.deltaTableClient;
                        }
                        catch (DeltaRuntimeException ex)
                        {
                            if (SafeCreateTableErrors.Any(pattern => Regex.IsMatch(ex.Message, pattern)))
                            {
                                Console.WriteLine($"Another replica must have created the Delta Table for - proceeding to load despite error: {ex.Message}");
                                this.deltaTableClient = this.LoadTable(this.deltaRuntime);
                            }
                            else
                            {
                                Console.WriteLine($"Hit unexpected error while creating Delta Table: {ex.Message}.");
                                throw;
                            }
                        }
                    }
                    else
                    {
                        this.deltaTableClient = this.LoadTable(this.deltaRuntime);
                        return this.deltaTableClient;
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error creating Delta Table Client: {ex.Message}.");
                    throw;
                }
            }
            throw new Exception("Delta Table does not exist and could not be created.");
        }

        private DeltaTable CreateTable(DeltaRuntime runtime)
        {
            var accessToken = this.RefreshToken();
            return
            DeltaTable
                    .CreateAsync(
                        runtime,
                        new TableCreateOptions(
                            locationAbfss,
                            schema
                        )
                        {
                            StorageOptions = new Dictionary<string, string> {
                                                                               { rustObjectStoreBearerTokenIdentifier, accessToken.Token }
                                                                            },
                        },
                        default
                    )
                    .GetAwaiter()
                    .GetResult();
        }

        private static TokenCredential GetSuitableTokenCredential()
        {
            bool isRunningInsideVisualStudio = !string.IsNullOrEmpty(envVarIfRunningInVisualStudio);
            if (isRunningInsideVisualStudio)
            {
                return new VisualStudioCredential();
            }
            return new WorkloadIdentityCredential();
        }

        private bool DoesDeltaTableExist()
        {
            foreach (var blobItem in storageClient.GetBlobs(prefix: $"{storageAccountRelativePath}/{dirDeltaTransactionLog}")) return true;
            return false;
        }

        private AccessToken RefreshToken() => tokenCredential.GetToken(new TokenRequestContext(StorageScope), default);

        private DeltaTable LoadTable(DeltaRuntime runtime)
        {
            AccessToken accessToken = this.RefreshToken();
            TableOptions options = new();
            options.StorageOptions.Add(
                rustObjectStoreBearerTokenIdentifier,
                accessToken.Token
            );

            return
                DeltaTable
                    .LoadAsync(
                        runtime,
                        locationAbfss,
                        options,
                        default
                    )
                    .GetAwaiter()
                    .GetResult();
        }
    }
}
