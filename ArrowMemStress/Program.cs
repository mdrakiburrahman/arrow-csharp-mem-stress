namespace ArrowMemStress
{
    using Apache.Arrow;
    using Apache.Arrow.Memory;
    using Apache.Arrow.Types;
    using DeltaLake.Table;
    using Microsoft.Data.Analysis;
    using System.Diagnostics;
    using System.Threading;

    public class Program
    {
        private const string alphabets = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        private const string stringColumnName = "colStringTest";
        private static Schema schema = BuildSchema();
        private static readonly SemaphoreSlim deltaTableTransactionLock = new(1, 1);

        public static void Main(string[] args)
        {
            long memoryAtAppStart = ProcessMemoryProfiler.ReportInMb();
            long managedHeapAtAppStart = ProcessMemoryProfiler.ReportManagedHeapLiveObjectsInMb();

            Console.WriteLine($"[ App Start ] App: {memoryAtAppStart:F0} MB, Managed Heap: {managedHeapAtAppStart:F0} MB");

            int numRows = int.Parse(Environment.GetEnvironmentVariable("NUM_ROWS") ?? "10000000");
            int numLoops = int.Parse(Environment.GetEnvironmentVariable("NUM_LOOPS") ?? "100");
            int numThreads = int.Parse(Environment.GetEnvironmentVariable("NUM_THREADS") ?? "1");
            int stringLength = int.Parse(Environment.GetEnvironmentVariable("NUM_CHARS_IN_WRITTEN_COLUMN") ?? "10");
            string storageAccountName = (Environment.GetEnvironmentVariable("STORAGE_ACCOUNT_NAME") ?? "someaccount");
            string storageContainerName = (Environment.GetEnvironmentVariable("STORAGE_CONTAINER_NAME") ?? "somecontainer");
            string storageAccountRelativePath = (Environment.GetEnvironmentVariable("STORAGE_TABLE_RELATIVE_PATH") ?? "some/path/table");

            var threadColumn = new PrimitiveDataFrameColumn<int>("Thread #");
            var loopColumn = new PrimitiveDataFrameColumn<int>("Loop #");
            var numRowsColumn = new PrimitiveDataFrameColumn<int>("Number of Rows");
            var memoryBeforeRecordBatchCreateColumn = new PrimitiveDataFrameColumn<long>("Memory Before RecordBatch Create");
            var managedHeapBeforeRecordBatchCreateColumn = new PrimitiveDataFrameColumn<long>("Managed Heap Before RecordBatch Create");
            var memoryAfterRecordBatchCreateColumn = new PrimitiveDataFrameColumn<long>("Memory After RecordBatch Create");
            var managedHeapAfterRecordBatchCreateColumn = new PrimitiveDataFrameColumn<long>("Managed Heap After RecordBatch Create");
            var memoryAfterDeltaWriteColumn = new PrimitiveDataFrameColumn<long>("Memory After Delta Write");
            var managedHeapAfterDeltaWriteColumn = new PrimitiveDataFrameColumn<long>("Managed Heap After Delta Write");
            var memoryAfterRecordBatchDisposeColumn = new PrimitiveDataFrameColumn<long>("Memory After RecordBatch Dispose");
            var managedHeapAfterRecordBatchDisposeColumn = new PrimitiveDataFrameColumn<long>("Managed Heap After RecordBatch Dispose");
            var memoryAfterGcColumn = new PrimitiveDataFrameColumn<long>("Memory After GC");
            var managedHeapAfterGcColumn = new PrimitiveDataFrameColumn<long>("Managed Heap After GC");
            var appStartToLoopEndMemoryColumn = new PrimitiveDataFrameColumn<long>("Memory Start and End Diff");
            var appStartToLoopEndManagedHeapColumn = new PrimitiveDataFrameColumn<long>("Managed Heap Start and End Diff");
            var appStartToLoopEndMemoryPercentColumn = new PrimitiveDataFrameColumn<long>("Memory Start and End %");
            var appStartToLoopEndManagedHeapPercentColumn = new PrimitiveDataFrameColumn<long>("Managed Heap Start and End %");

            Random randomValueGenerator = new ();
            ThreadSafeDeltaTableClient threadSafeDeltaTableClient = new ThreadSafeDeltaTableClient(storageAccountName, storageContainerName, storageAccountRelativePath, schema);
            NativeMemoryAllocator memoryAllocator = new(alignment: 64);

            Parallel.For(0, numThreads, t =>
            {
                for (int i = 0; i < numLoops; i++)
                {
                    Console.WriteLine($"[ Thread {t + 1} of {numThreads} ] Loop {i + 1} of {numLoops}");

                    string[] stringArray = Enumerable.Range(0, numRows).Select(_ => GenerateRandomString(randomValueGenerator, stringLength)).ToArray();

                    long memoryBeforeRecordBatchCreate = ProcessMemoryProfiler.ReportInMb();
                    long managedHeapBeforeRecordBatchCreate = ProcessMemoryProfiler.ReportManagedHeapLiveObjectsInMb();

                    RecordBatch.Builder recordBatchBuilder = new RecordBatch.Builder(memoryAllocator).Append(stringColumnName, false, col => col.String(arr => arr.AppendRange(stringArray)));
                    RecordBatch[] outgoingBatches = new RecordBatch[] { recordBatchBuilder.Build() };

                    // 1. After creating RecordBatch
                    long memoryAfterRecordBatchCreate = ProcessMemoryProfiler.ReportInMb();
                    long managedHeapAfterRecordBatchCreate = ProcessMemoryProfiler.ReportManagedHeapLiveObjectsInMb();

                    // 2. After Delta Write
                    deltaTableTransactionLock.Wait();
                    try
                    {
                        threadSafeDeltaTableClient.GetDeltaTableClient().InsertAsync(
                            outgoingBatches,
                            schema,
                            new InsertOptions { SaveMode = SaveMode.Append },
                            default
                        ).GetAwaiter().GetResult();
                    }
                    finally
                    {
                        deltaTableTransactionLock.Release();
                    }
                    long memoryAfterDeltaWrite = ProcessMemoryProfiler.ReportInMb();
                    long managedHeapAfterDeltaWrite = ProcessMemoryProfiler.ReportManagedHeapLiveObjectsInMb();

                    // 2. After disposing RecordBatch
                    foreach (RecordBatch recordBatch in outgoingBatches) recordBatch.Dispose();
                    recordBatchBuilder.Clear();
                    stringArray = null;

                    long memoryAfterRecordBatchDispose = ProcessMemoryProfiler.ReportInMb();
                    long managedHeapAfterRecordBatchDispose = ProcessMemoryProfiler.ReportManagedHeapLiveObjectsInMb();

                    // 3. After forcing GC
                    GC.Collect(generation: GC.MaxGeneration, mode: GCCollectionMode.Aggressive, blocking: true, compacting: true);

                    long memoryAfterGc = ProcessMemoryProfiler.ReportInMb();
                    long managedHeapAfterGc = ProcessMemoryProfiler.ReportManagedHeapLiveObjectsInMb();

                    // Diffs
                    long memoryRecordBatchActual = memoryAfterRecordBatchCreate - memoryBeforeRecordBatchCreate;
                    long managedHeapRecordBatchActual = managedHeapAfterRecordBatchCreate - managedHeapBeforeRecordBatchCreate;

                    long disposedMemoryInMb = memoryAfterRecordBatchDispose - memoryAfterRecordBatchCreate;
                    long disposedManagedHeapInMb = managedHeapAfterRecordBatchDispose - managedHeapAfterRecordBatchCreate;

                    long memorySavedAfterGc = memoryAfterGc - memoryAfterRecordBatchDispose;
                    long managedHeapSavedAfterGc = managedHeapAfterRecordBatchDispose - managedHeapAfterGc;

                    long appStartToLoopEndMemory = memoryAfterGc - memoryBeforeRecordBatchCreate;
                    long appStartToLoopEndManagedHeap = managedHeapAfterGc - managedHeapBeforeRecordBatchCreate;

                    long appStartToLoopEndMemoryPercentageGrowth = (appStartToLoopEndMemory * 100 / memoryBeforeRecordBatchCreate);
                    long appStartToLoopEndManagedHeapPercentageGrowth = (appStartToLoopEndManagedHeap * 100 / managedHeapBeforeRecordBatchCreate);

                    long appStartToLoopEndNativeHeapGrowth = appStartToLoopEndMemory - appStartToLoopEndManagedHeap;

                    lock (threadColumn)
                    {
                        threadColumn.Append(t + 1);
                        loopColumn.Append(i + 1);
                        numRowsColumn.Append(numRows);
                        memoryBeforeRecordBatchCreateColumn.Append(memoryBeforeRecordBatchCreate);
                        managedHeapBeforeRecordBatchCreateColumn.Append(managedHeapBeforeRecordBatchCreate);
                        memoryAfterRecordBatchCreateColumn.Append(memoryAfterRecordBatchCreate);
                        managedHeapAfterRecordBatchCreateColumn.Append(managedHeapAfterRecordBatchCreate);
                        memoryAfterDeltaWriteColumn.Append(memoryAfterDeltaWrite);
                        managedHeapAfterDeltaWriteColumn.Append(managedHeapAfterDeltaWrite);
                        memoryAfterRecordBatchDisposeColumn.Append(memoryAfterRecordBatchDispose);
                        managedHeapAfterRecordBatchDisposeColumn.Append(managedHeapAfterRecordBatchDispose);
                        memoryAfterGcColumn.Append(memoryAfterGc);
                        managedHeapAfterGcColumn.Append(managedHeapAfterGc);
                        appStartToLoopEndMemoryColumn.Append(appStartToLoopEndMemory);
                        appStartToLoopEndManagedHeapColumn.Append(appStartToLoopEndManagedHeap);
                        appStartToLoopEndMemoryPercentColumn.Append(appStartToLoopEndMemoryPercentageGrowth);
                        appStartToLoopEndManagedHeapPercentColumn.Append(appStartToLoopEndManagedHeapPercentageGrowth);
                    }
                }
            });

            var dataFrame = new DataFrame(
                threadColumn,
                loopColumn,
                numRowsColumn,
                memoryBeforeRecordBatchCreateColumn,
                managedHeapBeforeRecordBatchCreateColumn,
                memoryAfterRecordBatchCreateColumn,
                managedHeapAfterRecordBatchCreateColumn,
                memoryAfterDeltaWriteColumn,
                managedHeapAfterDeltaWriteColumn,
                memoryAfterRecordBatchDisposeColumn,
                managedHeapAfterRecordBatchDisposeColumn,
                memoryAfterGcColumn,
                managedHeapAfterGcColumn,
                appStartToLoopEndMemoryColumn,
                appStartToLoopEndManagedHeapColumn,
                appStartToLoopEndMemoryPercentColumn,
                appStartToLoopEndManagedHeapPercentColumn
            );
            Console.WriteLine(dataFrame.ToMarkdown());
        }

        private static class ProcessMemoryProfiler
        {
            internal static long ReportInMb() => Process.GetCurrentProcess().WorkingSet64 / 1024 / 1024;
            internal static long ReportManagedHeapLiveObjectsInMb() => GC.GetTotalMemory(false) / 1024 / 1024;
        }

        private static string GenerateRandomString(Random random, int length = 10) => new string(Enumerable.Repeat(alphabets, length).Select(s => s[random.Next(s.Length)]).ToArray());

        private static Schema BuildSchema()
        {
            var builder = new Apache.Arrow.Schema.Builder();
            builder = builder.Field(fb => { fb.Name(stringColumnName); fb.DataType(StringType.Default); fb.Nullable(false); });
            return builder.Build();
        }
    }
}
