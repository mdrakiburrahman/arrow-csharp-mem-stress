namespace ArrowMemStress
{
    using Apache.Arrow;
    using Apache.Arrow.Types;
    using DeltaLake.Table;
    using System.Diagnostics;
    using System.Text;
    using System.Threading;

    public class Program
    {
        private const string alphabets = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        private const string stringColumnName = "colStringTest";
        private const int maxDeltaRetries = 5;
        private static Schema schema = BuildSchema();
        private static readonly SemaphoreSlim deltaTableTransactionLock = new(1, 1);

        public static void Main(string[] args)
        {
            int numRows = int.Parse(Environment.GetEnvironmentVariable("NUM_ROWS") ?? "10000000");
            int numLoops = int.Parse(Environment.GetEnvironmentVariable("NUM_LOOPS") ?? "100");
            int numThreads = int.Parse(Environment.GetEnvironmentVariable("NUM_THREADS") ?? "1");
            int stringLength = int.Parse(Environment.GetEnvironmentVariable("NUM_CHARS_IN_WRITTEN_COLUMN") ?? "10");
            string storageAccountName = (Environment.GetEnvironmentVariable("STORAGE_ACCOUNT_NAME") ?? "someaccount");
            string storageContainerName = (Environment.GetEnvironmentVariable("STORAGE_CONTAINER_NAME") ?? "somecontainer");
            string storageAccountRelativePath = (Environment.GetEnvironmentVariable("STORAGE_TABLE_RELATIVE_PATH") ?? "some/path/table");
            bool writeDelta = bool.Parse(Environment.GetEnvironmentVariable("WRITE_DELTA") ?? "true");
            Random randomValueGenerator = new ();

            ThreadSafeDeltaTableClient threadSafeDeltaTableClient = new ThreadSafeDeltaTableClient(storageAccountName, storageContainerName, storageAccountRelativePath, schema);
            
            Parallel.For(0, numThreads, t =>
            {
                for (int i = 0; i < numLoops; i++)
                {
                    long memoryBeforeRecordBatchCreate = ProcessMemoryProfiler.ReportInMb();

                    RecordBatch.Builder recordBatchBuilder = new RecordBatch.Builder().Append(stringColumnName, false, col => col.String(arr => arr.AppendRange(Enumerable.Range(0, numRows).Select(_ => GenerateRandomString(randomValueGenerator, stringLength)))));
                    RecordBatch[] outgoingBatches = new RecordBatch[] { recordBatchBuilder.Build() };
                    long approxSizeInMb = ApproximateMemoryPressureInBytes(outgoingBatches) / 1024 / 1024;
                    
                    // 1. After creating RecordBatch
                    long memoryAfterRecordBatchCreate = ProcessMemoryProfiler.ReportInMb();

                    // 2. After Delta Write
                    deltaTableTransactionLock.Wait();
                    try
                    {
                        if (writeDelta)
                        {
                            threadSafeDeltaTableClient.GetDeltaTableClient().InsertAsync(
                                outgoingBatches,
                                schema,
                                new InsertOptions { SaveMode = SaveMode.Append },
                                default
                            ).GetAwaiter().GetResult();
                        }
                    }
                    finally
                    {
                        deltaTableTransactionLock.Release();
                    }
                    long memoryAfterDeltaWrite = ProcessMemoryProfiler.ReportInMb();

                    // 3. After disposing RecordBatch
                    foreach (RecordBatch recordBatch in outgoingBatches) recordBatch.Dispose();
                    long memoryAfterDisposeInMb = ProcessMemoryProfiler.ReportInMb();

                    // Diffs
                    long memoryRecordBatchActual = memoryAfterRecordBatchCreate - memoryBeforeRecordBatchCreate;
                    long deltaWriteMemory = memoryAfterDeltaWrite - memoryAfterRecordBatchCreate;
                    long disposedMemoryInMb = memoryAfterDisposeInMb - memoryAfterDeltaWrite;
                    long loopStartToEndMemory = memoryAfterDisposeInMb - memoryBeforeRecordBatchCreate;

                    StringBuilder sb = new StringBuilder();
                    sb.Append($"[ Threads: {t + 1}/{numThreads}, Loops: {i + 1} of {numLoops} ] {numRows} rows, wrote to delta: {writeDelta}, approx. size: {approxSizeInMb:F0} MB, ");
                    sb.Append($"before RecordBatch create: {memoryBeforeRecordBatchCreate:F0} MB -> ");
                    sb.Append($"after RecordBatch create: {memoryAfterRecordBatchCreate:F0} MB (^{memoryRecordBatchActual:F0} MB) -> ");
                    sb.Append($"after Delta write: {memoryAfterDeltaWrite:F0} MB (^{deltaWriteMemory:F0} MB) -> ");
                    sb.Append($"after RecordBatch dispose: {memoryAfterDisposeInMb:F0} (^{disposedMemoryInMb:F0} MB) -> ");
                    sb.Append($"loop {i + 1} start to end diff: START: {memoryBeforeRecordBatchCreate:F0} MB - END: {memoryAfterDisposeInMb:F0} MB (^{loopStartToEndMemory:F0} MB)");
                    Console.WriteLine(sb);
                }
            });
        }

        private static class ProcessMemoryProfiler
        {
            internal static long ReportInMb() => Process.GetCurrentProcess().WorkingSet64 / 1024 / 1024;
        }

        private static long ApproximateMemoryPressureInBytes(IReadOnlyCollection<RecordBatch> recordBatches)
        {
            long totalMemoryPressure = 0;

            foreach (var recordBatch in recordBatches)
            {
                foreach (var column in recordBatch.Arrays)
                {
                    totalMemoryPressure += ApproximateColumnMemoryPressureInBytes(column);
                }
            }

            return totalMemoryPressure;
        }

        private static string GenerateRandomString(Random random, int length = 10) => new string(Enumerable.Repeat(alphabets, length).Select(s => s[random.Next(s.Length)]).ToArray());

        private static long ApproximateBatchMemoryPressureInBytes(IEnumerable<IArrowArray> columns)
        {
            long memoryPressure = 0;
            foreach (var column in columns)
            {
                memoryPressure += ApproximateColumnMemoryPressureInBytes(column);
            }
            return memoryPressure;
        }

        private static long ApproximateColumnMemoryPressureInBytes(IArrowArray column)
        {
            long memoryPressure = 0;

            switch (column)
            {
                case Int8Array int8Array:
                    memoryPressure += int8Array.Length * sizeof(sbyte);
                    break;
                case Int16Array int16Array:
                    memoryPressure += int16Array.Length * sizeof(short);
                    break;
                case Int32Array int32Array:
                    memoryPressure += int32Array.Length * sizeof(int);
                    break;
                case Int64Array int64Array:
                    memoryPressure += int64Array.Length * sizeof(long);
                    break;
                case FloatArray floatArray:
                    memoryPressure += floatArray.Length * sizeof(float);
                    break;
                case DoubleArray doubleArray:
                    memoryPressure += doubleArray.Length * sizeof(double);
                    break;
                case StringArray stringArray:
                    foreach (var str in (IEnumerable<string>)stringArray)
                    {
                        if (str != null)
                        {
                            memoryPressure += str.Length * sizeof(char);
                        }
                    }
                    break;
                case BinaryArray binaryArray:
                    foreach (var binary in (IEnumerable<byte[]>)binaryArray)
                    {
                        if (binary != null)
                        {
                            memoryPressure += binary.Length;
                        }
                    }
                    break;
                default:
                    throw new NotImplementedException($"Memory pressure calculation for {column.GetType().Name} is not implemented.");
            }

            return memoryPressure;
        }

        private static Schema BuildSchema()
        {
            var builder = new Apache.Arrow.Schema.Builder();
            builder = builder.Field(fb => { fb.Name(stringColumnName); fb.DataType(StringType.Default); fb.Nullable(false); });
            return builder.Build();
        }
    }
}
