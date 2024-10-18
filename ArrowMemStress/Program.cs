namespace ArrowMemStress
{
    using Apache.Arrow;
    using Apache.Arrow.Types;
    using System.Diagnostics;
    using System.Text;

    public class Program
    {
        private const string alphabets = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        private const string stringColumnName = "colStringTest";

        public static void Main(string[] args)
        {
            int numRows = int.Parse(Environment.GetEnvironmentVariable("NUM_ROWS") ?? "10000000");
            int numLoops = int.Parse(Environment.GetEnvironmentVariable("NUM_LOOPS") ?? "100");
            int numThreads = int.Parse(Environment.GetEnvironmentVariable("NUM_THREADS") ?? "1");

            Parallel.For(0, numThreads, t =>
            {
                for (int i = 0; i < numLoops; i++)
                {
                    RecordBatch.Builder recordBatchBuilder = new RecordBatch.Builder().Append(stringColumnName, false, col => col.String(arr => arr.AppendRange(Enumerable.Range(0, numRows).Select(_ => alphabets))));
                    RecordBatch recordBatch = recordBatchBuilder.Build();
                    long approxSizeInMb = ApproximateBatchMemoryPressureInBytes(recordBatch.Arrays) / 1024 / 1024;
                    
                    // 1. Before disposing
                    long memoryBeforeDisposeInMb = ProcessMemoryProfiler.ReportInMb();

                    recordBatch.Dispose();
                    
                    // 2. After disposing
                    long memoryAfterDisposeInMb = ProcessMemoryProfiler.ReportInMb();
                    long memoryReleasedAfterDisposeInMb = memoryBeforeDisposeInMb - memoryAfterDisposeInMb;

                    Console.WriteLine($"[ Threads: {t + 1}/{numThreads}, Loops: {i + 1} of {numLoops} ] {recordBatch.Length} rows, approx. size: {approxSizeInMb:F0} MB, before dispose -> after dispose: [{memoryBeforeDisposeInMb} MB -> {memoryAfterDisposeInMb} MB, diff: {memoryReleasedAfterDisposeInMb} MB]");
                }
            });
        }

        private static class ProcessMemoryProfiler
        {
            internal static long ReportInMb() => Process.GetCurrentProcess().WorkingSet64 / 1024 / 1024;
        }

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
    }
}
