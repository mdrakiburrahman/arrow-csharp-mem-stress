namespace ArrowMemStress
{
    using Apache.Arrow;
    using Apache.Arrow.Memory;
    using System.Diagnostics;
    using System.Text;

    public class Program
    {
        private const string alphabets = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        private const string stringColumnName = "colStringTest";

        public static void Main(string[] args)
        {
            long memoryAtAppStart = ProcessMemoryProfiler.ReportInMb();

            int numRows = int.Parse(Environment.GetEnvironmentVariable("NUM_ROWS") ?? "10000000");
            int numLoops = int.Parse(Environment.GetEnvironmentVariable("NUM_LOOPS") ?? "100");
            int numThreads = int.Parse(Environment.GetEnvironmentVariable("NUM_THREADS") ?? "1");
            int stringLength = int.Parse(Environment.GetEnvironmentVariable("NUM_CHARS_IN_WRITTEN_COLUMN") ?? "10");
            
            Random randomValueGenerator = new ();
            NativeMemoryAllocator memoryAllocator = new(alignment: 64);

            Parallel.For(0, numThreads, t =>
            {
                for (int i = 0; i < numLoops; i++)
                {
                    long memoryBeforeRecordBatchCreate = ProcessMemoryProfiler.ReportInMb();

                    RecordBatch.Builder recordBatchBuilder = new RecordBatch.Builder(memoryAllocator).Append(stringColumnName, false, col => col.String(arr => arr.AppendRange(Enumerable.Range(0, numRows).Select(_ => GenerateRandomString(randomValueGenerator, stringLength)))));
                    RecordBatch[] outgoingBatches = new RecordBatch[] { recordBatchBuilder.Build() };

                    // 1. After creating RecordBatch
                    long memoryAfterRecordBatchCreate = ProcessMemoryProfiler.ReportInMb();

                    // 2. After disposing RecordBatch
                    foreach (RecordBatch recordBatch in outgoingBatches) recordBatch.Dispose();
                    recordBatchBuilder.Clear();

                    long memoryAfterDisposeInMb = ProcessMemoryProfiler.ReportInMb();

                    // Diffs
                    long memoryRecordBatchActual = memoryAfterRecordBatchCreate - memoryBeforeRecordBatchCreate;
                    long disposedMemoryInMb = memoryAfterDisposeInMb - memoryAfterRecordBatchCreate;
                    long appStartToLoopEndMemory = memoryAfterDisposeInMb - memoryAtAppStart;
                    long appStartToLoopEndMemoryPercentageGrowth = (appStartToLoopEndMemory / memoryAtAppStart) * 100;

                    StringBuilder sb = new StringBuilder();
                    sb.Append($"[ Threads: {t + 1}/{numThreads}, Loops: {i + 1} of {numLoops} ] {numRows} rows, ");
                    sb.Append($"number of allocations: {memoryAllocator.Statistics.Allocations} -> ");
                    sb.Append($"allocations in MB: {(memoryAllocator.Statistics.BytesAllocated / 1024 / 1024):F0} MB -> ");
                    sb.Append($"before RecordBatch create: {memoryBeforeRecordBatchCreate:F0} MB -> ");
                    sb.Append($"after RecordBatch create: {memoryAfterRecordBatchCreate:F0} MB (^{memoryRecordBatchActual:F0} MB) -> ");
                    sb.Append($"after RecordBatch dispose: {memoryAfterDisposeInMb:F0} (^{disposedMemoryInMb:F0} MB) -> ");
                    sb.Append($"app start to loop {i + 1} end memory diff: APP START: {memoryAtAppStart:F0} MB - LOOP END: {memoryAfterDisposeInMb:F0} MB (^{appStartToLoopEndMemory:F0} MB OR ^{appStartToLoopEndMemoryPercentageGrowth:F0} %)");
                    Console.WriteLine(sb);
                }
            });
        }

        private static class ProcessMemoryProfiler
        {
            internal static long ReportInMb() => Process.GetCurrentProcess().WorkingSet64 / 1024 / 1024;
        }

        private static string GenerateRandomString(Random random, int length = 10) => new string(Enumerable.Repeat(alphabets, length).Select(s => s[random.Next(s.Length)]).ToArray());
    }
}
