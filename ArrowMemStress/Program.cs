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
            long managedHeapAtAppStart = ProcessMemoryProfiler.ReportManagedHeapLiveObjectsInMb();

            Console.WriteLine($"[ App Start ] App: {memoryAtAppStart:F0} MB, Managed Heap: {managedHeapAtAppStart:F0} MB");

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
                    string[] stringArray = Enumerable.Range(0, numRows).Select(_ => GenerateRandomString(randomValueGenerator, stringLength)).ToArray();

                    long memoryBeforeRecordBatchCreate = ProcessMemoryProfiler.ReportInMb();
                    long managedHeapBeforeRecordBatchCreate = ProcessMemoryProfiler.ReportManagedHeapLiveObjectsInMb();

                    RecordBatch.Builder recordBatchBuilder = new RecordBatch.Builder(memoryAllocator).Append(stringColumnName, false, col => col.String(arr => arr.AppendRange(stringArray)));
                    RecordBatch[] outgoingBatches = new RecordBatch[] { recordBatchBuilder.Build() };

                    // 1. After creating RecordBatch
                    long memoryAfterRecordBatchCreate = ProcessMemoryProfiler.ReportInMb();
                    long managedHeapAfterRecordBatchCreate = ProcessMemoryProfiler.ReportManagedHeapLiveObjectsInMb();

                    // 2. After disposing RecordBatch
                    foreach (RecordBatch recordBatch in outgoingBatches) recordBatch.Dispose();
                    recordBatchBuilder.Clear();
                    stringArray = null;

                    long memoryAfterDisposeInMb = ProcessMemoryProfiler.ReportInMb();
                    long managedHeapAfterDisposeInMb = ProcessMemoryProfiler.ReportManagedHeapLiveObjectsInMb();

                    // 3. After forcing GC
                    GC.Collect(generation: GC.MaxGeneration, mode: GCCollectionMode.Aggressive, blocking: true, compacting: true);

                    long memoryAfterGcInMb = ProcessMemoryProfiler.ReportInMb();
                    long managedHeapAfterGcInMb = ProcessMemoryProfiler.ReportManagedHeapLiveObjectsInMb();

                    // Diffs
                    long memoryRecordBatchActual = memoryAfterRecordBatchCreate - memoryBeforeRecordBatchCreate;
                    long managedHeapRecordBatchActual = managedHeapAfterRecordBatchCreate - managedHeapBeforeRecordBatchCreate;

                    long disposedMemoryInMb = memoryAfterDisposeInMb - memoryAfterRecordBatchCreate;
                    long disposedManagedHeapInMb = managedHeapAfterDisposeInMb - managedHeapAfterRecordBatchCreate;

                    long memorySavedAfterGc = memoryAfterGcInMb - memoryAfterDisposeInMb;
                    long managedHeapSavedAfterGc = managedHeapAfterDisposeInMb - managedHeapAfterGcInMb;

                    long appStartToLoopEndMemory = memoryAfterGcInMb - memoryBeforeRecordBatchCreate;
                    long appStartToLoopEndManagedHeap = managedHeapAfterGcInMb - managedHeapBeforeRecordBatchCreate;

                    long appStartToLoopEndMemoryPercentageGrowth = (appStartToLoopEndMemory * 100 / memoryBeforeRecordBatchCreate);
                    long appStartToLoopEndManagedHeapPercentageGrowth = (appStartToLoopEndManagedHeap * 100 / managedHeapBeforeRecordBatchCreate);

                    long appStartToLoopEndNativeHeapGrowth = appStartToLoopEndMemory - appStartToLoopEndManagedHeap;

                    StringBuilder sb = new StringBuilder();
                    sb.Append($"[ Threads: {t + 1}/{numThreads}, Loops: {i + 1} of {numLoops} ] {numRows} rows, ");
                    sb.Append($"before RecordBatch create - App: {memoryBeforeRecordBatchCreate:F0} MB, Managed Heap: {managedHeapBeforeRecordBatchCreate} MB -> ");
                    sb.Append($"after RecordBatch create - App: {memoryAfterRecordBatchCreate:F0} MB (^{memoryRecordBatchActual:F0} MB), Managed Heap: {managedHeapAfterRecordBatchCreate} MB (^{managedHeapRecordBatchActual:F0} MB) -> ");
                    sb.Append($"after RecordBatch dispose - App: {memoryAfterDisposeInMb:F0} MB (^{disposedMemoryInMb:F0} MB), Managed Heap: {managedHeapAfterDisposeInMb} MB (^{disposedManagedHeapInMb:F0} MB) -> ");
                    sb.Append($"after GC - App: {memoryAfterGcInMb:F0} MB (^{memorySavedAfterGc:F0} MB), Managed Heap: {managedHeapAfterGcInMb} MB (^{managedHeapSavedAfterGc:F0} MB) -> ");
                    sb.Append($"final App memory diff - Start: {memoryBeforeRecordBatchCreate:F0} MB, End: {memoryAfterGcInMb:F0} MB -> ");
                    sb.Append($"(^{appStartToLoopEndMemory:F0} MB OR ^{appStartToLoopEndMemoryPercentageGrowth:F0} %) ");
                    sb.Append($"final managed heap diff - Start: {memoryBeforeRecordBatchCreate:F0} MB, End: {managedHeapAfterGcInMb:F0} MB -> ");
                    sb.Append($"(^{appStartToLoopEndManagedHeap:F0} MB OR ^{appStartToLoopEndManagedHeapPercentageGrowth:F0} %) -> ");
                    sb.Append($"final native heap diff: {appStartToLoopEndNativeHeapGrowth:F0} MB");
                    Console.WriteLine(sb);
                }
            });
        }

        private static class ProcessMemoryProfiler
        {
            internal static long ReportInMb() => Process.GetCurrentProcess().WorkingSet64 / 1024 / 1024;
            internal static long ReportManagedHeapLiveObjectsInMb() => GC.GetTotalMemory(false) / 1024 / 1024;
        }

        private static string GenerateRandomString(Random random, int length = 10) => new string(Enumerable.Repeat(alphabets, length).Select(s => s[random.Next(s.Length)]).ToArray());
    }
}
