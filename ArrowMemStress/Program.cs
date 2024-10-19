﻿namespace ArrowMemStress
{
    using Apache.Arrow;
    using Apache.Arrow.Memory;
    using Microsoft.Data.Analysis;
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

            var threadColumn = new PrimitiveDataFrameColumn<int>("Thread #");
            var loopColumn = new PrimitiveDataFrameColumn<int>("Loop #");
            var numRowsColumn = new PrimitiveDataFrameColumn<int>("Number of Rows");
            var memoryBeforeRecordBatchCreateColumn = new PrimitiveDataFrameColumn<long>("Memory Before RecordBatch Create");
            var managedHeapBeforeRecordBatchCreateColumn = new PrimitiveDataFrameColumn<long>("Managed Heap Before RecordBatch Create");
            var memoryAfterRecordBatchCreateColumn = new PrimitiveDataFrameColumn<long>("Memory After RecordBatch Create");
            var managedHeapAfterRecordBatchCreateColumn = new PrimitiveDataFrameColumn<long>("Managed Heap After RecordBatch Create");
            var memoryAfterRecordBatchDisposeColumn = new PrimitiveDataFrameColumn<long>("Memory After RecordBatch Dispose");
            var managedHeapAfterRecordBatchDisposeColumn = new PrimitiveDataFrameColumn<long>("Managed Heap After RecordBatch Dispose");
            var memoryAfterGcColumn = new PrimitiveDataFrameColumn<long>("Memory After GC");
            var managedHeapAfterGcColumn = new PrimitiveDataFrameColumn<long>("Managed Heap After GC");
            var appStartToLoopEndMemoryColumn = new PrimitiveDataFrameColumn<long>("Memory Start and End Diff");
            var appStartToLoopEndManagedHeapColumn = new PrimitiveDataFrameColumn<long>("Managed Heap Start and End Diff");
            var appStartToLoopEndMemoryPercentColumn = new PrimitiveDataFrameColumn<long>("Memory Start and End %");
            var appStartToLoopEndManagedHeapPercentColumn = new PrimitiveDataFrameColumn<long>("Managed Heap Start and End %");

            Random randomValueGenerator = new ();
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
    }
}
