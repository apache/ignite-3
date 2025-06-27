/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Benchmarks.Compute;

using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Internal.Buffers;
using Internal.Compute.Executor;
using Tests.Compute.Executor;
using Tests.TestHelpers;

/// <summary>
/// Measures the performance of assembly loading and job execution.
/// Results on i9-12900H, .NET SDK 8.0.15, Ubuntu 22.04:
///
/// | Method          | Mean     | Error   | StdDev  | Gen0   | Allocated |
/// |---------------- |---------:|--------:|--------:|-------:|----------:|
/// | ExecuteJobAsync | 412.7 ns | 3.36 ns | 2.81 ns | 0.0014 |     464 B |.
/// </summary>
[MemoryDiagnoser]
public class ComputeJobExecutorBenchmarks
{
    private static readonly PooledArrayBuffer ResBuf = new();

    private PooledBuffer _jobBuf = null!;

    private TempDir _tempDir = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _tempDir = new TempDir();
        var asmName = nameof(ComputeJobExecutorBenchmarks);
        JobGenerator.EmitEchoJob(_tempDir, asmName);

        using var jobBuf = new PooledArrayBuffer();
        WriteJob(jobBuf);
        var written = jobBuf.GetWrittenMemory();
        var arr = written.ToArray();
        _jobBuf = new PooledBuffer(arr, 0, arr.Length);

        void WriteJob(PooledArrayBuffer b)
        {
            var w = b.MessageWriter;
            w.Write(0); // Job id.
            w.Write($"TestNamespace.EchoJob, {asmName}");
            w.Write(1); // Deployment unit count.
            w.Write(_tempDir.Path);
            w.Write(false); // Retain units.
            w.WriteNil(); // Arg.
        }
    }

    [GlobalCleanup]
    public void GlobalCleanup() => _tempDir.Dispose();

    [Benchmark]
    public async Task ExecuteJobAsync()
    {
        _jobBuf.Position = 0;
        ResBuf.Position = 0;
        await ComputeJobExecutor.ExecuteJobAsync(_jobBuf, ResBuf, null!);
    }
}
