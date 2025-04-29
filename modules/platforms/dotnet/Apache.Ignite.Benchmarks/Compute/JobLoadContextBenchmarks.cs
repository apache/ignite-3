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

using System.Runtime.Loader;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Ignite.Compute;
using Internal.Buffers;
using Internal.Compute.Executor;
using Internal.Proto.MsgPack;

/// <summary>
/// Results on i9-12900H, .NET SDK 8.0.15, Ubuntu 22.04:
///
/// | Method           | Mean        | Error     | StdDev    | Allocated |
/// |----------------- |------------:|----------:|----------:|----------:|
/// | CreateJobWrapper | 2,108.30 ns | 27.510 ns | 25.733 ns |    1720 B | (dominated by GetType)
/// | ExecuteJob       |    60.21 ns |  0.570 ns |  0.506 ns |      24 B |.
/// </summary>
[MemoryDiagnoser]
public class JobLoadContextBenchmarks
{
    private static readonly PooledBuffer ArgBuf = new([MsgPackCode.Nil], 0, 1);

    private static readonly PooledArrayBuffer ResBuf = new();

    private static readonly IComputeJobWrapper JobWrapper =
        new JobLoadContext(AssemblyLoadContext.Default).CreateJobWrapper(typeof(EmptyJob).AssemblyQualifiedName!);

    [Benchmark]
    public object CreateJobWrapper()
    {
        var ctx = new JobLoadContext(AssemblyLoadContext.Default);

        return ctx.CreateJobWrapper(typeof(EmptyJob).AssemblyQualifiedName!);
    }

    [Benchmark]
    public async ValueTask ExecuteJob()
    {
        ResBuf.Position = 0;
        await JobWrapper.ExecuteAsync(null!, ArgBuf, ResBuf, CancellationToken.None);
    }

    private sealed class EmptyJob : IComputeJob<object, object>
    {
        public ValueTask<object> ExecuteAsync(IJobExecutionContext context, object arg, CancellationToken cancellationToken) =>
            ValueTask.FromResult(arg);
    }
}
