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
using Internal.Compute.Executor;

[MemoryDiagnoser]
public class JobLoadContextBenchmarks
{
    [Benchmark]
    public object CreateJobWrapper()
    {
        var ctx = new JobLoadContext(AssemblyLoadContext.Default);

        return ctx.CreateJobWrapper(typeof(AddOneJob).AssemblyQualifiedName!);
    }

    private sealed class AddOneJob : IComputeJob<int, int>
    {
        public ValueTask<int> ExecuteAsync(IJobExecutionContext context, int arg, CancellationToken cancellationToken) =>
            ValueTask.FromResult(arg + 1);
    }
}
