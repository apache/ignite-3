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

namespace Apache.Ignite.Tests.Common.Compute;

using System;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.Loader;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Apache.Ignite.Compute;

[SuppressMessage("Design", "CA1034:Nested types should not be visible", Justification = "Tests.")]
public static class DotNetJobs
{
    public static readonly JobDescriptor<int, int> AddOne = JobDescriptor.Of(new AddOneJob());
    public static readonly JobDescriptor<int, int> NoCtor = JobDescriptor.Of(new NoCtorJob(1));
    public static readonly JobDescriptor<object?, object?> Echo = JobDescriptor.Of(new EchoJob());
    public static readonly JobDescriptor<object?, object?> Error = new(typeof(ErrorJob));
    public static readonly JobDescriptor<object?, int> ProcessId = JobDescriptor.Of(new ProcessIdJob());
    public static readonly JobDescriptor<object?, object?> ProcessExit = JobDescriptor.Of(new ProcessExitJob());
    public static readonly JobDescriptor<string, string> ApiTest = new(typeof(ApiTestJob));
    public static readonly JobDescriptor<object?, int> AssemblyLoadContextCount = JobDescriptor.Of(new AssemblyLoadContextCountJob());

    public static readonly JobDescriptor<string, string> NewerDotNetJob = new(
        JobClassName: "NewerDotnetJobs.EchoJob, NewerDotnetJobs",
        Options: new JobExecutionOptions(ExecutorType: JobExecutorType.DotNetSidecar));

    public static async Task<string> WriteNewerDotnetJobsAssembly(string tempDirPath, string asmName)
    {
        var targetFile = Path.Combine(tempDirPath, asmName + ".dll");

        await using var fileStream = File.Create(targetFile);

        await Assembly.GetExecutingAssembly()
            .GetManifestResourceStream("Apache.Ignite.Tests.Compute.Executor.NewerDotnetJobs.NewerDotnetJobs.dll")!
            .CopyToAsync(fileStream);

        return targetFile;
    }

    public class AddOneJob : IComputeJob<int, int>
    {
        public ValueTask<int> ExecuteAsync(IJobExecutionContext context, int arg, CancellationToken cancellationToken) =>
            ValueTask.FromResult(arg + 1);
    }

    public class EchoJob : IComputeJob<object?, object?>
    {
        public ValueTask<object?> ExecuteAsync(IJobExecutionContext context, object? arg, CancellationToken cancellationToken) =>
            ValueTask.FromResult(arg);
    }

    public class ErrorJob : IComputeJob<object?, object?>
    {
        public async ValueTask<object?> ExecuteAsync(IJobExecutionContext context, object? arg, CancellationToken cancellationToken)
        {
            // Yield and throw from another method to check stack trace propagation.
            await Task.Yield();
            Throw(arg);
            return arg;
        }

        private static void Throw(object? arg) => throw new ArithmeticException("Test exception: " + arg);
    }

    public class NoCtorJob : IComputeJob<int, int>
    {
        [SuppressMessage("ReSharper", "UnusedParameter.Local", Justification = "Tests.")]
        public NoCtorJob(int ignore)
        {
            // No-op.
        }

        public ValueTask<int> ExecuteAsync(IJobExecutionContext context, int arg, CancellationToken cancellationToken) =>
            throw new NotImplementedException();
    }

    public class ProcessIdJob : IComputeJob<object?, int>
    {
        public ValueTask<int> ExecuteAsync(IJobExecutionContext context, object? arg, CancellationToken cancellationToken) =>
            ValueTask.FromResult(Environment.ProcessId);
    }

    public class ProcessExitJob : IComputeJob<object?, object?>
    {
        public ValueTask<object?> ExecuteAsync(IJobExecutionContext context, object? arg, CancellationToken cancellationToken)
        {
            Environment.Exit(1);
            return ValueTask.FromResult(arg);
        }
    }

    public class ApiTestJob : IComputeJob<string, string>
    {
        public async ValueTask<string> ExecuteAsync(IJobExecutionContext context, string arg, CancellationToken cancellationToken)
        {
            IIgnite ignite = context.Ignite;
            var sb = new StringBuilder();
            sb.Append($"Arg: {arg}|");

            await using var cursor = await ignite.Sql.ExecuteAsync(null, "select 42 as answer");
            await foreach (var row in cursor)
            {
                sb.Append($"SQL result: {row}|");
            }

            var table = await ignite.Tables.GetTableAsync("TBL1");
            var view = table!.GetKeyValueView<long, string>();

            await using var tx = await ignite.Transactions.BeginAsync();
            await view.PutAsync(tx, 1L, "Hello");
            var val = await view.GetAsync(tx, 1L);

            sb.Append($"Table result: {val}");

            return sb.ToString();
        }
    }

    public class AssemblyLoadContextCountJob : IComputeJob<object?, int>
    {
        public ValueTask<int> ExecuteAsync(IJobExecutionContext context, object? arg, CancellationToken cancellationToken)
        {
            GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced);
            GC.WaitForPendingFinalizers();

            return ValueTask.FromResult(AssemblyLoadContext.All.Count());
        }
    }
}
