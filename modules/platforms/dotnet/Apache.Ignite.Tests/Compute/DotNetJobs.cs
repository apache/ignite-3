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

namespace Apache.Ignite.Tests.Compute;

using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Ignite.Compute;

[SuppressMessage("Design", "CA1034:Nested types should not be visible", Justification = "Tests.")]
public static class DotNetJobs
{
    // TODO IGNITE-25116: Remove.
    public const string TempJobPrefix = "TEST_ONLY_DOTNET_JOB:";

    public static readonly JobDescriptor<int, int> AddOne = GetDesc(new AddOneJob());
    public static readonly JobDescriptor<int, int> NoCtor = GetDesc(new NoCtorJob(1));
    public static readonly JobDescriptor<object?, object?> Echo = GetDesc(new EchoJob());
    public static readonly JobDescriptor<object?, object?> Error = GetDesc(new ErrorJob());

    private static JobDescriptor<TArg, TRes> GetDesc<TArg, TRes>(IComputeJob<TArg, TRes> job) =>
        new(TempJobPrefix + job.GetType().AssemblyQualifiedName!);

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
        public NoCtorJob(int ctorArg)
        {
            // No-op.
        }

        public ValueTask<int> ExecuteAsync(IJobExecutionContext context, int arg, CancellationToken cancellationToken) =>
            throw new NotImplementedException();
    }
}
