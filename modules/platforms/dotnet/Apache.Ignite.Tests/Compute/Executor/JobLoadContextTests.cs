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

namespace Apache.Ignite.Tests.Compute.Executor;

using System;
using System.Collections.Concurrent;
using System.Reflection;
using System.Runtime.Loader;
using System.Threading;
using System.Threading.Tasks;
using Ignite.Compute;
using Internal.Compute.Executor;
using NUnit.Framework;

/// <summary>
/// Tests for <see cref="JobLoadContext"/>.
/// </summary>
public class JobLoadContextTests
{
    private static readonly ConcurrentDictionary<Guid, string> DisposedJobStates = new();

    [SetUp]
    public void SetUp() => DisposedJobStates.Clear();

    [Test]
    public async Task TestJobExecution()
    {
        var res = await ExecuteJobAsync(DotNetJobs.AddOne, 1);

        Assert.AreEqual(2, res);
    }

    [Test]
    public async Task TestDisposableJob([Values(true, false)] bool async)
    {
        var jobType = async ? typeof(AsyncDisposableJob) : typeof(DisposableJob);
        var jobDesc = new JobDescriptor<object, Guid>(jobType.AssemblyQualifiedName!);
        var expectedState = async ? "InitializedExecutingExecutedAsyncDisposed" : "InitializedExecutedDisposed";

        var execId = await ExecuteJobAsync(jobDesc, null);

        Assert.IsTrue(DisposedJobStates.TryRemove(execId, out var state));
        Assert.AreEqual(expectedState, state);
    }

    [Test]
    public void TestJobWithoutDefaultConstructorThrows()
    {
        var ex = Assert.ThrowsAsync<InvalidOperationException>(async () => await ExecuteJobAsync(DotNetJobs.NoCtor, 1));

        Assert.AreEqual($"No public parameterless constructor for job type '{typeof(DotNetJobs.NoCtorJob).AssemblyQualifiedName}'", ex.Message);
    }

    [Test]
    public void TestCreateJobWrapperWithMultipleJobInterfacesThrows()
    {
        var jobLoadCtx = new JobLoadContext(AssemblyLoadContext.Default);

        var ex = Assert.Throws<AmbiguousMatchException>(
            () => jobLoadCtx.CreateJobWrapper(typeof(MultiInterfaceJob).AssemblyQualifiedName!));

        Assert.AreEqual("Ambiguous match found for ' Apache.Ignite.Compute.IComputeJob`2[System.Object,System.Guid]'.", ex.Message);
    }

    private static async Task<TResult> ExecuteJobAsync<TArg, TResult>(JobDescriptor<TArg, TResult> job, TArg? jobArg)
    {
        var jobLoadCtx = new JobLoadContext(AssemblyLoadContext.Default);
        var jobWrapper = jobLoadCtx.CreateJobWrapper(job.JobClassName);

        return await JobWrapperHelper.ExecuteAsync<TArg, TResult>(jobWrapper, jobArg);
    }

    private class DisposableJob : IComputeJob<object, Guid>, IDisposable
    {
        private readonly Guid _id = Guid.NewGuid();

        private string _state;

        public DisposableJob() => _state = "Initialized";

        public ValueTask<Guid> ExecuteAsync(IJobExecutionContext context, object arg, CancellationToken cancellationToken)
        {
            _state += "Executed";
            return ValueTask.FromResult(_id);
        }

        public void Dispose() => DisposedJobStates[_id] = _state + "Disposed";
    }

    private class AsyncDisposableJob : IComputeJob<object, Guid>, IAsyncDisposable
    {
        private readonly Guid _id = Guid.NewGuid();

        private string _state;

        public AsyncDisposableJob() => _state = "Initialized";

        public async ValueTask<Guid> ExecuteAsync(IJobExecutionContext context, object arg, CancellationToken cancellationToken)
        {
            _state += "Executing";
            await Task.Delay(1, cancellationToken);
            _state += "Executed";

            return _id;
        }

        public async ValueTask DisposeAsync()
        {
            await Task.Delay(1);
            DisposedJobStates[_id] = _state + "AsyncDisposed";
        }
    }

    private class MultiInterfaceJob : IComputeJob<object, Guid>, IComputeJob<int, string>
    {
        public ValueTask<Guid> ExecuteAsync(IJobExecutionContext context, object arg, CancellationToken cancellationToken) =>
            ValueTask.FromResult(Guid.Empty);

        public ValueTask<string> ExecuteAsync(IJobExecutionContext context, int arg, CancellationToken cancellationToken) =>
            ValueTask.FromResult("x");
    }
}
