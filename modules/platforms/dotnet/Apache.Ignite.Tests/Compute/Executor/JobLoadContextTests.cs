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
using System.Runtime.Loader;
using System.Threading;
using System.Threading.Tasks;
using Ignite.Compute;
using Internal.Buffers;
using Internal.Compute;
using Internal.Compute.Executor;
using Internal.Proto.MsgPack;
using NUnit.Framework;

/// <summary>
/// Tests for <see cref="JobLoadContext"/>.
/// </summary>
public class JobLoadContextTests
{
    [Test]
    public async Task TestJobExecution()
    {
        var jobLoadCtx = new JobLoadContext(AssemblyLoadContext.Default);
        var jobTypeName = typeof(AddOneJob).AssemblyQualifiedName!;
        var jobWrapper = jobLoadCtx.GetOrCreateJobWrapper(jobTypeName);

        using var argBuf = PackArg(1);
        using var resBuf = new PooledArrayBuffer();

        await jobWrapper.ExecuteAsync(null!, argBuf, resBuf, CancellationToken.None);

        var res = UnpackRes<int>(resBuf);
        Assert.AreEqual(2, res);
    }

    private static PooledBuffer PackArg<T>(T arg)
    {
        using var buffer = new PooledArrayBuffer();
        var writer = buffer.MessageWriter;
        ComputePacker.PackArgOrResult(ref writer, arg, null);

        var mem = buffer.GetWrittenMemory();
        var arr = ByteArrayPool.Rent(mem.Length);
        mem.Span.CopyTo(arr);

        return new PooledBuffer(arr, 0, mem.Length);
    }

    private static T UnpackRes<T>(PooledArrayBuffer buf)
    {
        var reader = new MsgPackReader(buf.GetWrittenMemory().Span);

        return ComputePacker.UnpackArgOrResult<T>(ref reader, null);
    }

    private class AddOneJob : IComputeJob<int, int>
    {
        public ValueTask<int> ExecuteAsync(IJobExecutionContext context, int arg, CancellationToken cancellationToken) =>
            ValueTask.FromResult(arg + 1);
    }

    private class DisposableJob : IComputeJob<Guid, Guid>, IDisposable
    {
        public static readonly ConcurrentDictionary<Guid, Guid> DisposedIds = new();

        private Guid _arg;

        public ValueTask<Guid> ExecuteAsync(IJobExecutionContext context, Guid arg, CancellationToken cancellationToken)
        {
            _arg = arg;
            return ValueTask.FromResult(arg);
        }

        public void Dispose() => DisposedIds[_arg] = _arg;
    }
}
