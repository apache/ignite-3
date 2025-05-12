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

namespace Apache.Ignite.Internal.Table.StreamerReceiverExecutor;

using System;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Apache.Ignite.Internal.Buffers;
using Apache.Ignite.Internal.Compute;
using Apache.Ignite.Internal.Compute.Executor;
using Apache.Ignite.Internal.Proto.BinaryTuple;
using Apache.Ignite.Table;

/// <summary>
/// Internal compute job that executes user-defined data streamer receiver.
/// </summary>
[SuppressMessage("ReSharper", "UnusedType.Global", Justification = "Called via reflection from Java.")]
internal static class StreamerReceiverJob
{
    /// <summary>
    /// Executes the receiver job.
    /// </summary>
    /// <param name="argBuf">Arg buffer.</param>
    /// <param name="resBuf">Result buffer.</param>
    /// <param name="receiverContext">Receiver context.</param>
    /// <param name="jobLoadContext">Load context.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Task.</returns>
    public static async ValueTask ExecuteJobAsync(
        PooledBuffer argBuf,
        PooledArrayBuffer resBuf,
        IDataStreamerReceiverContext receiverContext,
        JobLoadContext jobLoadContext,
        CancellationToken cancellationToken)
    {
        var receiverTypeName = GetReceiverInfoReaderFast(argBuf).GetString(0);

        IDataStreamerReceiverWrapper receiverWrapper = jobLoadContext.CreateReceiverWrapper(receiverTypeName);

        await receiverWrapper.ExecuteAsync(receiverContext, argBuf, resBuf, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Unwraps receiver info from the job argument buffer.
    /// Performs simple offset calculations and can be called multiple times.
    /// </summary>
    /// <param name="jobArgBuf">Job argument buffer.</param>
    /// <returns>Binary tuple reader with streamer receiver info.</returns>
    public static BinaryTupleReader GetReceiverInfoReaderFast(PooledBuffer jobArgBuf)
    {
        var r = jobArgBuf.GetReader();

        // Excerpt from ComputePacker.
        int argType = r.ReadInt32();
        Debug.Assert(argType == ComputePacker.Native, $"Expected Native type, got: {argType}");

        // Excerpt from ReadObjectFromBinaryTuple.
        ReadOnlySpan<byte> tupleSpan = r.ReadBinary();
        var binTuple = new BinaryTupleReader(tupleSpan, 3);

        ReadOnlySpan<byte> receiverInfoSpan = binTuple.GetBytesSpan(2);
        int receiverElementCount = BinaryPrimitives.ReadInt32LittleEndian(receiverInfoSpan);

        return new BinaryTupleReader(receiverInfoSpan[4..], receiverElementCount);
    }
}
