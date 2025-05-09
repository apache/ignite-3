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

namespace Apache.Ignite.Internal.Table;

using System;
using System.Buffers.Binary;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Ignite.Compute;
using Proto.BinaryTuple;

/// <summary>
/// Internal compute job that executes user-defined data streamer receiver.
/// </summary>
[SuppressMessage("ReSharper", "UnusedType.Global", Justification = "Called via reflection from Java.")]
internal sealed class StreamerReceiverJob : IComputeJob<byte[], byte[]>
{
    /// <inheritdoc/>
    public async ValueTask<byte[]> ExecuteAsync(IJobExecutionContext context, byte[] arg, CancellationToken cancellationToken)
    {
        await Task.Delay(1, cancellationToken).ConfigureAwait(false);
        var payloadElementCount = BinaryPrimitives.ReadInt32LittleEndian(arg);
        var reader = new BinaryTupleReader(arg.AsSpan()[4..], payloadElementCount);

        // TODO: Implement
        return [];
    }
}
