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

using System.Threading;
using System.Threading.Tasks;
using Buffers;
using Compute.Executor;
using Ignite.Table;

/// <summary>
/// Non-generic receiver interface to cross the genericity boundary.
/// See also <see cref="IComputeJobWrapper"/>.
/// </summary>
internal interface IDataStreamerReceiverWrapper
{
    /// <summary>
    /// Executes the receiver.
    /// </summary>
    /// <param name="context">Receiver context.</param>
    /// <param name="requestBuf">The input buffer containing a job argument.</param>
    /// <param name="responseBuf">The output buffer for storing job execution results.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Job result.</returns>
    ValueTask ExecuteAsync(
        IDataStreamerReceiverContext context,
        PooledBuffer requestBuf,
        PooledArrayBuffer responseBuf,
        CancellationToken cancellationToken);
}
