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

namespace Apache.Ignite.Internal.Compute.Executor;

using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Buffers;
using Ignite.Compute;

/// <summary>
/// Non-generic job interface to cross the genericity boundary.
/// </summary>
internal interface IComputeJobWrapper
{
    /// <summary>
    /// Executes the job.
    /// </summary>
    /// <param name="context">Job execution context.</param>
    /// <param name="argBuf">The input buffer containing a job argument.</param>
    /// <param name="responseBuf">The output buffer for storing job execution results.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Job result.</returns>
    [SuppressMessage("Naming", "CA1711:Identifiers should not have incorrect suffix", Justification = "Reviewed.")]
    ValueTask ExecuteAsync(
        IJobExecutionContext context,
        PooledBuffer argBuf,
        PooledArrayBuffer responseBuf,
        CancellationToken cancellationToken);
}
