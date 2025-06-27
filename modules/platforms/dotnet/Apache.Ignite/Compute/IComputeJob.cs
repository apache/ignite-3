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

namespace Apache.Ignite.Compute;

using System.Threading;
using System.Threading.Tasks;
using Marshalling;

/// <summary>
/// Ignite compute job interface.
/// <para />
/// To define a compute job, implement this interface and deploy the binaries to the cluster with the Deployment API.
/// </summary>
/// <typeparam name="TArg">Argument type.</typeparam>
/// <typeparam name="TResult">Result type.</typeparam>
public interface IComputeJob<TArg, TResult>
{
    /// <summary>
    /// Gets the custom marshaller for the job input argument.
    /// </summary>
    IMarshaller<TArg>? InputMarshaller => null;

    /// <summary>
    /// Gets the custom marshaller for the job result.
    /// </summary>
    IMarshaller<TResult>? ResultMarshaller => null;

    /// <summary>
    /// Executes the job.
    /// </summary>
    /// <param name="context">Job execution context.</param>
    /// <param name="arg">Job argument.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Job result.</returns>
    ValueTask<TResult> ExecuteAsync(IJobExecutionContext context, TArg arg, CancellationToken cancellationToken);
}
