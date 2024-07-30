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

using System.Collections.Generic;
using System.Threading.Tasks;
using Network;

/// <summary>
/// Ignite Compute API provides distributed job execution functionality.
/// </summary>
public interface ICompute
{
    /// <summary>
    /// Submits a compute job represented by the given class for an execution on one of the specified nodes.
    /// </summary>
    /// <param name="target">Job execution target.</param>
    /// <param name="jobDescriptor">Job descriptor.</param>
    /// <param name="arg">Job argument.</param>
    /// <typeparam name="TTarget">Job target type.</typeparam>
    /// <typeparam name="TArg">Job argument type.</typeparam>
    /// <typeparam name="TResult">Job result type.</typeparam>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    Task<IJobExecution<TResult>> SubmitAsync<TTarget, TArg, TResult>(
        IJobTarget<TTarget> target,
        JobDescriptor<TArg, TResult> jobDescriptor,
        TArg arg)
        where TTarget : notnull;

    /// <summary>
    /// Submits a compute job represented by the given class for an execution on all of the specified nodes.
    /// </summary>
    /// <param name="nodes">Nodes to use for the job execution.</param>
    /// <param name="jobDescriptor">Job descriptor.</param>
    /// <param name="arg">Job argument.</param>
    /// <typeparam name="TArg">Job argument type.</typeparam>
    /// <typeparam name="TResult">Job result type.</typeparam>
    /// <returns>A map of <see cref="Task"/> representing the asynchronous operation for every node.</returns>
    IDictionary<IClusterNode, Task<IJobExecution<TResult>>> SubmitBroadcast<TArg, TResult>(
        IEnumerable<IClusterNode> nodes,
        JobDescriptor<TArg, TResult> jobDescriptor,
        TArg arg);

    /// <summary>
    /// Submits a compute map-reduce task represented by the given class.
    /// </summary>
    /// <param name="taskDescriptor">Task descriptor.</param>
    /// <param name="arg">Job arguments.</param>
    /// <typeparam name="TArg">Task argument type.</typeparam>
    /// <typeparam name="TResult">Task result type.</typeparam>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    Task<ITaskExecution<TResult>> SubmitMapReduceAsync<TArg, TResult>(
        TaskDescriptor<TArg, TResult> taskDescriptor,
        TArg arg);
}
