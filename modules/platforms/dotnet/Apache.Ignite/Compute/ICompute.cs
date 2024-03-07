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
using Table;

/// <summary>
/// Ignite Compute API provides distributed job execution functionality.
/// </summary>
public interface ICompute
{
    /// <summary>
    /// Submits a compute job represented by the given class for an execution on one of the specified nodes.
    /// </summary>
    /// <param name="nodes">Nodes to use for the job execution.</param>
    /// <param name="units">Deployment units. Can be empty.</param>
    /// <param name="jobClassName">Java class name of the job to execute.</param>
    /// <param name="options">Job execution options.</param>
    /// <param name="args">Job arguments.</param>
    /// <typeparam name="T">Job result type.</typeparam>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    Task<IJobExecution<T>> SubmitAsync<T>(
        IEnumerable<IClusterNode> nodes,
        IEnumerable<DeploymentUnit> units,
        string jobClassName,
        JobExecutionOptions options,
        params object?[]? args);

    /// <summary>
    /// Submits a compute job represented by the given class for an execution on one of the nodes where the given key is located.
    /// </summary>
    /// <param name="tableName">Name of the table to be used with <paramref name="key"/> to determine target node.</param>
    /// <param name="key">Table key to be used to determine the target node for job execution.</param>
    /// <param name="units">Deployment units. Can be empty.</param>
    /// <param name="jobClassName">Java class name of the job to execute.</param>
    /// <param name="options">Job execution options.</param>
    /// <param name="args">Job arguments.</param>
    /// <typeparam name="T">Job result type.</typeparam>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    Task<IJobExecution<T>> SubmitColocatedAsync<T>(
        string tableName,
        IIgniteTuple key,
        IEnumerable<DeploymentUnit> units,
        string jobClassName,
        JobExecutionOptions options,
        params object?[]? args);

    /// <summary>
    /// Submits a compute job represented by the given class for an execution on one of the nodes where the given key is located.
    /// </summary>
    /// <param name="tableName">Name of the table to be used with <paramref name="key"/> to determine target node.</param>
    /// <param name="key">Table key to be used to determine the target node for job execution.</param>
    /// <param name="units">Deployment units. Can be empty.</param>
    /// <param name="jobClassName">Java class name of the job to execute.</param>
    /// <param name="options">Job execution options.</param>
    /// <param name="args">Job arguments.</param>
    /// <typeparam name="T">Job result type.</typeparam>
    /// <typeparam name="TKey">Key type.</typeparam>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    Task<IJobExecution<T>> SubmitColocatedAsync<T, TKey>(
        string tableName,
        TKey key,
        IEnumerable<DeploymentUnit> units,
        string jobClassName,
        JobExecutionOptions options,
        params object?[]? args)
        where TKey : notnull;

    /// <summary>
    /// Submits a compute job represented by the given class for an execution on all of the specified nodes.
    /// </summary>
    /// <param name="nodes">Nodes to use for the job execution.</param>
    /// <param name="units">Deployment units. Can be empty.</param>
    /// <param name="jobClassName">Java class name of the job to execute.</param>
    /// <param name="options">Job execution options.</param>
    /// <param name="args">Job arguments.</param>
    /// <typeparam name="T">Job result type.</typeparam>
    /// <returns>A map of <see cref="Task"/> representing the asynchronous operation for every node.</returns>
    IDictionary<IClusterNode, Task<IJobExecution<T>>> SubmitBroadcast<T>(
        IEnumerable<IClusterNode> nodes,
        IEnumerable<DeploymentUnit> units,
        string jobClassName,
        JobExecutionOptions options,
        params object?[]? args);
}
