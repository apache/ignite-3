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

using System.Collections.Generic;
using System.Threading.Tasks;
using Ignite.Compute;
using Ignite.Table;
using Network;

/// <summary>
/// Test extension methods for <see cref="ICompute"/>.
/// </summary>
public static class ComputeTestExtensions
{
    public static async Task<IJobExecution<T>> SubmitAsync<T>(
        this ICompute compute,
        IEnumerable<IClusterNode> nodes,
        IEnumerable<DeploymentUnit> units,
        string jobClassName,
        params object?[]? args) =>
        await compute.SubmitAsync<T>(nodes, units, jobClassName, JobExecutionOptions.Default, args);

    public static async Task<IJobExecution<T>> SubmitColocatedAsync<T>(
        this ICompute compute,
        string tableName,
        IIgniteTuple key,
        IEnumerable<DeploymentUnit> units,
        string jobClassName,
        params object?[]? args) =>
        await compute.SubmitColocatedAsync<T>(tableName, key, units, jobClassName, JobExecutionOptions.Default, args);

    public static async Task<IJobExecution<T>> SubmitColocatedAsync<T, TKey>(
        this ICompute compute,
        string tableName,
        TKey key,
        IEnumerable<DeploymentUnit> units,
        string jobClassName,
        params object?[]? args)
        where TKey : notnull =>
        await compute.SubmitColocatedAsync<T, TKey>(tableName, key, units, jobClassName, JobExecutionOptions.Default, args);

    public static IDictionary<IClusterNode, Task<IJobExecution<T>>> SubmitBroadcast<T>(
        this ICompute compute,
        IEnumerable<IClusterNode> nodes,
        IEnumerable<DeploymentUnit> units,
        string jobClassName,
        params object?[]? args) =>
        compute.SubmitBroadcast<T>(nodes, units, jobClassName, JobExecutionOptions.Default, args);
}
