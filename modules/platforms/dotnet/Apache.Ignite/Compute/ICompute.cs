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

namespace Apache.Ignite.Compute
{
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Network;

    /// <summary>
    /// Ignite Compute API provides distributed job execution functionality.
    /// </summary>
    public interface ICompute
    {
        /// <summary>
        /// Executes a compute job represented by the given class on one of the specified nodes.
        /// </summary>
        /// <param name="nodes">Nodes to use for the job execution.</param>
        /// <param name="jobClassName">Java class name of the job to execute.</param>
        /// <param name="args">Job arguments.</param>
        /// <typeparam name="T">Job result type.</typeparam>
        /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
        Task<T> ExecuteAsync<T>(IEnumerable<IClusterNode> nodes, string jobClassName, params object[] args);

        /// <summary>
        /// Executes a compute job represented by the given class on all of the specified nodes.
        /// </summary>
        /// <param name="nodes">Nodes to use for the job execution.</param>
        /// <param name="jobClassName">Java class name of the job to execute.</param>
        /// <param name="args">Job arguments.</param>
        /// <typeparam name="T">Job result type.</typeparam>
        /// <returns>A map of <see cref="Task"/> representing the asynchronous operation for every node.</returns>
        IDictionary<IClusterNode, Task<T>> BroadcastAsync<T>(IEnumerable<IClusterNode> nodes, string jobClassName, params object[] args);
    }
}
