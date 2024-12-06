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

namespace Apache.Ignite
{
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Compute;
    using Network;
    using Sql;
    using Table;
    using Transactions;

    /// <summary>
    /// Ignite API entry point.
    /// <para />
    /// All Ignite APIs are thread-safe, unless noted otherwise.
    /// <para />
    /// Implementation can be a thin client (see <see cref="IIgniteClient"/> and <see cref="IgniteClient.StartAsync"/>),
    /// or a direct IPC connection for server-side functionality like compute.
    /// </summary>
    public interface IIgnite
    {
        /// <summary>
        /// Gets the tables API.
        /// </summary>
        ITables Tables { get; }

        /// <summary>
        /// Gets the transactions API.
        /// </summary>
        ITransactions Transactions { get; }

        /// <summary>
        /// Gets the compute API.
        /// </summary>
        ICompute Compute { get; }

        /// <summary>
        /// Gets the SQL API.
        /// </summary>
        ISql Sql { get; }

        /// <summary>
        /// Gets the cluster nodes.
        /// NOTE: Temporary API to enable Compute until we have proper Cluster API.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
        Task<IList<IClusterNode>> GetClusterNodesAsync();
    }
}
