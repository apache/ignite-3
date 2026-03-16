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

namespace Apache.Ignite.Internal
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Common;
    using Ignite.Compute;
    using Ignite.Network;
    using Ignite.Sql;
    using Ignite.Table;
    using Ignite.Transactions;
    using Network;
    using Proto;
    using Table;

    /// <summary>
    /// Ignite client implementation.
    /// </summary>
    internal sealed class IgniteClientInternal : IIgniteClient
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteClientInternal"/> class.
        /// </summary>
        /// <param name="socket">Socket.</param>
        public IgniteClientInternal(ClientFailoverSocket socket)
        {
            Socket = socket;

            // TODO IGNITE-27846 Extract TableCache and avoid circular dependency.
            var tables = new Tables(socket);
            var sql = new Sql.Sql(socket, tables);
            tables.Sql = sql;

            Tables = tables;
            Transactions = new Transactions.Transactions(socket);
            Compute = new Compute.Compute(socket, tables);
            Sql = sql;
        }

        /// <inheritdoc/>
        public IgniteClientConfiguration Configuration =>
            new(Socket.Configuration.Configuration); // Defensive copy.

        /// <inheritdoc/>
        public ITables Tables { get; }

        /// <inheritdoc/>
        public ITransactions Transactions { get; }

        /// <inheritdoc/>
        public ICompute Compute { get; }

        /// <inheritdoc/>
        public ISql Sql { get; }

        /// <summary>
        /// Gets a value indicating whether the client is disposed.
        /// </summary>
        public bool IsDisposed => Socket.IsDisposed;

        /// <summary>
        /// Gets the underlying socket.
        /// </summary>
        internal ClientFailoverSocket Socket { get; }

        /// <inheritdoc/>
        public async Task<IList<IClusterNode>> GetClusterNodesAsync()
        {
            using var resBuf = await Socket.DoOutInOpAsync(ClientOp.ClusterGetNodes).ConfigureAwait(false);

            return Read();

            IList<IClusterNode> Read()
            {
                var r = resBuf.GetReader();
                var count = r.ReadInt32();
                var res = new List<IClusterNode>(count);

                for (var i = 0; i < count; i++)
                {
                    res.Add(ClusterNode.Read(ref r));
                }

                return res;
            }
        }

        /// <inheritdoc/>
        public IList<IConnectionInfo> GetConnections() => Socket.GetConnections();

        /// <inheritdoc/>
        public void Dispose() => Socket.Dispose();

        /// <inheritdoc/>
        public override string ToString() =>
            new IgniteToStringBuilder(GetType())
                .AppendList(GetConnections().Select(c => c.Node), "Connections")
                .Build();
    }
}
