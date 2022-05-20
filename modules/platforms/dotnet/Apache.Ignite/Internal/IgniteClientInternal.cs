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
    using System.Net;
    using System.Threading.Tasks;
    using Ignite.Compute;
    using Ignite.Network;
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
        /** Underlying connection. */
        private readonly ClientFailoverSocket _socket;

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteClientInternal"/> class.
        /// </summary>
        /// <param name="socket">Socket.</param>
        public IgniteClientInternal(ClientFailoverSocket socket)
        {
            _socket = socket;

            var tables = new Tables(socket);
            Tables = tables;

            Transactions = new Transactions.Transactions(socket);

            Compute = new Compute.Compute(socket, tables);
        }

        /// <inheritdoc/>
        public IgniteClientConfiguration Configuration =>
            new(_socket.Configuration); // Defensive copy.

        /// <inheritdoc/>
        public ITables Tables { get; }

        /// <inheritdoc/>
        public ITransactions Transactions { get; }

        /// <inheritdoc/>
        public ICompute Compute { get; }

        /// <inheritdoc/>
        public async Task<IList<IClusterNode>> GetClusterNodesAsync()
        {
            using var resBuf = await _socket.DoOutInOpAsync(ClientOp.ClusterGetNodes).ConfigureAwait(false);

            return Read();

            IList<IClusterNode> Read()
            {
                var r = resBuf.GetReader();
                var count = r.ReadArrayHeader();
                var res = new List<IClusterNode>(count);

                for (var i = 0; i < count; i++)
                {
                    res.Add(new ClusterNode(
                        Id: r.ReadString(),
                        Name: r.ReadString(),
                        Address: new IPEndPoint(IPAddress.Parse(r.ReadString()), r.ReadInt32())));
                }

                return res;
            }
        }

        /// <inheritdoc/>
        public IList<IClusterNode> GetConnections() =>
            _socket.GetConnections().Select(ctx => ctx.ClusterNode).ToList();

        /// <inheritdoc/>
        public void Dispose()
        {
            _socket.Dispose();
        }
    }
}
