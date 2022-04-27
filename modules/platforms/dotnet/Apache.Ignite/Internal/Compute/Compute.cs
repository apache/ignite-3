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

namespace Apache.Ignite.Internal.Compute
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Buffers;
    using Common;
    using Ignite.Compute;
    using Ignite.Network;
    using Proto;

    /// <summary>
    /// Compute API.
    /// </summary>
    internal sealed class Compute : ICompute
    {
        /** Socket. */
        private readonly ClientFailoverSocket _socket;

        /// <summary>
        /// Initializes a new instance of the <see cref="Compute"/> class.
        /// </summary>
        /// <param name="socket">Socket.</param>
        public Compute(ClientFailoverSocket socket)
        {
            _socket = socket;
        }

        /// <inheritdoc/>
        public async Task<T> ExecuteAsync<T>(IEnumerable<IClusterNode> nodes, string jobClassName, params object[] args)
        {
            IgniteArgumentCheck.NotNull(nodes, nameof(nodes));
            IgniteArgumentCheck.NotNull(jobClassName, nameof(jobClassName));

            // TODO: Cluster awareness (IGNITE-16823): match specified nodes to known connections.
            return await ExecuteOnOneNode<T>(GetRandomNode(nodes), jobClassName, args).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        public IDictionary<IClusterNode, Task<T>> BroadcastAsync<T>(IEnumerable<IClusterNode> nodes, string jobClassName, params object[] args)
        {
            IgniteArgumentCheck.NotNull(nodes, nameof(nodes));
            IgniteArgumentCheck.NotNull(jobClassName, nameof(jobClassName));

            var res = new Dictionary<IClusterNode, Task<T>>();

            foreach (var node in nodes)
            {
                var task = ExecuteOnOneNode<T>(node, jobClassName, args);

                res[node] = task;
            }

            return res;
        }

        private static IClusterNode GetRandomNode(IEnumerable<IClusterNode> nodes)
        {
            var nodesCol = GetNodesCollection(nodes);

            IgniteArgumentCheck.Ensure(nodesCol.Count > 0, nameof(nodes), "Nodes can't be empty.");

            var idx = ThreadLocalRandom.Instance.Next(0, nodesCol.Count);

            return nodesCol.ElementAt(idx);
        }

        private static ICollection<IClusterNode> GetNodesCollection(IEnumerable<IClusterNode> nodes) =>
            nodes is ICollection<IClusterNode> col ? col : nodes.ToList();

        private async Task<T> ExecuteOnOneNode<T>(IClusterNode node, string jobClassName, object[] args)
        {
            IgniteArgumentCheck.NotNull(node, nameof(node));

            // Try direct connection to the specified node.
            if (_socket.GetEndpoint(node.Name) is { } endpoint)
            {
                using var writerWithoutNode = new PooledArrayBufferWriter();
                Write(writerWithoutNode, writeNode: false);

                using var res1 = await _socket.TryDoOutInOpAsync(endpoint, ClientOp.ComputeExecute, writerWithoutNode)
                    .ConfigureAwait(false);

                // Result is null when there was a connection issue, but retry policy allows another try.
                if (res1 != null)
                {
                    return Read(res1.Value);
                }
            }

            // When direct connection is not available, use default connection and pass target node info to the server.
            using var writerWithNode = new PooledArrayBufferWriter();
            Write(writerWithNode, writeNode: true);

            using var res2 = await _socket.DoOutInOpAsync(ClientOp.ComputeExecute, writerWithNode).ConfigureAwait(false);

            return Read(res2);

            void Write(PooledArrayBufferWriter writer, bool writeNode)
            {
                var w = writer.GetMessageWriter();

                if (writeNode)
                {
                    w.Write(node.Name);
                }
                else
                {
                    w.WriteNil();
                }

                w.Write(jobClassName);
                w.WriteObjectArrayWithTypes(args);

                w.Flush();
            }

            static T Read(in PooledBuffer buf)
            {
                var reader = buf.GetReader();

                return (T)reader.ReadObjectWithType()!;
            }
        }
    }
}
