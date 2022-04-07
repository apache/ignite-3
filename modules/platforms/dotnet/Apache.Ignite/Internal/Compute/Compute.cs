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

            using var writer = new PooledArrayBufferWriter();
            Write();

            using var resBuf = await _socket.DoOutInOpAsync(ClientOp.ComputeExecute, writer).ConfigureAwait(false);

            return Read();

            void Write()
            {
                var w = writer.GetMessageWriter();

                w.Write(node.Id);
                w.Write(node.Name);
                w.Write(node.Address.Address.ToString());
                w.Write(node.Address.Port);

                w.Write(jobClassName);
                w.WriteObjectArray(args);

                w.Flush();
            }

            T Read()
            {
                var reader = resBuf.GetReader();

                return (T)reader.ReadObjectWithType()!;
            }
        }
    }
}
