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
            // TODO: Random node.
            // TODO: Validate args.
            return await ExecuteOnOneNode<T>(nodes.First(), jobClassName, args).ConfigureAwait(false);
        }

        private async Task<T> ExecuteOnOneNode<T>(IClusterNode node, string jobClassName, object[] args)
        {
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
            }

            T Read()
            {
                var reader = resBuf.GetReader();

                return (T)reader.ReadObjectWithType()!;
            }
        }
    }
}
