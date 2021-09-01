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

namespace Apache.Ignite.Internal.Table
{
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Ignite.Table;
    using MessagePack;
    using Proto;

    /// <summary>
    /// Tables API.
    /// </summary>
    internal class Tables : ITables
    {
        /** Socket. */
        private readonly ClientFailoverSocket _socket;

        /// <summary>
        /// Initializes a new instance of the <see cref="Tables"/> class.
        /// </summary>
        /// <param name="socket">Socket.</param>
        public Tables(ClientFailoverSocket socket)
        {
            _socket = socket;
        }

        /// <inheritdoc/>
        public Task<ITable?> GetTableAsync(string name)
        {
            throw new System.NotImplementedException();
        }

        /// <inheritdoc/>
        public async Task<IList<ITable>> GetTablesAsync()
        {
            using var writer = await _socket.GetRequestWriterAsync(ClientOp.TablesGet).ConfigureAwait(false);

            using var resBuf = await writer.Socket.DoOutInOpAsync(writer).ConfigureAwait(false);

            return ReadTables(resBuf.GetReader());

            IList<ITable> ReadTables(MessagePackReader r)
            {
                var len = r.ReadArrayHeader();

                var res = new List<ITable>(len);

                for (var i = 0; i < len; i++)
                {
                    var id = r.ReadGuid();
                    var name = r.ReadString();

                    res.Add(new Table(name, id));
                }

                return res;
            }
        }
    }
}
