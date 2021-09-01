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
    using System;
    using System.Threading.Tasks;
    using Buffers;
    using Common;
    using Ignite.Table;
    using MessagePack;
    using Proto;

    /// <summary>
    /// Table API.
    /// </summary>
    internal class Table : ITable
    {
        /** Socket. */
        private readonly ClientFailoverSocket _socket;

        /// <summary>
        /// Initializes a new instance of the <see cref="Table"/> class.
        /// </summary>
        /// <param name="name">Table name.</param>
        /// <param name="id">Table id.</param>
        /// <param name="socket">Socket.</param>
        public Table(string name, Guid id, ClientFailoverSocket socket)
        {
            _socket = socket;
            Name = name;
            Id = id;
        }

        /// <inheritdoc/>
        public string Name { get; }

        /// <summary>
        /// Gets the id.
        /// </summary>
        public Guid Id { get; }

        /// <inheritdoc/>
        public async Task<IIgniteTuple?> GetAsync(IIgniteTuple keyRec)
        {
            IgniteArgumentCheck.NotNull(keyRec, nameof(keyRec));

            using var writer = new PooledArrayBufferWriter();
            Write(writer.GetMessageWriter());

            using var resBuf = await _socket.DoOutInOpAsync(ClientOp.TupleGet, writer).ConfigureAwait(false);
            return Read(resBuf.GetReader());

            void Write(MessagePackWriter w)
            {
                w.WriteGuid(Id);
                w.Write(1); // TODO: Schema id

                // TODO: Schema order with a pooled array.
                for (var i = 0; i < keyRec.FieldCount; i++)
                {
                    w.WriteObject(keyRec[i]);
                }

                w.Flush();
            }

            IIgniteTuple? Read(MessagePackReader r) =>
                r.NextMessagePackType == MessagePackType.Nil
                    ? null
                    : new IgniteTuple();
        }

        /// <inheritdoc/>
        public Task UpsertAsync(IIgniteTuple rec)
        {
            throw new NotImplementedException();
        }
    }
}
