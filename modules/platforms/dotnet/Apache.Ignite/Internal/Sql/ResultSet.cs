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

namespace Apache.Ignite.Internal.Sql
{
    using System.Threading.Tasks;
    using Buffers;
    using Ignite.Sql;

    /// <summary>
    /// SQL result set.
    /// </summary>
    internal sealed class ResultSet : IResultSet
    {
        private readonly ClientFailoverSocket _socket;

        private readonly long? _resourceId;

        /// <summary>
        /// Initializes a new instance of the <see cref="ResultSet"/> class.
        /// </summary>
        /// <param name="socket">Socket.</param>
        /// <param name="buf">Buffer to read initial data from.</param>
        public ResultSet(ClientFailoverSocket socket, PooledBuffer buf)
        {
            _socket = socket;

            var reader = buf.GetReader();

            _resourceId = reader.TryReadNil() ? null : reader.ReadInt64();
            AffectedRows = reader.ReadInt64();
        }

        /// <inheritdoc/>
        public long AffectedRows { get; }

        /// <inheritdoc/>
        public ValueTask DisposeAsync()
        {
            // TODO: Close server-side resources.
            throw new System.NotImplementedException();
        }
    }
}
