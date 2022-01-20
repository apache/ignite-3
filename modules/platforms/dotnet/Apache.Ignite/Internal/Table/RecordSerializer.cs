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
    using System.Collections.Generic;
    using Buffers;
    using MessagePack;

    /// <summary>
    /// Record serializer.
    /// </summary>
    /// <typeparam name="T">Record type.</typeparam>
    internal class RecordSerializer<T>
        where T : class
    {
        private readonly Table _table;

        private readonly IRecordSerializerHandler<T> _handler;

        /// <summary>
        /// Initializes a new instance of the <see cref="RecordSerializer{T}"/> class.
        /// </summary>
        /// <param name="table">Table.</param>
        /// <param name="handler">Handler.</param>
        public RecordSerializer(Table table, IRecordSerializerHandler<T> handler)
        {
            _table = table;
            _handler = handler;
        }

        /// <summary>
        /// Reads the value part.
        /// </summary>
        /// <param name="buf">Buffer.</param>
        /// <param name="schema">Schema.</param>
        /// <param name="key">Key part.</param>
        /// <returns>Resulting record with key and value parts.</returns>
        public T? ReadValue(PooledBuffer buf, Schema? schema, T key)
        {
            return _handler.ReadValuePart(buf, schema, key);
        }

        /// <summary>
        /// Read multiple records.
        /// </summary>
        /// <param name="buf">Buffer.</param>
        /// <param name="schema">Schema.</param>
        /// <param name="keyOnly">Key only mode.</param>
        /// <returns>List of records.</returns>
        public IList<T> ReadMultiple(PooledBuffer buf, Schema? schema, bool keyOnly = false)
        {
            if (schema == null)
            {
                return Array.Empty<T>();
            }

            // Skip schema version.
            var r = buf.GetReader();
            r.Skip();

            var count = r.ReadInt32();
            var res = new List<T>(count);

            for (var i = 0; i < count; i++)
            {
                res.Add(_handler.Read(ref r, schema, keyOnly));
            }

            return res;
        }

        /// <summary>
        /// Read multiple records, where some of them might be null.
        /// </summary>
        /// <param name="buf">Buffer.</param>
        /// <param name="schema">Schema.</param>
        /// <param name="keyOnly">Key only mode.</param>
        /// <returns>List of records.</returns>
        public IList<T?> ReadMultipleNullable(PooledBuffer buf, Schema? schema, bool keyOnly = false)
        {
            if (schema == null)
            {
                return Array.Empty<T?>();
            }

            // Skip schema version.
            var r = buf.GetReader();
            r.Skip();

            var count = r.ReadInt32();
            var res = new List<T?>(count);

            for (var i = 0; i < count; i++)
            {
                var hasValue = r.ReadBoolean();

                res.Add(hasValue ? _handler.Read(ref r, schema, keyOnly) : null);
            }

            return res;
        }

        /// <summary>
        /// Write record.
        /// </summary>
        /// <param name="buf">Buffer.</param>
        /// <param name="tx">Transaction.</param>
        /// <param name="schema">Schema.</param>
        /// <param name="tuple">Tuple.</param>
        /// <param name="keyOnly">Key only columns.</param>
        public void Write(
            PooledArrayBufferWriter buf,
            Transactions.Transaction? tx,
            Schema schema,
            T tuple,
            bool keyOnly = false)
        {
            var w = buf.GetMessageWriter();

            WriteWithHeader(ref w, tx, schema, tuple, keyOnly);

            w.Flush();
        }

        /// <summary>
        /// Write two records.
        /// </summary>
        /// <param name="buf">Buffer.</param>
        /// <param name="tx">Transaction.</param>
        /// <param name="schema">Schema.</param>
        /// <param name="t">Record 1.</param>
        /// <param name="t2">Record 2.</param>
        /// <param name="keyOnly">Key only columns.</param>
        public void WriteTwo(
            PooledArrayBufferWriter buf,
            Transactions.Transaction? tx,
            Schema schema,
            T t,
            T t2,
            bool keyOnly = false)
        {
            var w = buf.GetMessageWriter();

            WriteWithHeader(ref w, tx, schema, t, keyOnly);
            _handler.Write(ref w, schema, t2, keyOnly);

            w.Flush();
        }

        /// <summary>
        /// Write multiple records.
        /// </summary>
        /// <param name="buf">Buffer.</param>
        /// <param name="tx">Transaction.</param>
        /// <param name="schema">Schema.</param>
        /// <param name="tuples">Records.</param>
        /// <param name="keyOnly">Key only columns.</param>
        public void WriteMultiple(
            PooledArrayBufferWriter buf,
            Transactions.Transaction? tx,
            Schema schema,
            IEnumerator<T> tuples,
            bool keyOnly = false)
        {
            var w = buf.GetMessageWriter();

            _table.WriteIdAndTx(ref w, tx);
            w.Write(schema.Version);
            w.Flush();

            var count = 0;
            var countPos = buf.ReserveInt32();

            do
            {
                var tuple = tuples.Current;

                if (tuple == null)
                {
                    throw new ArgumentException("Tuple collection can't contain null elements.");
                }

                _handler.Write(ref w, schema, tuple, keyOnly);
                count++;
            }
            while (tuples.MoveNext()); // First MoveNext is called outside to check for empty IEnumerable.

            buf.WriteInt32(countPos, count);

            w.Flush();
        }

        /// <summary>
        /// Write record with header.
        /// </summary>
        /// <param name="w">Writer.</param>
        /// <param name="tx">Transaction.</param>
        /// <param name="schema">Schema.</param>
        /// <param name="tuple">Record.</param>
        /// <param name="keyOnly">Key only columns.</param>
        private void WriteWithHeader(
            ref MessagePackWriter w,
            Transactions.Transaction? tx,
            Schema schema,
            T tuple,
            bool keyOnly = false)
        {
            _table.WriteIdAndTx(ref w, tx);
            w.Write(schema.Version);

            _handler.Write(ref w, schema, tuple, keyOnly);
        }
    }
}
