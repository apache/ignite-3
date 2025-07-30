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

namespace Apache.Ignite.Internal.Table.Serialization
{
    using System;
    using System.Collections.Generic;
    using Buffers;
    using Proto.MsgPack;

    /// <summary>
    /// Generic record serializer.
    /// Works for tuples and user objects, any differences are handled by the underlying <see cref="IRecordSerializerHandler{T}"/>.
    /// </summary>
    /// <typeparam name="T">Record type.</typeparam>
    internal sealed class RecordSerializer<T>
    {
        /** Table. */
        private readonly Table _table;

        /** Serialization handler. */
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
        /// Gets the handler.
        /// </summary>
        public IRecordSerializerHandler<T> Handler => _handler;

        /// <summary>
        /// Reads the value part.
        /// </summary>
        /// <param name="buf">Buffer.</param>
        /// <param name="schema">Schema or null when there is no value.</param>
        /// <returns>Resulting record with key and value parts.</returns>
        public Option<T> ReadValue(PooledBuffer buf, Schema schema)
        {
            var r = buf.GetReader();

            r.Skip(); // Skip schema version.

            return r.TryReadNil()
                ? default
                : Option.Some(_handler.Read(ref r, schema));
        }

        /// <summary>
        /// Read multiple records.
        /// </summary>
        /// <param name="buf">Buffer.</param>
        /// <param name="schema">Schema or null when there is no value.</param>
        /// <param name="keyOnly">Key only mode.</param>
        /// <param name="resultFactory">Result factory.</param>
        /// <param name="addAction">Adds items to the result.</param>
        /// <typeparam name="TRes">Result type.</typeparam>
        /// <returns>List of records.</returns>
        public TRes ReadMultiple<TRes>(
            PooledBuffer buf,
            Schema? schema,
            bool keyOnly,
            Func<int, TRes> resultFactory,
            Action<TRes, T> addAction)
        {
            if (schema == null)
            {
                // Null schema means empty collection.
                return resultFactory(0);
            }

            // Skip schema version.
            var r = buf.GetReader();
            r.Skip();

            var count = r.ReadInt32();
            var res = resultFactory(count);

            for (var i = 0; i < count; i++)
            {
                addAction(res, _handler.Read(ref r, schema, keyOnly));
            }

            return res;
        }

        /// <summary>
        /// Read multiple records, where some of them might be null.
        /// </summary>
        /// <param name="buf">Buffer.</param>
        /// <param name="schema">Schema or null when there is no value.</param>
        /// <param name="resultFactory">Result factory.</param>
        /// <param name="addAction">Adds items to the result.</param>
        /// <typeparam name="TRes">Result type.</typeparam>
        /// <returns>List of records.</returns>
        public TRes ReadMultipleNullable<TRes>(
            PooledBuffer buf,
            Schema? schema,
            Func<int, TRes> resultFactory,
            Action<TRes, Option<T>> addAction)
        {
            if (schema == null)
            {
                // Null schema means empty collection.
                return resultFactory(0);
            }

            // Skip schema version.
            var r = buf.GetReader();
            r.Skip();

            var count = r.ReadInt32();
            var res = resultFactory(count);

            for (var i = 0; i < count; i++)
            {
                var option = r.ReadBoolean()
                    ? Option.Some(_handler.Read(ref r, schema))
                    : default;

                addAction(res, option);
            }

            return res;
        }

        /// <summary>
        /// Write record.
        /// </summary>
        /// <param name="buf">Buffer.</param>
        /// <param name="txId">Transaction id.</param>
        /// <param name="schema">Schema.</param>
        /// <param name="rec">Record.</param>
        /// <param name="keyOnly">Key only columns.</param>
        /// <returns>Colocation hash.</returns>
        public (int ColocationHash, int TxIdPos) Write(
            PooledArrayBuffer buf,
            long? txId,
            Schema schema,
            T rec,
            bool keyOnly = false)
        {
            var w = buf.MessageWriter;

            var colocationHash = WriteWithHeader(ref w, txId, schema, rec, keyOnly);

            return colocationHash;
        }

        /// <summary>
        /// Write two records.
        /// </summary>
        /// <param name="buf">Buffer.</param>
        /// <param name="txId">Transaction id.</param>
        /// <param name="schema">Schema.</param>
        /// <param name="t">Record 1.</param>
        /// <param name="t2">Record 2.</param>
        /// <param name="keyOnly">Key only columns.</param>
        /// <returns>First record hash.</returns>
        public (int ColocationHash, int TxIdPos) WriteTwo(
            PooledArrayBuffer buf,
            long? txId,
            Schema schema,
            T t,
            T t2,
            bool keyOnly = false)
        {
            var w = buf.MessageWriter;

            var firstHash = WriteWithHeader(ref w, txId, schema, t, keyOnly);
            _handler.Write(ref w, schema, t2, keyOnly);

            return firstHash;
        }

        /// <summary>
        /// Write multiple records.
        /// </summary>
        /// <param name="buf">Buffer.</param>
        /// <param name="txId">Transaction.</param>
        /// <param name="schema">Schema.</param>
        /// <param name="recs">Records.</param>
        /// <param name="keyOnly">Key only columns.</param>
        /// <returns>First record hash.</returns>
        public (int ColocationHash, int TxIdPos) WriteMultiple(
            PooledArrayBuffer buf,
            long? txId,
            Schema schema,
            IEnumerator<T> recs,
            bool keyOnly = false)
        {
            var w = buf.MessageWriter;

            var txIdPos = WriteIdAndTx(ref w, txId);
            w.Write(schema.Version);

            var count = 0;
            var firstHash = 0;

            var countPos = buf.ReserveMsgPackInt32();

            do
            {
                var rec = recs.Current;

                if (rec == null)
                {
                    throw new ArgumentException("Record collection can't contain null elements.");
                }

                var hash = _handler.Write(ref w, schema, rec, keyOnly, computeHash: count == 0);

                if (count == 0)
                {
                    firstHash = hash;
                }

                count++;
            }
            while (recs.MoveNext()); // First MoveNext is called outside to check for empty IEnumerable.

            buf.WriteMsgPackInt32(count, countPos);

            return (firstHash, txIdPos);
        }

        /// <summary>
        /// Write record with header.
        /// </summary>
        /// <param name="w">Writer.</param>
        /// <param name="txId">Transaction id.</param>
        /// <param name="schema">Schema.</param>
        /// <param name="rec">Record.</param>
        /// <param name="keyOnly">Key only columns.</param>
        /// <returns>Colocation hash.</returns>
        private (int ColocationHash, int TxIdPos) WriteWithHeader(
            ref MsgPackWriter w,
            long? txId,
            Schema schema,
            T rec,
            bool keyOnly = false)
        {
            var txIdPos = WriteIdAndTx(ref w, txId);
            w.Write(schema.Version);

            var colocationHash = _handler.Write(ref w, schema, rec, keyOnly, computeHash: true);

            return (colocationHash, txIdPos);
        }

        /// <summary>
        /// Writes table id and transaction id, if present.
        /// </summary>
        /// <param name="w">Writer.</param>
        /// <param name="txId">Transaction id.</param>
        private int WriteIdAndTx(ref MsgPackWriter w, long? txId)
        {
            w.Write(_table.Id);

            var txIdPos = w.Position;
            w.WriteTx(txId);

            return txIdPos;
        }
    }
}
