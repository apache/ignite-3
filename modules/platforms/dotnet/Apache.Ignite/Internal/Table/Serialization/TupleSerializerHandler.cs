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
    using System.Linq;
    using Common;
    using Ignite.Table;
    using Proto.BinaryTuple;
    using Proto.MsgPack;

    /// <summary>
    /// Serializer handler for <see cref="IIgniteTuple"/>.
    /// </summary>
    internal sealed class TupleSerializerHandler : IRecordSerializerHandler<IIgniteTuple>
    {
        /// <summary>
        /// Singleton instance.
        /// </summary>
        public static readonly IRecordSerializerHandler<IIgniteTuple> Instance = new TupleSerializerHandler();

        /// <summary>
        /// Initializes a new instance of the <see cref="TupleSerializerHandler"/> class.
        /// </summary>
        private TupleSerializerHandler()
        {
            // No-op.
        }

        /// <summary>
        /// Reads tuple from the buffer.
        /// </summary>
        /// <param name="buf">Buffer.</param>
        /// <param name="schema">Schema.</param>
        /// <param name="keyOnly">Whether to read only the key columns.</param>
        /// <returns>Tuple.</returns>
        public static IgniteTuple ReadTuple(ReadOnlySpan<byte> buf, Schema schema, bool keyOnly)
        {
            var columns = schema.GetColumnsFor(keyOnly);
            var tuple = new IgniteTuple(columns.Length);
            var tupleReader = new BinaryTupleReader(buf, columns.Length);

            foreach (var column in columns)
            {
                tuple[column.Name] = tupleReader.GetObject(column.GetBinaryTupleIndex(keyOnly), column.Type, column.Scale);
            }

            return tuple;
        }

        /// <summary>
        /// Reads single column from the binary tuple.
        /// </summary>
        /// <param name="buf">Binary tuple buffer.</param>
        /// <param name="schema">Schema.</param>
        /// <param name="keyOnly">Whether <paramref name="buf"/> is a key-only binary tuple.</param>
        /// <param name="index">Column index.</param>
        /// <returns>Column value.</returns>
        public static object? ReadObject(ReadOnlySpan<byte> buf, Schema schema, bool keyOnly, int index)
        {
            var columns = schema.GetColumnsFor(keyOnly);
            var tupleReader = new BinaryTupleReader(buf, columns.Length);
            var column = columns[index];

            return tupleReader.GetObject(index, column.Type, column.Scale);
        }

        /// <inheritdoc/>
        public IIgniteTuple Read(ref MsgPackReader reader, Schema schema, bool keyOnly = false) =>
            new BinaryTupleIgniteTupleAdapter(
                data: reader.ReadBinary().ToArray(),
                schema: schema,
                keyOnly);

        /// <inheritdoc/>
        public void Write(ref BinaryTupleBuilder tupleBuilder, IIgniteTuple record, Schema schema, bool keyOnly, scoped Span<byte> noValueSet)
        {
            int written = 0;
            var columns = keyOnly ? schema.KeyColumns : schema.Columns;

            foreach (var col in columns)
            {
                var colIdx = record.GetOrdinal(col.Name);

                if (colIdx >= 0)
                {
                    tupleBuilder.AppendObject(record[colIdx], col.Type, col.Scale, col.Precision);
                    written++;
                }
                else
                {
                    if (col.IsKey)
                    {
                        throw new ArgumentException($"Key column '{col.Name}' not found in the provided tuple '{record}'");
                    }

                    tupleBuilder.AppendNoValue(noValueSet);
                }
            }

            ValidateMappedCount(record, schema, columns.Length, written, keyOnly);
        }

        private static void ValidateMappedCount(IIgniteTuple record, Schema schema, int columnCount, int written, bool keyOnly)
        {
            if (written == 0)
            {
                var columnStr = schema.Columns.Select(x => x.Type + " " + x.Name).StringJoin();
                throw new ArgumentException($"Can't map '{record}' to columns '{columnStr}'. Matching fields not found.");
            }

            if (keyOnly && written == schema.KeyColumns.Length)
            {
                return;
            }

            if (record.FieldCount > written)
            {
                var extraColumns = new HashSet<string>(record.FieldCount, StringComparer.OrdinalIgnoreCase);
                for (int i = 0; i < record.FieldCount; i++)
                {
                    var name = record.GetName(i);

                    if (!extraColumns.Add(name))
                    {
                        throw new ArgumentException("Duplicate column in Tuple: " + name, nameof(record));
                    }
                }

                for (var i = 0; i < columnCount; i++)
                {
                    extraColumns.Remove(schema.Columns[i].Name);
                }

                throw SerializerExceptionExtensions.GetUnmappedColumnsException("Tuple", schema, extraColumns);
            }
        }
    }
}
