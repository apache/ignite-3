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

namespace Apache.Ignite.Internal.Table.Serialization;

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Common;
using Ignite.Table;
using Proto.BinaryTuple;
using Proto.MsgPack;

/// <summary>
/// Serializer handler for <see cref="IIgniteTuple"/>.
/// </summary>
internal sealed class TuplePairSerializerHandler : IRecordSerializerHandler<KvPair<IIgniteTuple, IIgniteTuple>>
{
    /// <summary>
    /// Singleton instance.
    /// </summary>
    public static readonly IRecordSerializerHandler<KvPair<IIgniteTuple, IIgniteTuple>> Instance = new TuplePairSerializerHandler();

    /// <summary>
    /// Initializes a new instance of the <see cref="TuplePairSerializerHandler"/> class.
    /// </summary>
    private TuplePairSerializerHandler()
    {
        // No-op.
    }

    /// <inheritdoc/>
    public KvPair<IIgniteTuple, IIgniteTuple> Read(ref MsgPackReader reader, Schema schema, bool keyOnly = false)
    {
        if (keyOnly)
        {
            var keyTuple = new IgniteTuple(schema.KeyColumns.Length);
            var tupleReader = new BinaryTupleReader(reader.ReadBinary(), schema.KeyColumns.Length);

            foreach (var column in schema.KeyColumns)
            {
                keyTuple[column.Name] = tupleReader.GetObject(column.KeyIndex, column.Type, column.Scale);
            }

            return new(keyTuple);
        }
        else
        {
            var keyTuple = new IgniteTuple(schema.KeyColumns.Length);
            var valTuple = new IgniteTuple(schema.ValColumns.Length);
            var tupleReader = new BinaryTupleReader(reader.ReadBinary(), schema.Columns.Length);

            foreach (var column in schema.Columns)
            {
                var tuple = column.IsKey ? keyTuple : valTuple;
                tuple[column.Name] = tupleReader.GetObject(column.SchemaIndex, column.Type, column.Scale);
            }

            return new(keyTuple, valTuple);
        }
    }

    /// <inheritdoc/>
    public void Write(
        ref BinaryTupleBuilder tupleBuilder,
        KvPair<IIgniteTuple, IIgniteTuple> record,
        Schema schema,
        bool keyOnly,
        scoped Span<byte> noValueSet)
    {
        var key = record.Key;
        var val = record.Val;

        IgniteArgumentCheck.NotNull(key);

        if (!keyOnly)
        {
            IgniteArgumentCheck.NotNull(val);
        }

        int written = 0;
        var columns = schema.GetColumnsFor(keyOnly);

        foreach (var col in columns)
        {
            var rec = col.IsKey ? key : val;
            var colIdx = rec.GetOrdinal(col.Name);

            if (colIdx >= 0)
            {
                tupleBuilder.AppendObject(rec[colIdx], col.Type, col.Scale, col.Precision);
                written++;
            }
            else
            {
                tupleBuilder.AppendNoValue(noValueSet);
            }
        }

        ValidateMappedCount(record, schema, columns.Length, written);
    }

    private static void ValidateMappedCount(KvPair<IIgniteTuple, IIgniteTuple> record, Schema schema, int columnCount, int written)
    {
        if (written == 0)
        {
            var columnStr = schema.Columns.Select(x => x.Type + " " + x.Name).StringJoin();
            throw new ArgumentException($"Can't map '{record}' to columns '{columnStr}'. Matching fields not found.");
        }

        var recordFieldCount = record.Key.FieldCount;

        // ReSharper disable once ConditionIsAlwaysTrueOrFalseAccordingToNullableAPIContract
        if (record.Val != null)
        {
            recordFieldCount += record.Val.FieldCount;
        }

        if (recordFieldCount > written)
        {
            var extraColumns = new HashSet<string>(recordFieldCount, StringComparer.OrdinalIgnoreCase);

            for (int i = 0; i < record.Key.FieldCount; i++)
            {
                var name = record.Key.GetName(i);

                if (!extraColumns.Add(name))
                {
                    throw new ArgumentException("Duplicate column in Key portion of KeyValue pair: " + name, nameof(record));
                }
            }

            if (record.Val != null)
            {
                for (int i = 0; i < record.Val.FieldCount; i++)
                {
                    var name = record.Val.GetName(i);

                    if (!extraColumns.Add(name))
                    {
                        throw new ArgumentException("Duplicate column in Value portion of KeyValue pair: " + name, nameof(record));
                    }
                }
            }

            for (var i = 0; i < columnCount; i++)
            {
                extraColumns.Remove(schema.Columns[i].Name);
            }

            Debug.Assert(extraColumns.Count > 0, "extraColumns.Count > 0");

            throw SerializerExceptionExtensions.GetUnmappedColumnsException("Tuple pair", schema, extraColumns);
        }
    }
}
