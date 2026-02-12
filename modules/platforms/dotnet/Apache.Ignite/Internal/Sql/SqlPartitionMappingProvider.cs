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

namespace Apache.Ignite.Internal.Sql;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Text;
using System.Threading.Tasks;
using NodaTime;
using Proto;
using Proto.BinaryTuple;
using Table;

/// <summary>
/// SQL partition mapping provider.
/// </summary>
internal sealed record SqlPartitionMappingProvider(SqlPartitionAwarenessMetadata Meta, Table Table)
{
    /// <summary>
    /// Gets the preferred node for routing based on the given query parameters.
    /// </summary>
    /// <param name="args">Query parameter values.</param>
    /// <returns>Preferred node, or default if mapping cannot be computed yet.</returns>
    public async ValueTask<PreferredNode> GetPreferredNode(ICollection<object?>? args)
    {
        // Both async calls return cached results if available, no need to cache here.
        var schema = await Table.GetSchemaAsync(Table.SchemaVersionUnknown).ConfigureAwait(false);
        var assignments = await Table.GetPartitionAssignmentAsync().ConfigureAwait(false);

        return GetPreferredNodeInternal(args, schema, assignments);
    }

    // TODO: Move to HashUtils.
    private static int HashValue(object? value, int scale, int precision) => value switch
    {
        null => HashUtils.Hash32((sbyte)0),
        bool b => HashUtils.Hash32(BinaryTupleCommon.BoolToByte(b)),
        sbyte sb => HashUtils.Hash32(sb),
        short s => HashUtils.Hash32(s),
        int i => HashUtils.Hash32(i),
        long l => HashUtils.Hash32(l),
        float f => HashUtils.Hash32(f),
        double d => HashUtils.Hash32(d),
        decimal dec => HashDecimal(new BigDecimal(dec), scale),
        BigDecimal bd => HashDecimal(bd, scale),
        Guid g => HashGuid(g),
        string str => HashString(str),
        byte[] bytes => HashUtils.Hash32(bytes),
        LocalDate date => HashUtils.Hash32(date),
        LocalTime time => HashUtils.Hash32(time, precision),
        LocalDateTime dateTime => HashUtils.Hash32(dateTime, precision),
        Instant instant => HashTimestamp(instant, precision),
        _ => throw new NotSupportedException("Unsupported value type for partition awareness hash: " + value.GetType())
    };

    private static int HashDecimal(BigDecimal value, int columnScale)
    {
        var unscaledValue = value.UnscaledValue;

        if (value.Scale > columnScale)
        {
            unscaledValue /= BigInteger.Pow(10, value.Scale - columnScale);
        }
        else if (value.Scale < columnScale)
        {
            unscaledValue *= BigInteger.Pow(10, columnScale - value.Scale);
        }

        return HashUtils.Hash32(unscaledValue.ToByteArray(isBigEndian: true));
    }

    private static int HashGuid(Guid value)
    {
        Span<byte> span = stackalloc byte[16];
        UuidSerializer.Write(value, span);

        var lo = BinaryPrimitives.ReadInt64LittleEndian(span[..8]);
        var hi = BinaryPrimitives.ReadInt64LittleEndian(span[8..]);

        return HashUtils.Hash32(hi, HashUtils.Hash32(lo));
    }

    private static int HashString(string value)
    {
        if (value.Length == 0)
        {
            return HashUtils.Hash32(ReadOnlySpan<byte>.Empty);
        }

        var maxByteCount = Encoding.UTF8.GetMaxByteCount(value.Length);
        Span<byte> buffer = maxByteCount <= 256 ? stackalloc byte[maxByteCount] : new byte[maxByteCount];
        var actualBytes = Encoding.UTF8.GetBytes(value, buffer);

        return HashUtils.Hash32(buffer[..actualBytes]);
    }

    private static int HashTimestamp(Instant value, int precision)
    {
        var (seconds, nanos) = value.ToSecondsAndNanos(precision);

        return HashUtils.Hash32(nanos, HashUtils.Hash32(seconds));
    }

    private PreferredNode GetPreferredNodeInternal(ICollection<object?>? args, Schema schema, string?[] assignments)
    {
        var indexes = Meta.Indexes.Span;
        var hash = Meta.Hash.Span;
        var colocationColumns = schema.ColocationColumns;

        if (colocationColumns.Length != indexes.Length)
        {
            return default;
        }

        IList<object?> args0 = args as IList<object?> ?? args?.ToArray() ?? [];

        int colocationHash = 0;

        // NOTE: Can't reuse BinaryTupleBuilder for hash calculation because of constant values that are not present in args.
        for (int i = 0; i < colocationColumns.Length; i++)
        {
            int idx = indexes[i];

            if (idx < 0)
            {
                colocationHash = HashUtils.Combine(colocationHash, hash[-(idx + 1)]);
                continue;
            }

            if (idx >= args0.Count)
            {
                return default;
            }

            Column column = colocationColumns[i];
            object? arg = args0[idx];
            int valueHash = HashValue(arg, column.Scale, column.Precision);

            colocationHash = HashUtils.Combine(colocationHash, valueHash);
        }

        int partition = Math.Abs(colocationHash % assignments.Length);
        var node = assignments[partition];

        return node == null ? default : PreferredNode.FromName(node);
    }
}
