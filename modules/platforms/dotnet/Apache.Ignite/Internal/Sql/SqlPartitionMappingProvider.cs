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
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
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

    private PreferredNode GetPreferredNodeInternal(ICollection<object?>? args, Schema schema, string?[] assignments)
    {
        var indexes = Meta.Indexes.Span;
        var colocationColumns = schema.ColocationColumns;

        if (colocationColumns.Length != indexes.Length)
        {
            return default;
        }

        IList<object?> args0 = args as IList<object?> ?? args?.ToArray() ?? [];

        using var tupleBuilder = new BinaryTupleBuilder(
            numElements: colocationColumns.Length,
            hashedColumnsPredicate: Meta);

        // First pass: append values for non-constant columns.
        for (int i = 0; i < colocationColumns.Length; i++)
        {
            int idx = indexes[i];
            if (idx < 0)
            {
                tupleBuilder.AppendNull(); // Skip and advance.
                continue;
            }

            if (idx >= args0.Count)
            {
                // Not enough arguments to determine partition.
                return default;
            }

            Column column = colocationColumns[i];
            object? arg = args0[idx];

            tupleBuilder.AppendObject(arg, column.Type, column.Scale, column.Precision);
        }

        // Second pass: combine hashes for constant and written columns.
        int colocationHash = 0;
        var writtenHashes = tupleBuilder.GetHashes();

        for (int i = 0; i < colocationColumns.Length; i++)
        {
            int idx = indexes[i];

            var valueHash = idx < 0
                ? Meta.Hash.Span[-(idx + 1)]
                : writtenHashes[i];

            colocationHash = HashUtils.Combine(colocationHash, valueHash);
        }

        int partition = Math.Abs(colocationHash % assignments.Length);
        var node = assignments[partition];

        return node == null ? default : PreferredNode.FromName(node);
    }
}
