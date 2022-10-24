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

namespace Apache.Ignite.Internal.Proto;

using System;
using BinaryTuple;
using Table;

/// <summary>
/// Hash calculator.
/// </summary>
internal static class HashCalculator
{
    /// <summary>
    /// Calculates binary tuple hash.
    /// </summary>
    /// <param name="mem">Binary tuple bytes.</param>
    /// <param name="keyOnly">Whether binary tuple contains only the key part.</param>
    /// <param name="schema">Schema.</param>
    /// <returns>Binary tuple hash.</returns>
    public static int CalculateBinaryTupleHash(ReadOnlyMemory<byte> mem, bool keyOnly, Schema schema)
    {
        // TODO: Compute hash from resulting BinaryTuple
        var reader = new BinaryTupleReader(mem, keyOnly ? schema.KeyColumnCount : schema.Columns.Count);
        var cols = schema.Columns;
        var hash = 0;

        for (int i = 0; i < schema.KeyColumnCount; i++)
        {
            var col = cols[i];

            hash |= Hash(col, reader);
        }

        return hash;
    }

    private static int Hash(Column col, BinaryTupleReader reader)
    {
        switch (col.Type)
        {
            case ClientDataType.Int8:
                break;
            case ClientDataType.Int16:
                break;
            case ClientDataType.Int32:
                // TODO: port HashCalculator and HashUtils from Java.
                return reader.GetInt(col.SchemaIndex);
            case ClientDataType.Int64:
                break;
            case ClientDataType.Float:
                break;
            case ClientDataType.Double:
                break;
            case ClientDataType.Decimal:
                break;
            case ClientDataType.Uuid:
                break;
            case ClientDataType.String:
                break;
            case ClientDataType.Bytes:
                break;
            case ClientDataType.BitMask:
                break;
            case ClientDataType.Date:
                break;
            case ClientDataType.Time:
                break;
            case ClientDataType.DateTime:
                break;
            case ClientDataType.Timestamp:
                break;
            case ClientDataType.Number:
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(col));
        }

        return 0;
    }
}
