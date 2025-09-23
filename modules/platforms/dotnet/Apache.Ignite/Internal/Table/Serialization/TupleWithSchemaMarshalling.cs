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
using System.Buffers;
using System.Buffers.Binary;
using Ignite.Sql;
using Ignite.Table;
using Proto.BinaryTuple;

/// <summary>
/// Tuple with schema marshalling - see also o.a.i.internal.binarytuple.inlineschema.TupleWithSchemaMarshalling.
/// </summary>
internal static class TupleWithSchemaMarshalling
{
    /// <summary>
    /// Type id for <see cref="IIgniteTuple"/>.
    /// </summary>
    public const int TypeIdTuple = -1;

    /// <summary>
    /// Packs tuple with schema.
    /// </summary>
    /// <param name="w">Packer.</param>
    /// <param name="tuple">Tuple.</param>
    public static void Pack(IBufferWriter<byte> w, IIgniteTuple tuple)
    {
        int elementCount = tuple.FieldCount;

        using var schemaBuilder = new BinaryTupleBuilder(elementCount * 2);
        using var valueBuilder = new BinaryTupleBuilder(elementCount);

        for (int i = 0; i < elementCount; i++)
        {
            string fieldName = tuple.GetName(i);
            object? fieldValue = tuple[i];

            schemaBuilder.AppendString(fieldName);

            if (fieldValue is IIgniteTuple nestedTuple)
            {
                valueBuilder.AppendBytes(static (bufWriter, arg) => Pack(bufWriter, arg), nestedTuple);
                schemaBuilder.AppendInt(TypeIdTuple);
            }
            else
            {
                ColumnType typeId = valueBuilder.AppendObjectAndGetType(fieldValue);
                schemaBuilder.AppendInt((int)typeId);
            }
        }

        Memory<byte> schemaMem = schemaBuilder.Build();
        Memory<byte> valueMem = valueBuilder.Build();

        // Size: int32 (tuple size), int32 (value offset), schema, value.
        var schemaOffset = 8;
        var valueOffset = schemaOffset + schemaMem.Length;
        var totalSize = valueOffset + valueMem.Length;

        Span<byte> targetSpan = w.GetSpan(totalSize);
        w.Advance(totalSize);

        BinaryPrimitives.WriteInt32LittleEndian(targetSpan, elementCount);
        BinaryPrimitives.WriteInt32LittleEndian(targetSpan[4..], valueOffset);
        schemaMem.Span.CopyTo(targetSpan[schemaOffset..]);
        valueMem.Span.CopyTo(targetSpan[valueOffset..]);
    }

    /// <summary>
    /// Unpacks tuple with schema.
    /// </summary>
    /// <param name="span">Bytes.</param>
    /// <returns>Tuple.</returns>
    public static IgniteTuple Unpack(ReadOnlySpan<byte> span)
    {
        int elementCount = BinaryPrimitives.ReadInt32LittleEndian(span);
        int valueOffset = BinaryPrimitives.ReadInt32LittleEndian(span[4..]);

        ReadOnlySpan<byte> schemaBytes = span[8..valueOffset];
        ReadOnlySpan<byte> valueBytes = span[valueOffset..];

        var res = new IgniteTuple(elementCount);

        var schemaReader = new BinaryTupleReader(schemaBytes, elementCount * 2);
        var valueReader = new BinaryTupleReader(valueBytes, elementCount);

        for (int i = 0; i < elementCount; i++)
        {
            string fieldName = schemaReader.GetString(i * 2);
            int fieldTypeId = schemaReader.GetInt(i * 2 + 1);

            if (fieldTypeId == TypeIdTuple)
            {
                res[fieldName] = Unpack(valueReader.GetBytesSpan(i));
            }
            else
            {
                res[fieldName] = valueReader.GetObject(i, (ColumnType)fieldTypeId);
            }
        }

        return res;
    }
}
