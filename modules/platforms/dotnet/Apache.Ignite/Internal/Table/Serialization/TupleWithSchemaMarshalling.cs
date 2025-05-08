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

using Ignite.Sql;
using Ignite.Table;
using Proto.BinaryTuple;
using Proto.MsgPack;

/// <summary>
/// Tuple with schema marshalling - see also o.a.i.internal.binarytuple.inlineschema.TupleWithSchemaMarshalling.
/// </summary>
internal static class TupleWithSchemaMarshalling
{
    private const int TypeIdTuple = -1;

    /// <summary>
    /// Packs tuple with schema.
    /// </summary>
    /// <param name="w">Packer.</param>
    /// <param name="tuple">Tuple.</param>
    public static void Pack(ref MsgPackWriter w, IIgniteTuple tuple)
    {
        int size = tuple.FieldCount;

        using var schemaBuilder = new BinaryTupleBuilder(size * 2);
        using var valueBuilder = new BinaryTupleBuilder(size);

        for (int i = 0; i < size; i++)
        {
            var fieldName = tuple.GetName(i);
            var fieldValue = tuple[i];

            schemaBuilder.AppendString(fieldName);
        }
    }

    private static int GetColumnTypeId(object? val)
    {
        if (val is IIgniteTuple)
        {
            return TypeIdTuple;
        }
    }
}
