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

namespace Apache.Ignite.Tests.Table;

using System;
using System.Collections.Generic;
using Ignite.Sql;
using Ignite.Table;
using Internal.Proto.BinaryTuple;
using Internal.Table;
using NUnit.Framework;

/// <summary>
/// Tests for <see cref="BinaryTupleIgniteTupleAdapter"/>. Ensures consistency with <see cref="IgniteTuple"/>.
/// </summary>
[TestFixture]
public class BinaryTupleIgniteTupleAdapterTests : IgniteTupleTests
{
    protected override string GetShortClassName() => nameof(BinaryTupleIgniteTupleAdapter);

    protected override IIgniteTuple CreateTuple(IIgniteTuple source)
    {
        var cols = new List<Column>();
        var builder = new BinaryTupleBuilder(source.FieldCount);

        for (var i = 0; i < source.FieldCount; i++)
        {
            var name = source.GetName(i);
            var val = source[i]!;
            var type = GetColumnType(val);
            var col = new Column(name, type, true, false, 0, i, 0, 0);

            cols.Add(col);
            builder.AppendObject(val, type);
        }

        var buf = builder.Build();
        var schema = new Schema(0, 0, 0, 0, cols);

        return new BinaryTupleIgniteTupleAdapter(buf, schema, cols.Count);

        static ColumnType GetColumnType(object? obj)
        {
            if (obj == null || obj is string)
            {
                return ColumnType.String;
            }

            if (obj is int)
            {
                return ColumnType.Int32;
            }

            if (obj is long)
            {
                return ColumnType.Int64;
            }

            throw new NotSupportedException("Unsupported type: " + obj.GetType());
        }
    }
}
