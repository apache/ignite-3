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

// ReSharper disable UnusedAutoPropertyAccessor.Local
namespace Apache.Ignite.Tests.Table.Serialization;

using System;
using Ignite.Sql;
using Ignite.Table;
using Ignite.Table.Mapper;
using Internal.Buffers;
using Internal.Table;
using Internal.Table.Serialization;
using NUnit.Framework;

/// <summary>
/// Tests that different serializers produce consistent results.
/// </summary>
public class SerializerHandlerConsistencyTests
{
    private const int ExpectedColocationHash = 1326971215;

    private static readonly Schema Schema = Schema.CreateInstance(
        version: 0,
        tableId: 0,
        columns: new[]
        {
            new Column("val1", ColumnType.String, false, KeyIndex: -1, ColocationIndex: -1, SchemaIndex: 0, 0, 0),
            new Column("key1", ColumnType.Int32, false, KeyIndex: 1, ColocationIndex: 0, SchemaIndex: 1, 0, 0),
            new Column("val2", ColumnType.Uuid, false, KeyIndex: -1, ColocationIndex: -1, SchemaIndex: 2, 0, 0),
            new Column("key2", ColumnType.String, false, KeyIndex: 0, ColocationIndex: 1, SchemaIndex: 3, 0, 0)
        });

    [Test]
    public void TestSerializationAndHashing([Values(true, false)] bool keyOnly)
    {
        var tupleHandler = TupleSerializerHandler.Instance;
        var tupleKvHandler = TuplePairSerializerHandler.Instance;
        var objectHandler = new ObjectSerializerHandler<Poco>();
        var objectKvHandler = new ObjectSerializerHandler<KvPair<PocoKey, PocoVal>>();
        var mapperHandler = new MapperSerializerHandler<Poco>(new PocoMapper());
        var mapperKvHandler = new MapperSerializerHandler<KvPair<PocoKey, PocoVal>>(new PocoKvMapper());

        var poco = new Poco
        {
            Val1 = "v1",
            Key1 = 123,
            Val2 = Guid.NewGuid(),
            Key2 = "k2"
        };

        var pocoKv = new KvPair<PocoKey, PocoVal>
        {
            Key = new PocoKey
            {
                Key1 = poco.Key1,
                Key2 = poco.Key2
            },
            Val = new PocoVal
            {
                Val1 = poco.Val1,
                Val2 = poco.Val2
            }
        };

        var tuple = new IgniteTuple
        {
            ["key1"] = poco.Key1,
            ["key2"] = poco.Key2
        };

        if (!keyOnly)
        {
            tuple["val1"] = poco.Val1;
            tuple["val2"] = poco.Val2;
        }

        var tupleKv = new KvPair<IIgniteTuple, IIgniteTuple>(
            new IgniteTuple
            {
                ["key1"] = poco.Key1,
                ["key2"] = poco.Key2
            },
            new IgniteTuple());

        if (!keyOnly)
        {
            tupleKv.Val["val1"] = poco.Val1;
            tupleKv.Val["val2"] = poco.Val2;
        }

        var (tupleBuf, tupleHash) = Serialize(tupleHandler, tuple, keyOnly);
        var (tupleKvBuf, tupleKvHash) = Serialize(tupleKvHandler, tupleKv, keyOnly);
        var (pocoBuf, pocoHash) = Serialize(objectHandler, poco, keyOnly);
        var (pocoKvBuf, pocoKvHash) = Serialize(objectKvHandler, pocoKv, keyOnly);
        var (mapperBuf, mapperHash) = Serialize(mapperHandler, poco, keyOnly);

        Assert.AreEqual(ExpectedColocationHash, tupleHash);
        Assert.AreEqual(ExpectedColocationHash, tupleKvHash);
        Assert.AreEqual(ExpectedColocationHash, pocoHash);
        Assert.AreEqual(ExpectedColocationHash, pocoKvHash);
        Assert.AreEqual(ExpectedColocationHash, mapperHash);

        CollectionAssert.AreEqual(tupleBuf, tupleKvBuf);
        CollectionAssert.AreEqual(tupleBuf, pocoBuf);
        CollectionAssert.AreEqual(tupleBuf, pocoKvBuf);
        CollectionAssert.AreEqual(tupleBuf, mapperBuf);
    }

    private static (byte[] Buf, int Hash) Serialize<T>(IRecordSerializerHandler<T> handler, T obj, bool keyOnly = false)
    {
        using var buf = new PooledArrayBuffer();

        var writer = buf.MessageWriter;
        var hash = handler.Write(ref writer, Schema, obj, keyOnly, computeHash: true);

        return (buf.GetWrittenMemory().ToArray(), hash);
    }

    private class Poco
    {
        public string? Val1 { get; set; }

        public int Key1 { get; set; }

        public Guid Val2 { get; set; }

        public string Key2 { get; set; } = string.Empty;
    }

    private class PocoKey
    {
        public int Key1 { get; set; }

        public string Key2 { get; set; } = string.Empty;
    }

    private class PocoVal
    {
        public string? Val1 { get; set; }

        public Guid Val2 { get; set; }
    }

    private class PocoMapper : IMapper<Poco>
    {
        public void Write(Poco obj, ref RowWriter rowWriter, IMapperSchema schema)
        {
            foreach (var column in schema.Columns)
            {
                switch (column.Name)
                {
                    case "val1":
                        rowWriter.WriteString(obj.Val1);
                        break;

                    case "key1":
                        rowWriter.WriteInt(obj.Key1);
                        break;

                    case "val2":
                        rowWriter.WriteGuid(obj.Val2);
                        break;

                    case "key2":
                        rowWriter.WriteString(obj.Key2);
                        break;

                    default:
                        rowWriter.Skip();
                        break;
                }
            }
        }

        public Poco Read(ref RowReader rowReader, IMapperSchema schema)
        {
            var res = new Poco();

            foreach (var column in schema.Columns)
            {
                switch (column.Name)
                {
                    case "val1":
                        res.Val1 = rowReader.ReadString();
                        break;

                    case "key1":
                        res.Key1 = rowReader.ReadInt()!.Value;
                        break;

                    case "val2":
                        res.Val2 = rowReader.ReadGuid()!.Value;
                        break;

                    case "key2":
                        res.Key2 = rowReader.ReadString()!;
                        break;

                    default:
                        rowReader.Skip();
                        break;
                }
            }

            return res;
        }
    }
}
