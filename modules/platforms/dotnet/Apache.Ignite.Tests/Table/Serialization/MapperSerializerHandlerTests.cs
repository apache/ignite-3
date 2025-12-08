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

namespace Apache.Ignite.Tests.Table.Serialization;

using System;
using Ignite.Table.Mapper;
using Internal.Table.Serialization;
using NUnit.Framework;

/// <summary>
/// Tests for <see cref="MapperSerializerHandler{T}"/>.
/// </summary>
[TestFixture]
internal class MapperSerializerHandlerTests : SerializerHandlerTestBase
{
    [Test]
    public override void TestReadUnsupportedFieldTypeThrowsException()
    {
        var ex = Assert.Throws<IgniteClientException>(() =>
        {
            var reader = WriteAndGetReader();
            GetHandler<BadPoco>().Read(ref reader, Schema);
        });

        Assert.AreEqual("Can't read a value of type 'Uuid' from column 'Key' of type 'Int64'.", ex!.Message);
    }

    [Test]
    public override void TestWriteUnsupportedFieldTypeThrowsException()
    {
        var ex = Assert.Throws<IgniteClientException>(() => Write(new BadPoco(Guid.Empty, DateTimeOffset.Now)));

        Assert.AreEqual("Can't write a value of type 'Uuid' to column 'Key' of type 'Int64'.", ex!.Message);
    }

    [Test]
    public void TestWriteLessColumnsThanSchemaThrowsException()
    {
        var ex = Assert.Throws<IgniteClientException>(() => Write(new Poco { Key = 1234, Val = "skip-me"}));

        StringAssert.StartsWith("Not all columns were written by the mapper. Expected: 2, written: 1, schema: : [Column { Name = Key, Type = Int64", ex!.Message);
    }

    [Test]
    public void TestWriteMoreColumnsThanSchemaThrowsException()
    {
        var ex = Assert.Throws<IgniteClientException>(() => Write(new Poco { Key = 1234, Val = "write-more"}));

        StringAssert.StartsWith("Attempted to write more columns than defined in the schema: [Column { Name = Key, Type = Int64", ex.Message);
    }

    protected override IRecordSerializerHandler<T> GetHandler<T>()
    {
        if (typeof(T) == typeof(Poco))
        {
            return new MapperSerializerHandler<T>((IMapper<T>)new PocoMapper());
        }

        if (typeof(T) == typeof(BadPoco))
        {
            return new MapperSerializerHandler<T>((IMapper<T>)new BadPocoMapper());
        }

        throw new NotSupportedException($"Type '{typeof(T)}' is not supported.");
    }

    public class PocoMapper : IMapper<Poco>
    {
        public void Write(Poco obj, ref RowWriter rowWriter, IMapperSchema schema)
        {
            foreach (var column in schema.Columns)
            {
                switch (column.Name)
                {
                    case "Key":
                        rowWriter.WriteLong(obj.Key);
                        break;

                    case "Val":
                        if (obj.Val != "skip-me")
                        {
                            rowWriter.WriteString(obj.Val);
                        }

                        break;

                    default:
                        rowWriter.Skip();
                        break;
                }
            }

            if (obj.Val == "write-more")
            {
                rowWriter.WriteString("extra-value");
            }
        }

        public Poco Read(ref RowReader rowReader, IMapperSchema schema)
        {
            var res = new Poco();

            foreach (var column in schema.Columns)
            {
                switch (column.Name)
                {
                    case "Key":
                        res.Key = rowReader.ReadLong()!.Value;
                        break;

                    case "Val":
                        res.Val = rowReader.ReadString();
                        break;

                    default:
                        rowReader.Skip();
                        break;
                }
            }

            return res;
        }
    }

    public class BadPocoMapper : IMapper<BadPoco>
    {
        public void Write(BadPoco obj, ref RowWriter rowWriter, IMapperSchema schema)
        {
            foreach (var column in schema.Columns)
            {
                switch (column.Name)
                {
                    case "Key":
                        rowWriter.WriteGuid(obj.Key);
                        break;

                    default:
                        rowWriter.Skip();
                        break;
                }
            }
        }

        public BadPoco Read(ref RowReader rowReader, IMapperSchema schema)
        {
            Guid key = Guid.Empty;

            foreach (var column in schema.Columns)
            {
                switch (column.Name)
                {
                    case "Key":
                        key = rowReader.ReadGuid()!.Value;
                        break;

                    default:
                        rowReader.Skip();
                        break;
                }
            }

            return new BadPoco(key, default);
        }
    }
}
