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
                        rowWriter.Write(obj.Key);
                        break;

                    case "Val":
                        rowWriter.Write(obj.Val);
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
                        rowWriter.Write(obj.Key);
                        break;

                    case "Val":
                        rowWriter.Write(obj.Val);
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
