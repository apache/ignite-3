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

namespace Apache.Ignite.Benchmarks.Table.Serialization
{
    using System;
    using System.Collections.Generic;
    using BenchmarkDotNet.Engines;
    using Ignite.Sql;
    using Ignite.Table;
    using Ignite.Table.Mapper;
    using Internal.Buffers;
    using Internal.Table;
    using Internal.Table.Serialization;
    using Internal.Table.Serialization.Mappers;

    /// <summary>
    /// Base class for <see cref="IRecordSerializerHandler{T}"/> benchmarks.
    /// </summary>
    public abstract class SerializerHandlerBenchmarksBase
    {
        internal static readonly Car Object = new()
        {
            Id = Guid.NewGuid(),
            BodyType = "Sedan",
            Seats = 5
        };

        internal static readonly IgniteTuple Tuple = new()
        {
            [nameof(Car.Id)] = Object.Id,
            [nameof(Car.BodyType)] = Object.BodyType,
            [nameof(Car.Seats)] = Object.Seats
        };

        internal static readonly Schema Schema = Schema.CreateInstance(
            version: 1,
            tableId: 1,
            columns: new[]
            {
                new Column(nameof(Car.Id), ColumnType.Uuid, IsNullable: false, ColocationIndex: 0, KeyIndex: 0, SchemaIndex: 0, Scale: 0, Precision: 0),
                new Column(nameof(Car.BodyType), ColumnType.String, IsNullable: false, ColocationIndex: -1, KeyIndex: -1, SchemaIndex: 1, Scale: 0, Precision: 0),
                new Column(nameof(Car.Seats), ColumnType.Int32, IsNullable: false, ColocationIndex: -1, KeyIndex: -1, SchemaIndex: 2, Scale: 0, Precision: 0)
            });

        internal static readonly byte[] SerializedData = GetSerializedData();

        internal static readonly IRecordSerializerHandler<Car> ObjectSerializerHandler = new ObjectSerializerHandler<Car>();

        internal static readonly IRecordSerializerHandler<Car> MapperSerializerHandler = new MapperSerializerHandler<Car>(new CarMapper());

        internal static readonly IRecordSerializerHandler<Car> MapperKnownOrderSerializerHandler = new MapperSerializerHandler<Car>(new CarMapperKnownOrder());

        internal static readonly IRecordSerializerHandler<KvPair<Guid, string>> MapperPairSerializerHandler = new MapperPairSerializerHandler<Guid, string>(KeyValueMappers.TryCreate<Guid, string>()!);

        protected Consumer Consumer { get; } = new();

        internal static void VerifyWritten(PooledArrayBuffer pooledWriter)
        {
            var bytesWritten = pooledWriter.GetWrittenMemory().Length;

            if (bytesWritten != 31)
            {
                throw new Exception("Unexpected number of bytes written: " + bytesWritten);
            }
        }

        private static byte[] GetSerializedData()
        {
            using var pooledWriter = new PooledArrayBuffer();
            var writer = pooledWriter.MessageWriter;

            TupleSerializerHandler.Instance.Write(ref writer, Schema, Tuple);

            return pooledWriter.GetWrittenMemory().Slice(3).ToArray();
        }

        protected internal class Car
        {
            public Guid Id { get; set; }

            public string BodyType { get; set; } = null!;

            public int Seats { get; set; }
        }

        protected internal class CarMapper : IMapper<Car>
        {
            public void Write(Car obj, ref RowWriter rowWriter, IMapperSchema schema)
            {
                foreach (var column in schema.Columns)
                {
                    switch (column.Name)
                    {
                        case nameof(Car.Id):
                            rowWriter.WriteGuid(obj.Id);
                            break;

                        case nameof(Car.BodyType):
                            rowWriter.WriteString(obj.BodyType);
                            break;

                        case nameof(Car.Seats):
                            rowWriter.WriteInt(obj.Seats);
                            break;

                        default:
                            rowWriter.Skip();
                            break;
                    }
                }
            }

            public Car Read(ref RowReader rowReader, IMapperSchema schema)
            {
                var res = new Car();

                foreach (var column in schema.Columns)
                {
                    switch (column.Name)
                    {
                        case nameof(Car.Id):
                            res.Id = rowReader.ReadGuid()!.Value;
                            break;

                        case nameof(Car.BodyType):
                            res.BodyType = rowReader.ReadString()!;
                            break;

                        case nameof(Car.Seats):
                            res.Seats = rowReader.ReadInt()!.Value;
                            break;

                        default:
                            rowReader.Skip();
                            break;
                    }
                }

                return res;
            }
        }

        protected internal class CarMapperKnownOrder : IMapper<Car>
        {
            public void Write(Car obj, ref RowWriter rowWriter, IMapperSchema schema)
            {
                rowWriter.WriteGuid(obj.Id);
                rowWriter.WriteString(obj.BodyType);
                rowWriter.WriteInt(obj.Seats);
            }

            public Car Read(ref RowReader rowReader, IMapperSchema schema)
            {
                return new Car
                {
                    Id = rowReader.ReadGuid()!.Value,
                    BodyType = rowReader.ReadString()!,
                    Seats = rowReader.ReadInt()!.Value
                };
            }
        }
    }
}
