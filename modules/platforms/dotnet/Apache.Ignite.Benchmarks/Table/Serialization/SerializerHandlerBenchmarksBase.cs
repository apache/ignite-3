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
    using BenchmarkDotNet.Engines;
    using Ignite.Table;
    using Internal.Buffers;
    using Internal.Proto;
    using Internal.Table;
    using Internal.Table.Serialization;

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

        internal static readonly Schema Schema = new(1, 1, new[]
        {
            new Column(nameof(Car.Id), ClientDataType.Uuid, Nullable: false, IsKey: true, SchemaIndex: 0),
            new Column(nameof(Car.BodyType), ClientDataType.String, Nullable: false, IsKey: false, SchemaIndex: 1),
            new Column(nameof(Car.Seats), ClientDataType.Int32, Nullable: false, IsKey: false, SchemaIndex: 2)
        });

        internal static readonly byte[] SerializedData = GetSerializedData();

        internal static readonly ObjectSerializerHandler<Car> ObjectSerializerHandler = new();

        internal static readonly ObjectSerializerHandlerOld<Car> ObjectSerializerHandlerOld = new();

        protected Consumer Consumer { get; } = new();

        internal static void VerifyWritten(PooledArrayBufferWriter pooledWriter)
        {
            var bytesWritten = pooledWriter.GetWrittenMemory().Length;

            if (bytesWritten != 31)
            {
                throw new Exception("Unexpected number of bytes written: " + bytesWritten);
            }
        }

        private static byte[] GetSerializedData()
        {
            using var pooledWriter = new PooledArrayBufferWriter();
            var writer = pooledWriter.GetMessageWriter();

            TupleSerializerHandler.Instance.Write(ref writer, Schema, Tuple);

            writer.Flush();
            return pooledWriter.GetWrittenMemory().Slice(3).ToArray();
        }

        protected internal class Car
        {
            public Guid Id { get; set; }

            public string BodyType { get; set; } = null!;

            public int Seats { get; set; }
        }
    }
}
