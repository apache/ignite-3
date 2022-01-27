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

#pragma warning disable SA1401 // Fields should be private.
namespace Apache.Ignite.Benchmarks.Table.Serialization
{
    using System;
    using System.Diagnostics.CodeAnalysis;
    using BenchmarkDotNet.Attributes;
    using Ignite.Table;
    using Internal.Buffers;
    using Internal.Proto;
    using Internal.Table;
    using Internal.Table.Serialization;

    /// <summary>
    /// Benchmarks for <see cref="IRecordSerializerHandler{T}"/> implementations.
    /// </summary>
    [SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic", Justification = "Benchmarks.")]
    [MemoryDiagnoser]
    public class SerializerHandlerBenchmarks
    {
        private static readonly Car Object = new()
        {
            Id = Guid.NewGuid(),
            BodyType = "Sedan",
            Seats = 5
        };

        private static readonly IgniteTuple Tuple = new()
        {
            [nameof(Car.Id)] = Object.Id,
            [nameof(Car.BodyType)] = Object.BodyType,
            [nameof(Car.Seats)] = Object.Seats
        };

        private static readonly Schema Schema = new(1, 1, new[]
        {
            new Column("Id", ClientDataType.Uuid, false, true, 0),
            new Column("Name", ClientDataType.String, false, true, 1),
            new Column("Seat", ClientDataType.Int32, false, true, 2)
        });

        private static readonly ObjectSerializerHandler<Car> ObjectSerializerHandler = new();

        [Benchmark]
        public void WriteTuple()
        {
            using var pooledWriter = new PooledArrayBufferWriter();
            var writer = pooledWriter.GetMessageWriter();

            TupleSerializerHandler.Instance.Write(ref writer, Schema, Tuple);
        }

        [Benchmark]
        public void WriteObject()
        {
            using var pooledWriter = new PooledArrayBufferWriter();
            var writer = pooledWriter.GetMessageWriter();

            ObjectSerializerHandler.Write(ref writer, Schema, Object);
        }

        private class Car
        {
            public Guid Id;

            public string BodyType = null!;

            public int Seats;
        }
    }
}
