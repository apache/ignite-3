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
    using System.Diagnostics.CodeAnalysis;
    using BenchmarkDotNet.Attributes;
    using Ignite.Table;
    using Internal.Buffers;
    using Internal.Proto;
    using Internal.Table;
    using Internal.Table.Serialization;

    /// <summary>
    /// Benchmarks for <see cref="IRecordSerializerHandler{T}"/> implementations.
    /// Results on Intel Core i7-9700K, .NET SDK 3.1.416, Ubuntu 20.04:
    /// |         Method |     Mean |   Error |  StdDev | Ratio |  Gen 0 | Allocated |
    /// |--------------- |---------:|--------:|--------:|------:|-------:|----------:|
    /// |     WriteTuple | 331.6 ns | 2.66 ns | 2.49 ns |  0.42 | 0.0229 |     144 B |
    /// |    WriteObject | 163.2 ns | 3.19 ns | 3.42 ns |  0.21 | 0.0062 |      40 B |
    /// | WriteObjectOld | 786.7 ns | 3.19 ns | 2.98 ns |  1.00 | 0.0381 |     240 B |.
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
            new Column(nameof(Car.Id), ClientDataType.Uuid, Nullable: false, IsKey: true, SchemaIndex: 0),
            new Column(nameof(Car.BodyType), ClientDataType.String, Nullable: false, IsKey: false, SchemaIndex: 1),
            new Column(nameof(Car.Seats), ClientDataType.Int32, Nullable: false, IsKey: false, SchemaIndex: 2)
        });

        private static readonly ObjectSerializerHandler<Car> ObjectSerializerHandler = new();

        private static readonly ObjectSerializerHandlerOld<Car> ObjectSerializerHandlerOld = new();

        [Benchmark]
        public void WriteTuple()
        {
            using var pooledWriter = new PooledArrayBufferWriter();
            var writer = pooledWriter.GetMessageWriter();

            TupleSerializerHandler.Instance.Write(ref writer, Schema, Tuple);

            writer.Flush();
            VerifyWritten(pooledWriter);
        }

        [Benchmark]
        public void WriteObject()
        {
            using var pooledWriter = new PooledArrayBufferWriter();
            var writer = pooledWriter.GetMessageWriter();

            ObjectSerializerHandler.Write(ref writer, Schema, Object);

            writer.Flush();
            VerifyWritten(pooledWriter);
        }

        [Benchmark(Baseline = true)]
        public void WriteObjectOld()
        {
            using var pooledWriter = new PooledArrayBufferWriter();
            var writer = pooledWriter.GetMessageWriter();

            ObjectSerializerHandlerOld.Write(ref writer, Schema, Object);

            writer.Flush();
            VerifyWritten(pooledWriter);
        }

        private static void VerifyWritten(PooledArrayBufferWriter pooledWriter)
        {
            var bytesWritten = pooledWriter.GetWrittenMemory().Length;

            if (bytesWritten != 29)
            {
                throw new Exception("Unexpected number of bytes written: " + bytesWritten);
            }
        }

        private class Car
        {
            public Guid Id { get; set; }

            public string BodyType { get; set; } = null!;

            public int Seats { get; set; }
        }
    }
}
