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
    using System.Diagnostics.CodeAnalysis;
    using BenchmarkDotNet.Attributes;
    using Internal.Buffers;
    using Internal.Proto.BinaryTuple;
    using Internal.Table.Serialization;

    /// <summary>
    /// Benchmarks for <see cref="IRecordSerializerHandler{T}"/> write methods.
    ///
    /// Results on i9-12900H, .NET SDK 8.0.416, Ubuntu 22.04:
    ///
    /// | Method                | Mean     | Error   | StdDev  | Ratio | Gen0   | Allocated |
    /// |---------------------- |---------:|--------:|--------:|------:|-------:|----------:|
    /// | WriteObjectManual     | 118.9 ns | 0.57 ns | 0.50 ns |  1.00 | 0.0002 |      80 B |
    /// | WriteObject           | 134.8 ns | 0.49 ns | 0.43 ns |  1.13 | 0.0002 |      80 B |
    /// | WriteObjectWithMapper | 130.2 ns | 0.92 ns | 0.81 ns |  1.10 | 0.0002 |     112 B |
    /// | WriteTuple            | 186.3 ns | 0.68 ns | 0.60 ns |  1.57 | 0.0005 |     184 B |.
    /// </summary>
    [SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic", Justification = "Benchmarks.")]
    [MemoryDiagnoser]
    public class SerializerHandlerWriteBenchmarks : SerializerHandlerBenchmarksBase
    {
        [Benchmark(Baseline = true)]
        public void WriteObjectManual()
        {
            using var pooledWriter = new PooledArrayBuffer();
            var writer = pooledWriter.MessageWriter;

            using var tupleBuilder = new BinaryTupleBuilder(3);
            tupleBuilder.AppendGuid(Object.Id);
            tupleBuilder.AppendString(Object.BodyType);
            tupleBuilder.AppendInt(Object.Seats);

            writer.WriteBitSet(3);
            writer.Write(tupleBuilder.Build().Span);

            VerifyWritten(pooledWriter);
        }

        [Benchmark]
        public void WriteObject()
        {
            using var pooledWriter = new PooledArrayBuffer();
            var writer = pooledWriter.MessageWriter;

            ObjectSerializerHandler.Write(ref writer, Schema, Object);

            VerifyWritten(pooledWriter);
        }

        [Benchmark]
        public void WriteObjectWithMapper()
        {
            using var pooledWriter = new PooledArrayBuffer();
            var writer = pooledWriter.MessageWriter;

            MapperSerializerHandler.Write(ref writer, Schema, Object);

            VerifyWritten(pooledWriter);
        }

        [Benchmark]
        public void WriteObjectWithMapperKnownOrder()
        {
            using var pooledWriter = new PooledArrayBuffer();
            var writer = pooledWriter.MessageWriter;

            MapperKnownOrderSerializerHandler.Write(ref writer, Schema, Object);

            VerifyWritten(pooledWriter);
        }

        [Benchmark]
        public void WriteTuple()
        {
            using var pooledWriter = new PooledArrayBuffer();
            var writer = pooledWriter.MessageWriter;

            TupleSerializerHandler.Instance.Write(ref writer, Schema, Tuple);

            VerifyWritten(pooledWriter);
        }
    }
}
