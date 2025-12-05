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
    /// Results on i9-12900H, .NET SDK 6.0.419, Ubuntu 22.04:
    ///
    /// |            Method |     Mean |   Error |  StdDev | Ratio | RatioSD |  Gen 0 | Allocated |
    /// |------------------ |---------:|--------:|--------:|------:|--------:|-------:|----------:|
    /// | WriteObjectManual | 116.4 ns | 1.33 ns | 1.11 ns |  1.00 |    0.00 | 0.0002 |      80 B |
    /// |       WriteObject | 137.5 ns | 1.26 ns | 1.18 ns |  1.18 |    0.01 | 0.0002 |      80 B |
    /// |        WriteTuple | 213.3 ns | 2.76 ns | 2.59 ns |  1.83 |    0.02 | 0.0007 |     184 B |.
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
        public void WriteTuple()
        {
            using var pooledWriter = new PooledArrayBuffer();
            var writer = pooledWriter.MessageWriter;

            TupleSerializerHandler.Instance.Write(ref writer, Schema, Tuple);

            VerifyWritten(pooledWriter);
        }
    }
}
