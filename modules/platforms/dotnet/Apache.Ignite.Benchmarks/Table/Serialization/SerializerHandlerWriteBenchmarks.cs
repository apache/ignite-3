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
    using Internal.Proto;
    using Internal.Proto.BinaryTuple;
    using Internal.Table.Serialization;

    /// <summary>
    /// Benchmarks for <see cref="IRecordSerializerHandler{T}.Write"/> implementations.
    /// Results on Intel Core i7-9700K, .NET SDK 3.1.416, Ubuntu 20.04:
    /// |            Method |     Mean |   Error |  StdDev | Ratio | RatioSD |  Gen 0 | Allocated |
    /// |------------------ |---------:|--------:|--------:|------:|--------:|-------:|----------:|
    /// | WriteObjectManual | 155.8 ns | 1.15 ns | 1.07 ns |  1.00 |    0.00 | 0.0062 |      40 B |
    /// |       WriteObject | 167.0 ns | 0.76 ns | 0.75 ns |  1.07 |    0.01 | 0.0062 |      40 B |
    /// |        WriteTuple | 324.7 ns | 4.35 ns | 4.07 ns |  2.08 |    0.02 | 0.0229 |     144 B |
    /// |    WriteObjectOld | 798.5 ns | 5.10 ns | 4.77 ns |  5.13 |    0.04 | 0.0381 |     240 B |.
    /// </summary>
    [SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic", Justification = "Benchmarks.")]
    [MemoryDiagnoser]
    public class SerializerHandlerWriteBenchmarks : SerializerHandlerBenchmarksBase
    {
        [Benchmark(Baseline = true)]
        public void WriteObjectManual()
        {
            using var pooledWriter = new PooledArrayBufferWriter();
            var writer = pooledWriter.GetMessageWriter();

            using var tupleBuilder = new BinaryTupleBuilder(3);
            tupleBuilder.AppendGuid(Object.Id);
            tupleBuilder.AppendString(Object.BodyType);
            tupleBuilder.AppendInt(Object.Seats);

            writer.WriteBitSet(3);
            writer.Write(tupleBuilder.Build().Span);

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

        [Benchmark]
        public void WriteTuple()
        {
            using var pooledWriter = new PooledArrayBufferWriter();
            var writer = pooledWriter.GetMessageWriter();

            TupleSerializerHandler.Instance.Write(ref writer, Schema, Tuple);

            writer.Flush();
            VerifyWritten(pooledWriter);
        }

        // [Benchmark]
        public void WriteObjectOld()
        {
            using var pooledWriter = new PooledArrayBufferWriter();
            var writer = pooledWriter.GetMessageWriter();

            ObjectSerializerHandlerOld.Write(ref writer, Schema, Object);

            writer.Flush();
            VerifyWritten(pooledWriter);
        }
    }
}
