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
    /// Benchmarks for <see cref="IRecordSerializerHandler{T}.Write(ref Apache.Ignite.Internal.Proto.MsgPack.MsgPackWriter,Apache.Ignite.Internal.Table.Schema,T,bool,bool)"/> implementations.
    ///
    /// Comparison of MessagePack library and our own implementation, i9-12900H, .NET SDK 6.0.405, Ubuntu 22.04:
    ///
    /// MessagePack 2.1.90 (old):
    ///
    /// |            Method |     Mean |   Error |  StdDev | Ratio |  Gen 0 | Allocated |
    /// |------------------ |---------:|--------:|--------:|------:|-------:|----------:|
    /// | WriteObjectManual | 189.6 ns | 0.55 ns | 0.51 ns |  1.00 | 0.0002 |      80 B |
    /// |       WriteObject | 221.7 ns | 0.78 ns | 0.73 ns |  1.17 | 0.0002 |      80 B |
    /// |        WriteTuple | 310.3 ns | 1.59 ns | 1.41 ns |  1.64 | 0.0005 |     184 B |
    ///
    /// Custom MsgPack (new):
    ///
    /// |            Method |     Mean |   Error |  StdDev | Ratio |  Gen 0 | Allocated |
    /// |------------------ |---------:|--------:|--------:|------:|-------:|----------:|
    /// | WriteObjectManual | 135.7 ns | 1.13 ns | 1.06 ns |  1.00 | 0.0002 |      80 B |
    /// |       WriteObject | 161.2 ns | 0.59 ns | 0.52 ns |  1.19 | 0.0002 |      80 B |
    /// |        WriteTuple | 250.5 ns | 0.91 ns | 0.85 ns |  1.85 | 0.0005 |     184 B |.
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
        public void WriteTuple()
        {
            using var pooledWriter = new PooledArrayBuffer();
            var writer = pooledWriter.MessageWriter;

            TupleSerializerHandler.Instance.Write(ref writer, Schema, Tuple);

            VerifyWritten(pooledWriter);
        }
    }
}
