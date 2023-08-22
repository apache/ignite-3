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
    using Internal.Proto.BinaryTuple;
    using Internal.Proto.MsgPack;
    using Internal.Table.Serialization;

    /// <summary>
    /// Benchmarks for <see cref="IRecordSerializerHandler{T}.Read"/> implementations.
    ///
    /// Results on i9-12900H, .NET SDK 6.0.405, Ubuntu 22.04:
    ///
    /// |             Method |      Mean |    Error |   StdDev | Ratio | RatioSD |  Gen 0 | Allocated |
    /// |------------------- |----------:|---------:|---------:|------:|--------:|-------:|----------:|
    /// |   ReadObjectManual |  53.23 ns | 0.320 ns | 0.268 ns |  1.00 |    0.00 | 0.0003 |      80 B |
    /// |         ReadObject |  88.01 ns | 0.484 ns | 0.453 ns |  1.65 |    0.01 | 0.0002 |      80 B |
    /// |          ReadTuple |  23.66 ns | 0.274 ns | 0.257 ns |  0.44 |    0.00 | 0.0004 |     120 B |
    /// | ReadTupleAndFields | 136.34 ns | 2.014 ns | 1.884 ns |  2.56 |    0.04 | 0.0007 |     208 B |.
    /// </summary>
    [SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic", Justification = "Benchmarks.")]
    [MemoryDiagnoser]
    public class SerializerHandlerReadBenchmarks : SerializerHandlerBenchmarksBase
    {
        [Benchmark(Baseline = true)]
        public void ReadObjectManual()
        {
            var reader = new MsgPackReader(SerializedData);
            var tupleReader = new BinaryTupleReader(reader.ReadBinary(), 3);

            var res = new Car
            {
                Id = tupleReader.GetGuid(0),
                BodyType = tupleReader.GetString(1),
                Seats = tupleReader.GetInt(2)
            };

            Consumer.Consume(res);
        }

        [Benchmark]
        public void ReadObject()
        {
            var reader = new MsgPackReader(SerializedData);
            var res = ObjectSerializerHandler.Read(ref reader, Schema);

            Consumer.Consume(res);
        }

        [Benchmark]
        public void ReadTuple()
        {
            var reader = new MsgPackReader(SerializedData);
            var res = TupleSerializerHandler.Instance.Read(ref reader, Schema);

            Consumer.Consume(res);
        }

        [Benchmark]
        public void ReadTupleAndFields()
        {
            var reader = new MsgPackReader(SerializedData);
            var res = TupleSerializerHandler.Instance.Read(ref reader, Schema);

            Consumer.Consume(res[0]);
            Consumer.Consume(res[1]);
            Consumer.Consume(res[2]);
        }
    }
}
