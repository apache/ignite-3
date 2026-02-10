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
    /// Benchmarks for <see cref="IRecordSerializerHandler{T}"/> read methods.
    ///
    /// Results on i9-12900H, .NET SDK 8.0.416, Ubuntu 22.04:
    ///
    /// | Method                         | Mean      | Error    | StdDev   | Ratio | Gen0   | Allocated | Alloc Ratio |
    /// |------------------------------- |----------:|---------:|---------:|------:|-------:|----------:|------------:|
    /// | ReadObjectManual               |  40.78 ns | 0.119 ns | 0.099 ns |  1.00 | 0.0002 |      80 B |        1.00 |
    /// | ReadObject                     |  69.94 ns | 0.287 ns | 0.268 ns |  1.72 | 0.0002 |      80 B |        1.00 |
    /// | ReadObjectWithMapper           |  52.73 ns | 0.205 ns | 0.192 ns |  1.29 | 0.0004 |     112 B |        1.40 |
    /// | ReadObjectWithMapperKnownOrder |  41.87 ns | 0.128 ns | 0.120 ns |  1.03 | 0.0002 |      80 B |        1.00 |
    /// | ReadTuple                      |  21.82 ns | 0.092 ns | 0.077 ns |  0.53 | 0.0004 |     112 B |        1.40 |
    /// | ReadTupleAndFields             | 132.84 ns | 0.433 ns | 0.384 ns |  3.26 | 0.0005 |     200 B |        2.50 |.
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
        public void ReadObjectWithMapper()
        {
            var reader = new MsgPackReader(SerializedData);
            Car res = MapperSerializerHandler.Read(ref reader, Schema);

            Consumer.Consume(res);
        }

        [Benchmark]
        public void ReadObjectWithMapperKnownOrder()
        {
            var reader = new MsgPackReader(SerializedData);
            Car res = MapperKnownOrderSerializerHandler.Read(ref reader, Schema);

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

            Consumer.Consume(res[0]!);
            Consumer.Consume(res[1]!);
            Consumer.Consume(res[2]!);
        }

        [Benchmark]
        public void ReadKeyValuePair()
        {
            var reader = new MsgPackReader(SerializedData);
            var res = MapperPairSerializerHandler.Read(ref reader, Schema);

            Consumer.Consume(res);
        }
    }
}
