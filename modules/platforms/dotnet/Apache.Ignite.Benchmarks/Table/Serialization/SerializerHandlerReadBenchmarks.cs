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
    using Ignite.Table.Mapper;
    using Internal.Proto.BinaryTuple;
    using Internal.Proto.MsgPack;
    using Internal.Table.Serialization;

    /// <summary>
    /// Benchmarks for <see cref="IRecordSerializerHandler{T}"/> read methods.
    ///
    /// Results on i9-12900H, .NET SDK 8.0.416, Ubuntu 22.04:
    ///
    /// | Method               | Mean      | Error    | StdDev   | Ratio | RatioSD | Gen0   | Allocated |
    /// |--------------------- |----------:|---------:|---------:|------:|--------:|-------:|----------:|
    /// | ReadObjectManual     |  41.10 ns | 0.240 ns | 0.212 ns |  1.00 |    0.01 | 0.0002 |      80 B |
    /// | ReadObject           |  70.69 ns | 0.304 ns | 0.284 ns |  1.72 |    0.01 | 0.0002 |      80 B |
    /// | ReadObjectWithMapper |  46.33 ns | 0.645 ns | 0.603 ns |  1.13 |    0.02 | 0.0004 |     112 B |
    /// | ReadTuple            |  21.61 ns | 0.055 ns | 0.046 ns |  0.53 |    0.00 | 0.0004 |     112 B |
    /// | ReadTupleAndFields   | 134.81 ns | 0.783 ns | 0.654 ns |  3.28 |    0.02 | 0.0005 |     200 B |.
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
            var tupleReader = new BinaryTupleReader(reader.ReadBinary(), 3);
            var rowReader = new RowReader(ref tupleReader);

            Car res = Mapper.Read(ref rowReader, Schema.GetMapperSchema(false));
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
    }
}
