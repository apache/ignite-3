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
    /// Results on Intel Core i7-9700K, .NET SDK 3.1.416, Ubuntu 20.04:
    /// |           Method |       Mean |   Error |  StdDev | Ratio | RatioSD |  Gen 0 | Allocated |
    /// |----------------- |-----------:|--------:|--------:|------:|--------:|-------:|----------:|
    /// | ReadObjectManual |   210.9 ns | 0.73 ns | 0.65 ns |  1.00 |    0.00 | 0.0126 |      80 B |
    /// |       ReadObject |   257.5 ns | 1.41 ns | 1.25 ns |  1.22 |    0.01 | 0.0124 |      80 B |
    /// |        ReadTuple |   561.0 ns | 3.09 ns | 2.89 ns |  2.66 |    0.01 | 0.0849 |     536 B |
    /// |    ReadObjectOld | 1,020.9 ns | 9.05 ns | 8.47 ns |  4.84 |    0.05 | 0.0744 |     472 B |.
    ///
    /// Results on i7-7700HQ, .NET SDK 6.0.400, Ubuntu 20.04:
    /// MsgPack (old)
    /// |           Method |     Mean |   Error |  StdDev | Ratio | RatioSD |  Gen 0 | Allocated |
    /// |----------------- |---------:|--------:|--------:|------:|--------:|-------:|----------:|
    /// | ReadObjectManual | 289.4 ns | 2.92 ns | 2.59 ns |  1.00 |    0.00 | 0.0024 |      80 B |
    /// |       ReadObject | 364.3 ns | 3.28 ns | 3.07 ns |  1.26 |    0.02 | 0.0024 |      80 B |
    /// |        ReadTuple | 755.4 ns | 2.82 ns | 2.35 ns |  2.61 |    0.03 | 0.0181 |     536 B |
    ///
    /// BinaryTuple (new)
    /// |           Method |     Mean |   Error |  StdDev | Ratio | RatioSD |  Gen 0 | Allocated |
    /// |----------------- |---------:|--------:|--------:|------:|--------:|-------:|----------:|
    /// | ReadObjectManual | 299.3 ns | 3.42 ns | 3.20 ns |  1.00 |    0.00 | 0.0024 |      80 B |
    /// |       ReadObject | 382.9 ns | 2.49 ns | 2.21 ns |  1.28 |    0.02 | 0.0024 |      80 B |
    /// |        ReadTuple | 769.0 ns | 6.06 ns | 5.37 ns |  2.57 |    0.04 | 0.0181 |     536 B |.
    ///
    /// Comparison of MessagePack library and our own implementation, i9-12900H, .NET SDK 6.0.405, Ubuntu 22.04:
    ///
    /// MessagePack 2.1.90 (old)
    /// |           Method |     Mean |   Error |  StdDev | Ratio | RatioSD |  Gen 0 | Allocated |
    /// |----------------- |---------:|--------:|--------:|------:|--------:|-------:|----------:|
    /// | ReadObjectManual | 100.3 ns | 0.46 ns | 0.41 ns |  1.00 |    0.00 | 0.0002 |      80 B |
    /// |       ReadObject | 142.3 ns | 0.35 ns | 0.31 ns |  1.42 |    0.01 | 0.0002 |      80 B |
    /// |        ReadTuple | 266.8 ns | 2.52 ns | 2.35 ns |  2.66 |    0.03 | 0.0019 |     544 B |.
    ///
    /// Custom MsgPackReader (new)
    /// |           Method |      Mean |    Error |   StdDev | Ratio | RatioSD |  Gen 0 | Allocated |
    /// |----------------- |----------:|---------:|---------:|------:|--------:|-------:|----------:|
    /// | ReadObjectManual |  38.30 ns | 0.265 ns | 0.247 ns |  1.00 |    0.00 | 0.0003 |      80 B |
    /// |       ReadObject |  80.51 ns | 0.158 ns | 0.124 ns |  2.10 |    0.01 | 0.0002 |      80 B |
    /// |        ReadTuple | 208.63 ns | 0.654 ns | 0.611 ns |  5.45 |    0.04 | 0.0019 |     544 B |.
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

            Consumer.Consume(res[nameof(Car.Id)]);
            Consumer.Consume(res[nameof(Car.BodyType)]);
            Consumer.Consume(res[nameof(Car.Seats)]);
        }
    }
}
