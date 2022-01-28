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
    using Internal.Proto;
    using Internal.Table.Serialization;
    using MessagePack;

    /// <summary>
    /// Benchmarks for <see cref="IRecordSerializerHandler{T}.Read"/> implementations.
    /// Results on Intel Core i7-9700K, .NET SDK 3.1.416, Ubuntu 20.04:
    /// |           Method |       Mean |   Error |  StdDev | Ratio | RatioSD |  Gen 0 | Allocated |
    /// |----------------- |-----------:|--------:|--------:|------:|--------:|-------:|----------:|
    /// | ReadObjectManual |   180.5 ns | 1.36 ns | 1.28 ns |  1.00 |    0.00 | 0.0126 |      80 B |
    /// |       ReadObject |   257.5 ns | 1.38 ns | 1.22 ns |  1.43 |    0.01 | 0.0124 |      80 B |
    /// |        ReadTuple |   554.2 ns | 5.91 ns | 5.52 ns |  3.07 |    0.02 | 0.0849 |     536 B |
    /// |    ReadObjectOld | 1,011.3 ns | 5.00 ns | 4.18 ns |  5.59 |    0.04 | 0.0744 |     472 B |.
    /// </summary>
    [SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic", Justification = "Benchmarks.")]
    [MemoryDiagnoser]
    public class SerializerHandlerReadBenchmarks : SerializerHandlerBenchmarksBase
    {
        [Benchmark(Baseline = true)]
        public void ReadObjectManual()
        {
            var reader = new MessagePackReader(SerializedData);

            var res = new Car
            {
                Id = reader.TryReadNoValue() ? default : reader.ReadGuid(),
                BodyType = reader.TryReadNoValue() ? default! : reader.ReadString(),
                Seats = reader.TryReadNoValue() ? default : reader.ReadInt32()
            };

            Consumer.Consume(res);
        }

        [Benchmark]
        public void ReadObject()
        {
            var reader = new MessagePackReader(SerializedData);
            var res = ObjectSerializerHandler.Read(ref reader, Schema);

            Consumer.Consume(res);
        }

        [Benchmark]
        public void ReadTuple()
        {
            var reader = new MessagePackReader(SerializedData);
            var res = TupleSerializerHandler.Instance.Read(ref reader, Schema);

            Consumer.Consume(res);
        }

        [Benchmark]
        public void ReadObjectOld()
        {
            var reader = new MessagePackReader(SerializedData);
            var res = ObjectSerializerHandlerOld.Read(ref reader, Schema);

            Consumer.Consume(res);
        }
    }
}
