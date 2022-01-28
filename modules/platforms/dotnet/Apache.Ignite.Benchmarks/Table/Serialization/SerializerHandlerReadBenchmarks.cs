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
    using Internal.Table.Serialization;
    using MessagePack;

    /// <summary>
    /// Benchmarks for <see cref="IRecordSerializerHandler{T}.Read"/> implementations.
    /// Results on Intel Core i7-9700K, .NET SDK 3.1.416, Ubuntu 20.04:
    /// |        Method |       Mean |   Error |  StdDev | Ratio |  Gen 0 | Allocated |
    /// |-------------- |-----------:|--------:|--------:|------:|-------:|----------:|
    /// |     ReadTuple |   555.6 ns | 2.56 ns | 2.40 ns |  0.53 | 0.0849 |     536 B |
    /// |    ReadObject |   262.1 ns | 0.37 ns | 0.33 ns |  0.25 | 0.0124 |      80 B |
    /// | ReadObjectOld | 1,040.9 ns | 3.93 ns | 3.68 ns |  1.00 | 0.0744 |     472 B |.
    /// </summary>
    [SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic", Justification = "Benchmarks.")]
    [MemoryDiagnoser]
    public class SerializerHandlerReadBenchmarks : SerializerHandlerBenchmarksBase
    {
        [Benchmark]
        public void ReadTuple()
        {
            var reader = new MessagePackReader(SerializedData);
            TupleSerializerHandler.Instance.Read(ref reader, Schema);
        }

        [Benchmark]
        public void ReadObject()
        {
            var reader = new MessagePackReader(SerializedData);
            ObjectSerializerHandler.Read(ref reader, Schema);
        }

        [Benchmark(Baseline = true)]
        public void ReadObjectOld()
        {
            var reader = new MessagePackReader(SerializedData);
            ObjectSerializerHandlerOld.Read(ref reader, Schema);
        }
    }
}
