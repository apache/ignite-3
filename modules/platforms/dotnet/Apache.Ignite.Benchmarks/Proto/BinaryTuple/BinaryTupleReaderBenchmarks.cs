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

namespace Apache.Ignite.Benchmarks.Proto.BinaryTuple;

using BenchmarkDotNet.Attributes;
using Internal.Proto.BinaryTuple;

/// <summary>
/// Binary tuple reader benchmarks. Compares nullable and non-nullable reads.
/// </summary>
public class BinaryTupleReaderBenchmarks
{
    private const int NumElements = 4;
    private static readonly byte[] Bytes = WriteTuple();

    [Benchmark]
    public int GetInt()
    {
        var reader = new BinaryTupleReader(Bytes, NumElements);

        return reader.GetInt(1);
    }

    [Benchmark]
    public int? GetIntNullable()
    {
        var reader = new BinaryTupleReader(Bytes, NumElements);

        return reader.GetIntNullable(1);
    }

    [Benchmark]
    public sbyte GetByte()
    {
        var reader = new BinaryTupleReader(Bytes, NumElements);

        return reader.GetByte(2);
    }

    [Benchmark]
    public sbyte? GetByteNullable()
    {
        var reader = new BinaryTupleReader(Bytes, NumElements);

        return reader.GetByteNullable(2);
    }

    private static byte[] WriteTuple()
    {
        using var writer = new BinaryTupleBuilder(numElements: NumElements);

        writer.AppendNull();
        writer.AppendInt(123);
        writer.AppendByte(96);
        writer.AppendString("foo");

        return writer.Build().ToArray();
    }
}
