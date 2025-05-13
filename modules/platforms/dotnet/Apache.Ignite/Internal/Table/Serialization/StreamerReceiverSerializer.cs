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

namespace Apache.Ignite.Internal.Table.Serialization;

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using Buffers;
using Compute;
using Ignite.Table;
using Proto.BinaryTuple;
using Proto.MsgPack;

/// <summary>
/// Streamer receiver serializer.
/// Corresponds to org.apache.ignite.internal.client.proto.StreamerReceiverSerializer.
/// </summary>
internal static class StreamerReceiverSerializer
{
    /// <summary>
    /// Writes receiver info.
    /// </summary>
    /// <param name="w">Writer.</param>
    /// <param name="className">Receiver class name.</param>
    /// <param name="arg">Receiver argument.</param>
    /// <param name="items">Receiver items.</param>
    /// <typeparam name="T">Item type.</typeparam>
    public static void WriteReceiverInfo<T>(
        ref MsgPackWriter w,
        string className,
        object? arg,
        ArraySegment<T> items)
    {
        Debug.Assert(items.Count > 0, "items.Count > 0");

        // className + arg + items size + item type + items.
        int binaryTupleSize = 1 + 3 + 1 + 1 + items.Count;
        using var builder = new BinaryTupleBuilder(binaryTupleSize);

        builder.AppendString(className);

        if (arg is IIgniteTuple tupleArg)
        {
            builder.AppendInt(TupleWithSchemaMarshalling.TypeIdTuple);
            builder.AppendInt(0); // Scale.
            builder.AppendBytes(static (bufWriter, arg) => TupleWithSchemaMarshalling.Pack(bufWriter, arg), tupleArg);
        }
        else
        {
            builder.AppendObjectWithType(arg);
        }

        if (items[0] is IIgniteTuple)
        {
            builder.AppendInt(TupleWithSchemaMarshalling.TypeIdTuple);
            builder.AppendInt(items.Count);

            foreach (var item in items)
            {
                builder.AppendBytes(static (bufWriter, arg) => TupleWithSchemaMarshalling.Pack(bufWriter, (IIgniteTuple)arg!), item);
            }
        }
        else
        {
            builder.AppendObjectCollectionWithType(items);
        }

        w.Write(binaryTupleSize);
        w.Write(builder.Build().Span);
    }

    /// <summary>
    /// Unwraps receiver info from the job argument buffer.
    /// Performs simple offset calculations and can be called multiple times.
    /// </summary>
    /// <param name="jobArgBuf">Job argument buffer.</param>
    /// <returns>Binary tuple reader with streamer receiver info.</returns>
    public static BinaryTupleReader GetReceiverInfoReaderFast(PooledBuffer jobArgBuf)
    {
        var r = jobArgBuf.GetReader();

        // Excerpt from ComputePacker.
        int argType = r.ReadInt32();
        Debug.Assert(argType == ComputePacker.Native, $"Expected Native type, got: {argType}");

        // Excerpt from ReadObjectFromBinaryTuple.
        ReadOnlySpan<byte> tupleSpan = r.ReadBinary();
        var binTuple = new BinaryTupleReader(tupleSpan, 3);

        ReadOnlySpan<byte> receiverInfoSpan = binTuple.GetBytesSpan(2);
        int receiverElementCount = BinaryPrimitives.ReadInt32LittleEndian(receiverInfoSpan);

        return new BinaryTupleReader(receiverInfoSpan[4..], receiverElementCount);
    }

    /// <summary>
    /// Writes receiver execution results. Opposite of <see cref="ReadReceiverResults{T}"/>.
    /// </summary>
    /// <param name="w">Writer.</param>
    /// <param name="res">Results.</param>
    /// <typeparam name="T">Result item type.</typeparam>
    public static void WriteReceiverResults<T>(MsgPackWriter w, IList<T>? res)
    {
        if (res == null)
        {
            w.WriteNil();
            return;
        }

        int resTupleElementCount = res.Count + 2;

        // Reserve a 4-byte prefix for resTupleElementCount.
        using var builder = new BinaryTupleBuilder(resTupleElementCount, prefixSize: 4);

        if (res.Count > 0 && res[0] is IIgniteTuple)
        {
            // TODO: Deduplicate this.
            builder.AppendInt(TupleWithSchemaMarshalling.TypeIdTuple);
            builder.AppendInt(res.Count);

            foreach (var item in res)
            {
                builder.AppendBytes(static (bufWriter, arg) => TupleWithSchemaMarshalling.Pack(bufWriter, (IIgniteTuple)arg!), item);
            }
        }
        else
        {
            builder.AppendObjectCollectionWithType(res);
        }

        Memory<byte> jobResultTupleMemWithPrefix = builder.Build();
        BinaryPrimitives.WriteInt32LittleEndian(jobResultTupleMemWithPrefix.Span, resTupleElementCount);
        ComputePacker.PackArgOrResult(ref w, jobResultTupleMemWithPrefix, null);
    }

    /// <summary>
    /// Reads receiver execution results. Opposite of <see cref="WriteReceiverResults{T}"/>.
    /// </summary>
    /// <param name="reader">Reader.</param>
    /// <typeparam name="T">Result element type.</typeparam>
    /// <returns>Pooled array with results and the actual element count.</returns>
    public static (T[]? ResultsPooledArray, int ResultsCount) ReadReceiverResults<T>(MsgPackReader reader)
    {
        if (reader.TryReadNil())
        {
            return (null, 0);
        }

        var numElements = reader.ReadInt32();
        if (numElements == 0)
        {
            return (null, 0);
        }

        var tuple = new BinaryTupleReader(reader.ReadBinary(), numElements);
        if (tuple.GetInt(0) != TupleWithSchemaMarshalling.TypeIdTuple)
        {
            return tuple.GetObjectCollectionWithType<T>();
        }

        int elementCount = tuple.GetInt(1);
        T[] resultsPooledArr = ArrayPool<T>.Shared.Rent(elementCount);

        try
        {
            for (var i = 0; i < elementCount; i++)
            {
                resultsPooledArr[i] = (T)(object)TupleWithSchemaMarshalling.Unpack(tuple.GetBytesSpan(2 + i));
            }

            return (resultsPooledArr, elementCount);
        }
        catch
        {
            ArrayPool<T>.Shared.Return(resultsPooledArr);
            throw;
        }
    }
}
