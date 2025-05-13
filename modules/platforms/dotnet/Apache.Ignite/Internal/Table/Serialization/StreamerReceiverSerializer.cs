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
using System.Diagnostics;
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
    /// Reads receiver execution results.
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
