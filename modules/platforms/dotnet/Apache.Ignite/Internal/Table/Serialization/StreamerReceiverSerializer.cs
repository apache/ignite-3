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
using System.Diagnostics.CodeAnalysis;
using Buffers;
using Compute;
using Ignite.Sql;
using Ignite.Table;
using Marshalling;
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
    /// <param name="payloadMarshaller">Payload marshaller.</param>
    /// <param name="argMarshaller">Argument marshaller.</param>
    /// <typeparam name="T">Item type.</typeparam>
    /// <typeparam name="TArg">Arg type.</typeparam>
    public static void WriteReceiverInfo<T, TArg>(
        ref MsgPackWriter w,
        string className,
        TArg arg,
        ArraySegment<T> items,
        IMarshaller<T>? payloadMarshaller,
        IMarshaller<TArg>? argMarshaller)
    {
        using var builder = BuildReceiverInfo(className, arg, items, payloadMarshaller, argMarshaller);

        w.Write(builder.NumElements);
        w.Write(builder.Build().Span);
    }

    /// <summary>
    /// Builds receiver info.
    /// </summary>
    /// <param name="className">Receiver class name.</param>
    /// <param name="arg">Receiver argument.</param>
    /// <param name="items">Receiver items.</param>
    /// <param name="payloadMarshaller">Payload marshaller.</param>
    /// <param name="argMarshaller">Argument marshaller.</param>
    /// <param name="prefixSize">Builder prefix size.</param>
    /// <typeparam name="T">Item type.</typeparam>
    /// <typeparam name="TArg">Argument type.</typeparam>
    /// <returns>Binary tuple builder.</returns>
    public static BinaryTupleBuilder BuildReceiverInfo<T, TArg>(
        string className,
        TArg arg,
        ArraySegment<T> items,
        IMarshaller<T>? payloadMarshaller,
        IMarshaller<TArg>? argMarshaller,
        int prefixSize = 0)
    {
        Debug.Assert(items.Count > 0, "items.Count > 0");

        // className + arg + items size + item type + items.
        int binaryTupleSize = 1 + 3 + 1 + 1 + items.Count;
        var builder = new BinaryTupleBuilder(binaryTupleSize, prefixSize: prefixSize);

        try
        {
            builder.AppendString(className);

            if (argMarshaller != null)
            {
                builder.AppendInt((int)ColumnType.ByteArray);
                builder.AppendInt(0); // Scale.
                builder.AppendBytes(static (bufWriter, a) => a.argMarshaller.Marshal(a.arg, bufWriter), (arg, argMarshaller));
            }
            else if (arg is IIgniteTuple tupleArg)
            {
                builder.AppendInt(TupleWithSchemaMarshalling.TypeIdTuple);
                builder.AppendInt(0); // Scale.
                builder.AppendBytes(static (bufWriter, arg) => TupleWithSchemaMarshalling.Pack(bufWriter, arg), tupleArg);
            }
            else
            {
                builder.AppendObjectWithType(arg);
            }

            AppendCollection(builder, items, payloadMarshaller);

            return builder;
        }
        catch (Exception)
        {
            builder.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Writes receiver execution results. Opposite of <see cref="ReadReceiverResults{T}"/>.
    /// </summary>
    /// <param name="w">Writer.</param>
    /// <param name="res">Results.</param>
    /// <param name="marshaller">Marshaller.</param>
    /// <typeparam name="T">Result item type.</typeparam>
    public static void WriteReceiverResults<T>(MsgPackWriter w, IList<T>? res, IMarshaller<T>? marshaller)
    {
        if (res == null)
        {
            w.WriteNil();
            return;
        }

        int resTupleElementCount = res.Count + 2;

        // Reserve a 4-byte prefix for resTupleElementCount.
        using var builder = new BinaryTupleBuilder(resTupleElementCount, prefixSize: 4);
        AppendCollection(builder, res, marshaller);

        Memory<byte> jobResultTupleMemWithPrefix = builder.Build();
        BinaryPrimitives.WriteInt32LittleEndian(jobResultTupleMemWithPrefix.Span, resTupleElementCount);
        ComputePacker.PackArgOrResult(ref w, jobResultTupleMemWithPrefix, null);
    }

    /// <summary>
    /// Reads receiver execution results. Opposite of <see cref="WriteReceiverResults{T}"/>.
    /// </summary>
    /// <param name="reader">Reader.</param>
    /// <param name="marshaller">Marshaller.</param>
    /// <typeparam name="T">Result element type.</typeparam>
    /// <returns>Pooled array with results and the actual element count.</returns>
    public static (T[]? ResultsPooledArray, int ResultsCount) ReadReceiverResults<T>(MsgPackReader reader, IMarshaller<T>? marshaller)
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
        if (tuple.GetInt(0) != TupleWithSchemaMarshalling.TypeIdTuple && marshaller == null)
        {
            return tuple.GetObjectCollectionWithType<T>();
        }

        int elementCount = tuple.GetInt(1);
        T[] resultsPooledArr = ArrayPool<T>.Shared.Rent(elementCount);

        try
        {
            if (marshaller != null)
            {
                for (var i = 0; i < elementCount; i++)
                {
                    resultsPooledArr[i] = marshaller.Unmarshal(tuple.GetBytesSpan(2 + i));
                }

                return (resultsPooledArr, elementCount);
            }

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

    /// <summary>
    /// Reads the receiver type name.
    /// </summary>
    /// <param name="buf">Buffer.</param>
    /// <returns>Receiver type name.</returns>
    public static string ReadReceiverTypeName(PooledBuffer buf) =>
        GetReceiverInfoReaderFast(buf).GetString(0);

    /// <summary>
    /// Reads the receiver info from the buffer.
    /// </summary>
    /// <param name="buf">Buffer.</param>
    /// <param name="payloadMarshaller">Payload marshaller.</param>
    /// <param name="argumentMarshaller">Argument marshaller.</param>
    /// <typeparam name="TItem">Item type.</typeparam>
    /// <typeparam name="TArg">Argument type.</typeparam>
    /// <returns>Receiver info.</returns>
    public static ReceiverInfo<TItem, TArg> ReadReceiverInfo<TItem, TArg>(
        PooledBuffer buf,
        IMarshaller<TItem>? payloadMarshaller,
        IMarshaller<TArg>? argumentMarshaller)
    {
        BinaryTupleReader receiverInfo = GetReceiverInfoReaderFast(buf);

        var arg = (TArg)ReadReceiverArg(ref receiverInfo, 1, argumentMarshaller)!;
        List<TItem> items = ReadReceiverPage(ref receiverInfo, payloadMarshaller);

        return new(items, arg);
    }

    [SuppressMessage("Design", "CA1002:Do not expose generic lists", Justification = "Private method.")]
    private static List<T> ReadReceiverPage<T>(ref BinaryTupleReader receiverInfo, IMarshaller<T>? marshaller)
    {
        int itemType = receiverInfo.GetInt(4);
        int itemCount = receiverInfo.GetInt(5);

        List<T> items = new List<T>(itemCount);

        if (marshaller != null)
        {
            for (int i = 0; i < itemCount; i++)
            {
                T item = marshaller.Unmarshal(receiverInfo.GetBytesSpan(i + 6));
                items.Add(item);
            }
        }
        else if (itemType == TupleWithSchemaMarshalling.TypeIdTuple)
        {
            for (int i = 0; i < itemCount; i++)
            {
                IgniteTuple tuple = TupleWithSchemaMarshalling.Unpack(receiverInfo.GetBytesSpan(i + 6));
                items.Add((T)(object)tuple);
            }
        }
        else
        {
            ColumnType colType = (ColumnType)itemType;
            for (int i = 0; i < itemCount; i++)
            {
                object? item = receiverInfo.GetObject(i + 6, colType);
                items.Add((T)item!);
            }
        }

        return items;
    }

    private static TArg? ReadReceiverArg<TArg>(ref BinaryTupleReader reader, int index, IMarshaller<TArg>? marshaller)
    {
        if (reader.IsNull(index))
        {
            return default;
        }

        if (reader.GetInt(index) == TupleWithSchemaMarshalling.TypeIdTuple)
        {
            return (TArg)(object)TupleWithSchemaMarshalling.Unpack(reader.GetBytesSpan(index + 2));
        }

        if (marshaller != null)
        {
            ReadOnlySpan<byte> bytes = reader.GetBytesSpan(index + 2);
            return marshaller.Unmarshal(bytes);
        }

        return (TArg?)reader.GetObject(index);
    }

    private static BinaryTupleReader GetReceiverInfoReaderFast(PooledBuffer jobArgBuf)
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

    private static void AppendTupleCollection<T>(BinaryTupleBuilder builder, ICollection<T> items)
    {
        builder.AppendInt(TupleWithSchemaMarshalling.TypeIdTuple);
        builder.AppendInt(items.Count);

        foreach (var item in items)
        {
            builder.AppendBytes(static (bufWriter, arg) => TupleWithSchemaMarshalling.Pack(bufWriter, (IIgniteTuple)arg!), item);
        }
    }

    private static void AppendMarshalledCollection<T>(BinaryTupleBuilder builder, ICollection<T> items, IMarshaller<T> marshaller)
    {
        builder.AppendInt((int)ColumnType.ByteArray);
        builder.AppendInt(items.Count);

        foreach (var item in items)
        {
            builder.AppendBytes(
                static (bufWriter, arg) => arg.marsh.Marshal(arg.obj, bufWriter),
                (marsh: marshaller, obj: item));
        }
    }

    private static void AppendCollection<T>(BinaryTupleBuilder builder, IList<T> items, IMarshaller<T>? marshaller)
    {
        if (marshaller != null)
        {
            AppendMarshalledCollection(builder, items, marshaller);
        }
        else if (items.Count > 0 && items[0] is IIgniteTuple)
        {
            AppendTupleCollection(builder, items);
        }
        else
        {
            builder.AppendObjectCollectionWithType(items);
        }
    }

    [SuppressMessage("Design", "CA1002:Do not expose generic lists", Justification = "Performance.")]
    [SuppressMessage("StyleCop.CSharp.DocumentationRules", "SA1600:Elements should be documented", Justification = "DTO.")]
    internal record ReceiverInfo<TItem, TArg>(List<TItem> Page, TArg Arg);
}
