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

namespace Apache.Ignite.Internal.Compute;

using System;
using System.Buffers;
using Ignite.Sql;
using Ignite.Table;
using Marshalling;
using Proto.BinaryTuple;
using Proto.MsgPack;

/// <summary>
/// Compute packer utils.
/// </summary>
internal static class ComputePacker
{
    private const int Native = 1;
    private const int Tuple = -1;
    private const int MarshallerObject = -2;

    /// <summary>
    /// Packs compute job arg.
    /// </summary>
    /// <param name="w">Packer.</param>
    /// <param name="obj">Arg.</param>
    /// <param name="marshaller">Marshaller.</param>
    /// <typeparam name="T">Arg type.</typeparam>
    internal static void PackArg<T>(ref MsgPackWriter w, T obj, IMarshaller<T>? marshaller)
    {
        if (obj == null)
        {
            w.WriteNil();
            return;
        }

        if (marshaller != null)
        {
            w.Write(MarshallerObject);

            using var builder = new BinaryTupleBuilder(3);
            builder.AppendInt((int)ColumnType.ByteArray);
            builder.AppendInt(0); // Scale.
            builder.AppendBytes(
                static (IBufferWriter<byte> writer, (T Obj, IMarshaller<T> Marshaller) arg) => arg.Marshaller.Marshal(arg.Obj, writer),
                arg: (obj, marshaller));

            w.Write(builder.Build().Span);
            return;
        }

        if (obj is IIgniteTuple)
        {
            // TODO: Ticket.
            w.Write(Tuple);
            throw new NotSupportedException("TODO: Ticket");
        }

        w.Write(Native);
        w.WriteObjectAsBinaryTuple(obj);
    }

    /// <summary>
    /// Unpacks compute job result.
    /// </summary>
    /// <param name="r">Reader.</param>
    /// <param name="marshaller">Optional marshaller.</param>
    /// <typeparam name="T">Result type.</typeparam>
    /// <returns>Result.</returns>
    internal static T UnpackResult<T>(ref MsgPackReader r, IMarshaller<T>? marshaller)
    {
        if (r.TryReadNil())
        {
            return (T)(object)null!;
        }

        int type = r.ReadInt32();

        return type switch
        {
            Tuple => throw new NotSupportedException("TODO: Ticket"),
            MarshallerObject => Unmarshal(ref r, marshaller),
            _ => (T)r.ReadObjectFromBinaryTuple()!
        };

        static T Unmarshal(ref MsgPackReader r, IMarshaller<T>? marshaller)
        {
            if (marshaller == null)
            {
                throw new ArgumentNullException(nameof(marshaller), "Compute job result marshaller is required but not provided.");
            }

            var tuple = new BinaryTupleReader(r.ReadBinary(), 3);
            var type = (ColumnType)tuple.GetInt(0);

            if (type != ColumnType.ByteArray)
            {
                throw new UnsupportedObjectTypeMarshallingException(
                    Guid.NewGuid(),
                    ErrorGroups.Marshalling.UnsupportedObjectType,
                    "Unsupported object type. Expected byte[], got " + type);
            }

            var bytes = tuple.GetBytesSpan(2);

            return marshaller.Unmarshal(bytes);
        }
    }
}
