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

namespace Apache.Ignite.Internal.Proto.MsgPack;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using BinaryTuple;
using Buffers;
using Transactions;

/// <summary>
/// MsgPack writer.
/// </summary>
[SuppressMessage("StyleCop.CSharp.DocumentationRules", "SA1600:Elements should be documented", Justification = "TODO")] // TODO
internal readonly record struct MsgPackWriter(PooledArrayBufferWriter Writer)
{
    private const int MaxFixPositiveInt = 127;
    private const int MaxFixStringLength = 31;
    private const int MinFixNegativeInt = -32;

    /*
    private const int MaxFixNegativeInt = -1;
    private const int MinFixStringLength = 0;
    private const int MaxFixMapCount = 15;
    private const int MaxFixArrayCount = 15;
    */

    /// <summary>
    /// Writes an unsigned value to specified memory location and returns number of bytes written.
    /// </summary>
    /// <param name="span">Span.</param>
    /// <param name="val">Value.</param>
    /// <returns>Bytes written.</returns>
    public static int WriteUnsigned(Span<byte> span, ulong val)
    {
        unchecked
        {
            if (val <= MaxFixPositiveInt)
            {
                span[0] = (byte)val;
                return 1;
            }

            if (val <= byte.MaxValue)
            {
                span[0] = MsgPackCode.UInt8;
                span[1] = (byte)val;

                return 2;
            }

            if (val <= ushort.MaxValue)
            {
                span[0] = MsgPackCode.UInt16;
                BinaryPrimitives.WriteUInt16BigEndian(span[1..], (ushort)val);

                return 3;
            }

            if (val <= uint.MaxValue)
            {
                span[0] = MsgPackCode.UInt32;
                BinaryPrimitives.WriteUInt32BigEndian(span[1..], (uint)val);

                return 5;
            }

            span[0] = MsgPackCode.UInt64;
            BinaryPrimitives.WriteUInt64BigEndian(span[1..], val);

            return 9;
        }
    }

    public void WriteNil() => Writer.GetSpanAndAdvance(1)[0] = MsgPackCode.Nil;

    public void Write(bool val) => Writer.GetSpanAndAdvance(1)[0] = val ? MsgPackCode.True : MsgPackCode.False;

    /// <summary>
    /// Writes a <see cref="Guid"/> as UUID (RFC #4122).
    /// <para />
    /// <see cref="Guid"/> uses a mixed-endian format which differs from UUID,
    /// see https://en.wikipedia.org/wiki/Universally_unique_identifier#Encoding.
    /// </summary>
    /// <param name="val">Guid.</param>
    public void Write(Guid val)
    {
        var span = Writer.GetSpanAndAdvance(18);
        span[0] = MsgPackCode.FixExt16;
        span[1] = (byte)ClientMessagePackType.Uuid;

        UuidSerializer.Write(val, span[2..]);
    }

    public void Write(string? val)
    {
        if (val == null)
        {
            WriteNil();
            return;
        }

        var byteCount = ProtoCommon.StringEncoding.GetMaxByteCount(val.Length);
        var bufferSize = byteCount + 5;
        var span = Writer.GetSpan(bufferSize);

        if (byteCount <= MaxFixStringLength)
        {
            var bytesWritten = ProtoCommon.StringEncoding.GetBytes(val, span[1..]);
            span[0] = (byte)(MsgPackCode.MinFixStr | bytesWritten);
            Writer.Advance(bytesWritten + 1);
        }
        else if (byteCount <= byte.MaxValue)
        {
            var bytesWritten = ProtoCommon.StringEncoding.GetBytes(val, span[2..]);
            span[0] = MsgPackCode.Str8;
            span[1] = unchecked((byte)bytesWritten);
            Writer.Advance(bytesWritten + 2);
        }
        else if (byteCount <= ushort.MaxValue)
        {
            var bytesWritten = ProtoCommon.StringEncoding.GetBytes(val, span[3..]);
            span[0] = MsgPackCode.Str16;
            BinaryPrimitives.WriteUInt16BigEndian(span[1..], (ushort)bytesWritten);
            Writer.Advance(bytesWritten + 3);
        }
        else
        {
            var bytesWritten = ProtoCommon.StringEncoding.GetBytes(val, span[5..]);
            span[0] = MsgPackCode.Str32;
            BinaryPrimitives.WriteUInt32BigEndian(span[1..], (uint)bytesWritten);
            Writer.Advance(bytesWritten + 5);
        }
    }

    public void Write(long value)
    {
        if (value >= 0)
        {
            var span = Writer.GetSpan(9);
            var written = WriteUnsigned(span, (ulong)value);
            Writer.Advance(written);
        }
        else
        {
            if (value >= MinFixNegativeInt)
            {
                var span = Writer.GetSpan(1);
                span[0] = unchecked((byte)value);
                Writer.Advance(1);
            }
            else if (value >= sbyte.MinValue)
            {
                var span = Writer.GetSpan(2);
                span[0] = MsgPackCode.Int8;
                span[1] = unchecked((byte)value);
                Writer.Advance(2);
            }
            else if (value >= short.MinValue)
            {
                var span = Writer.GetSpan(3);
                span[0] = MsgPackCode.Int16;
                BinaryPrimitives.WriteInt16BigEndian(span[1..], (short)value);
                Writer.Advance(3);
            }
            else if (value >= int.MinValue)
            {
                var span = Writer.GetSpan(5);
                span[0] = MsgPackCode.Int32;
                BinaryPrimitives.WriteInt32BigEndian(span[1..], (int)value);
                Writer.Advance(5);
            }
            else
            {
                this.WriteInt64(value);
            }
        }
    }

    public void Write(int val)
    {
        throw new NotImplementedException();
    }

    /// <summary>
    /// Writes an array of objects with type codes.
    /// </summary>
    /// <param name="col">Array.</param>
    public void WriteObjectCollectionAsBinaryTuple(ICollection<object?>? col)
    {
        if (col == null)
        {
            WriteNil();

            return;
        }

        Write(col.Count);

        using var builder = new BinaryTupleBuilder(col.Count * 3);

        foreach (var obj in col)
        {
            builder.AppendObjectWithType(obj);
        }

        Write(builder.Build().Span);
    }

    /// <summary>
    /// Writes a transaction.
    /// </summary>
    /// <param name="tx">Transaction.</param>
    public void WriteTx(Transaction? tx)
    {
        if (tx == null)
        {
            WriteNil();
        }
        else
        {
            Write(tx.Id);
        }
    }

    public void Write(Span<byte> span)
    {
        throw new NotImplementedException();
    }

    public Span<byte> WriteBitSet(int bitCount)
    {
        // var byteCount = bitCount / 8 + 1;
        // writer.WriteExtensionFormatHeader(new ExtensionHeader((sbyte)ClientMessagePackType.Bitmask, byteCount));
        //
        // var span = writer.GetSpan(byteCount)[..byteCount];
        // span.Clear();
        // writer.Advance(byteCount);
        //
        // return span;
        throw new NotImplementedException();
    }

    public void WriteArrayHeader(int count)
    {
        throw new NotImplementedException();
    }

    public void WriteBinHeader(int count)
    {
        throw new NotImplementedException();
    }

    public void WriteMapHeader(int count)
    {
        throw new NotImplementedException();
    }
}
